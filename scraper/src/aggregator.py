import time
import psycopg2
import logging
from ruamel import yaml
import pymongo
from pymongo import MongoClient
from datetime import datetime
from psycopg2 import sql
import pytz


class Aggregator:
    def __init__(self, configpath='src/scraper_config.yml',
                 keypath='keys.yml'):
        self.aggregation_interval = 3600
        self.config_path = configpath
        with open(configpath) as f:
            config = yaml.safe_load(f)
        with open(keypath) as f:
            keys = yaml.safe_load(f)
        self.twitch_db = config['twitch']['db']
        self.youtube_db = config['youtube']['db']
        self.postgres = config['postgres']
        self.postgres['user'] = keys['postgres']['user']
        self.postgres['passwd'] = keys['postgres']['passwd']
        self.client = None

    @staticmethod
    def table_exists(conn, tablename):
        """
        Returns True if the 'games' table exists.

        :param conn:
        :return: bool
        """
        tables = ['games', 'twitch_top_games', 'twitch_broadcasts',
                  'twitch_channels', 'channel_affiliations', 'orgs']
        if tablename not in tables:
            return False
        exists = ('SELECT count(*) '
                  '    FROM information_schema.tables'
                  '    WHERE table_name = \'{}\'')
        cursor = conn.cursor()
        cursor.execute(exists.format(tablename))
        return cursor.fetchone()[0] > 0

    @staticmethod
    def create_tables(conn):
        """
        Creates Postgres tables.


        :param conn: psycopg2 connection
        :return:
        """
        games = (
            'CREATE TABLE games( '
            '    game_id integer PRIMARY KEY, '
            '    name text NOT NULL UNIQUE, '
            '    giantbomb_id integer '
            ');'
        )
        games_index = 'CREATE INDEX game_name_idx ON games USING HASH (name);'
        twitch_top_games = (
            'CREATE TABLE twitch_top_games( '
            '    game_id integer REFERENCES games(game_id), '
            '    epoch integer NOT NULL, '
            '    viewers integer NOT NULL, '
            '    PRIMARY KEY (game_id, epoch) '
            ');'
        )
        twitch_channels = (
            'CREATE TABLE twitch_channels( '
            '    channel_id integer PRIMARY KEY, '
            '    name text NOT NULL ' 
            ');'
        )
        twitch_broadcasts = (
            'CREATE TABLE twitch_broadcasts( ' 
            '    channel_id integer REFERENCES twitch_channels(channel_id), '
            '    epoch integer NOT NULL, '
            '    game_id integer REFERENCES games(game_id), '
            '    viewers integer NOT NULL, '
            '    stream_title text, '
            '    PRIMARY KEY (channel_id, epoch)'
            ');'
        )
        orgs = (
            'CREATE TABLE orgs( '
            '    org_id integer PRIMARY KEY, '
            '    name text NOT NULL '
            ');'
        )
        channel_affiliations = (
            'CREATE TABLE channel_affiliations( '
            '    org_id integer REFERENCES orgs(org_id), '
            '    channel_id integer REFERENCES twitch_channels(channel_id), '
            '    PRIMARY KEY (channel_id) '
            ');'
        )
        curs = conn.cursor()
        if not Aggregator.table_exists(conn, 'games'):
            curs.execute(games)
            curs.execute(games_index)
            logging.info('Created Table: games')
        if not Aggregator.table_exists(conn, 'twitch_top_games'):
            curs.execute(twitch_top_games)
            logging.info('Created Table: twitch_top_games')
        if not Aggregator.table_exists(conn, 'twitch_channels'):
            curs.execute(twitch_channels)
            logging.info('Created Table: twitch_channels')
        if not Aggregator.table_exists(conn, 'twitch_broadcasts'):
            curs.execute(twitch_broadcasts)
            logging.info('Created Table: twitch_broadcasts')
        if not Aggregator.table_exists(conn, 'orgs'):
            curs.execute(orgs)
            logging.info('Created Table: orgs')
        if not Aggregator.table_exists(conn, 'channel_affiliations'):
            curs.execute(channel_affiliations)
            logging.info('Created Table: channel_affiliations')

    def initdb(self):
        """
        Initializes the Postgres database.

        Initializes the database using the schema shown in the schema
        design file (PNG).
        """
        try:
            conn = psycopg2.connect( host=self.postgres['host'],
                                     port=self.postgres['port'],
                                     user=self.postgres['user'],
                                     password=self.postgres['passwd'],
                                     dbname=self.postgres['db_name'])
            tables = ['games', 'twitch_top_games', 'twitch_broadcasts',
                      'twitch_channels', 'channel_affiliations', 'orgs']
            if not all(self.table_exists(conn, t) for t in tables):
                conn.commit()
                logging.info('Initializing missing tables.')
                self.create_tables(conn)
                conn.commit()
                logging.info('Twitch Schema Initialized.')
            conn.close()
            logging.debug('Postgres database ready.')
        except psycopg2.DatabaseError as e:
            logging.warning('Failed to initialize database: {}'.format(e))
            raise psycopg2.DatabaseError

    @staticmethod
    def last_postgres_update(conn, table):
        """
        Returns the largest entry in the epoch column.

        The specified table must contain a column named epoch and 0 is returned
        if the column is empty.

        :param conn: psycopg2.connection, Database to connect to.
        :param table: str, Name of table.
        :return: int, Epoch corresponding to last update.
        """
        query = ('SELECT COALESCE(MAX(epoch), 0) '
                 'FROM {}')
        query = sql.SQL(query).format(sql.Identifier(table))
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def first_entry_after(self, start):
        """
        Returns the timestamp of the first entry after start.

        The instance variable client must be initialized to a MongoClient.

        :param start:
        :return: int
        """
        db = self.client[self.twitch_db['db_name']]
        topgames = db[self.twitch_db['top_games']]
        cursor = topgames.find(
            {'timestamp': {'$gt': start}}
        ).sort('timestamp', pymongo.ASCENDING)
        if cursor.count() > 0:
            return int(cursor[0]['timestamp'])
        else:
            return 1 << 31

    def docsbetween(self, start, end, collname):
        """
        Returns entries with timestamps between start and end.

        Returns a cursor to documents in the specified Mongo collection that
        have a field 'timestamp' with values greater than or equal to start
        and less than end.

        :param start: int, Timestamp of the earliest entry
        :param end: int, Timestamp of the last entry
        :return: pymongo.cursor.Cursor
        """
        db = self.client[self.twitch_db['db_name']]
        coll = db[collname]
        cursor = coll.find(
            {'timestamp': {'$gte': start, '$lt': end}}
        ).sort('timestamp', pymongo.ASCENDING)
        return cursor

    @staticmethod
    def agg_top_games_period(entries, start, end):
        """
        Determines the average viewercount of each game over the specified 
        period.  Entries must be in ascending order based on timestamp.
        
        :param entries: cursor or list,
        :param start: int 
        :param end: int
        :return: dict, keys are game_ids and values are dictionaries where
            'v' is the viewercount and 'name' is the game's name.
        """
        # Get documents from cursor
        docs = []
        for doc in entries:
            docs.append(doc)

        # Calculate viewership
        avgviewers = Aggregator.average_viewers(docs, start, end, 'games',
                                                'viewers')
        # Format Results
        for id, viewers in avgviewers.items():
            avgviewers[id] = {'v': viewers}
        for doc in docs:
            for gameid, game in doc['games'].items():
                avgviewers[gameid]['name'] = game['name']
                avgviewers[gameid]['giantbomb_id'] = game['giantbomb_id']
        return avgviewers

    @staticmethod
    def average_viewers(entries, start, end, aggkey, viewers):
        """
        Returns the average viewercount over the specified period.

        Entries must be in ascending order based on timestamp.
        Each dict in the entries parameter must be in the format:
            { aggkey: {
                        id: {viewers: int}
                      }
              'timestamp': int, Epoch
            }

        :param entries: list[dict], Entries to be aggregated
        :param start: int, Start of aggregation period
        :param end: int, End of aggregation period
        :param aggkey: str, Name of the id key in entries.
        :param viewers: str, Name of the viewers key in entries.
        :return: dict, {id: average_viewers}, Average viewer of each item.
        """
        # TODO: Check that broadcasts are in ascending order
        last_timestamp = start
        res = {}
        # Add up the total number of viewer seconds.
        doc = None
        for doc in entries:
            cur_timestamp = doc['timestamp']
            for gameid, game in doc[aggkey].items():
                if gameid not in res:
                    res[gameid] = 0
                viewersecs = game[viewers] * (cur_timestamp - last_timestamp)
                res[gameid] += viewersecs
            last_timestamp = cur_timestamp

        # Return if there were no entries
        if not doc:
            return res

        # Add viewers from the last entry to the end of the period
        for gameid, game in doc[aggkey].items():
            res[gameid] += game['viewers']*(end-last_timestamp)
        # Convert viewerseconds into the average number of viewers
        for gameid in res:
            res[gameid] //= (end-start)
        return res

    def store_top_games(self, games, timestamp, conn):
        """
        Stores top game entries using conn.

        There is no error checking in this function so the games id field
        must already exist in the games table.  Call store_game_ids on the
        list of game entries first to make sure this function does not crash.
        """
        if not games:
            return
        cursor = conn.cursor()
        query = ('INSERT INTO twitch_top_games '
                 'VALUES {} '
                 'ON CONFLICT DO NOTHING ')

        # Sanitize values
        values = []
        for gid, game in games.items():
            tup = (int(gid), timestamp, game['v'])
            values.append(cursor.mogrify("(%s,%s,%s)", tup).decode())
        query = query.format(','.join(values))
        cursor.execute(query)
        logging.debug("Top games stored from: " + self.strtime(timestamp))

    @staticmethod
    def strtime(timestamp):
        tz = pytz.timezone('US/Pacific')
        dt = datetime.fromtimestamp(timestamp, tz)
        return dt.strftime("%Z - %Y/%m/%d, %H:%M:%S")

    @staticmethod
    def store_game_ids(games, conn):
        """
        Stores the game ids specified in games or does nothing if they
        already are stored.  Does not commit.

        :param games: dict, Output from agg_top_games_period
        :param conn: psycog2.Connection, database connection
        :return:
        """
        if not games:
            return
        query = ('INSERT INTO games (game_id, name, giantbomb_id) '
                 'VALUES {} '
                 'ON CONFLICT DO NOTHING')
        curs = conn.cursor()
        values = []
        for gid, game in games.items():
            tup = (gid, game['name'], game['giantbomb_id'])
            values.append(curs.mogrify("(%s, %s, %s)", tup).decode())
        query = query.format(','.join(values))
        curs.execute(query)

    @staticmethod
    def store_twitch_channels(streams, conn):
        """
        Adds new channels to db.

        Stores the game ids specified in streams or does nothing if they
        already are stored.  Does not commit.

        :param streams: dict,
        :param conn: psycog2.Connection, Postgres connection.
        :return:
        """
        if not streams:
            return
        # TODO: Handle name change updates
        query = ('INSERT INTO twitch_channels (channel_id, name) '
                 'VALUES {} '
                 'ON CONFLICT DO NOTHING')
        curs = conn.cursor()
        values = []
        for chid, stream in streams.items():
            tup = (chid, stream['ch_name'])
            values.append(curs.mogrify("(%s, %s)", tup).decode())
        query = query.format(','.join(values))
        curs.execute(query)

    def store_broadcasts(self, streams, timestamp, conn):
        """
        Stores top game entries using conn.

        There is no error checking in this function so the games id field
        must already exist in the games table.  Call store_game_ids on the
        list of game entries first to make sure this function does not crash.
        """
        if not streams:
            return
        cursor = conn.cursor()
        query = ('INSERT INTO twitch_broadcasts '
                 'VALUES {} '
                 'ON CONFLICT DO NOTHING ')

        # Sanitize values
        values = []
        game_ids = {}
        for channel_id, stream in streams.items():
            gn = stream['game_name']
            if gn not in game_ids:
                game_ids[gn] = self.game_name_to_id(conn, gn)
            tup = (int(channel_id),
                   timestamp,
                   game_ids[gn],
                   stream['viewers'],
                   stream['stream_title'])
            values.append(cursor.mogrify("(%s,%s,%s,%s,%s)", tup).decode())
        query = query.format(','.join(values))
        cursor.execute(query)
        logging.debug("Broadcasts stored from: " + self.strtime(timestamp))

    def game_name_to_id(self, conn, name):
        query = ('SELECT game_id '
                 'FROM games '
                 'WHERE name = %s')
        cursor = conn.cursor()
        cursor.execute(query, (name,))
        return cursor.fetchone()[0]

    def agg_top_games(self):
        """
        Aggregates and stores twitch game info.

        Checks the MongoDB specified in the config file for new top games
        entries, aggregates them, and stores them in Postgres.  initdb Must
        be called before calling this function.

        :return: None
        """
        # start is the first second of the next hour that we need to aggregate
        # end is the last second of the most recent full hour
        conn = self.postgresconn()
        self.client = MongoClient(self.twitch_db['host'],
                                  self.twitch_db['port'])
        curhrstart, curhrend, last = self._agg_ts(conn, self.twitch_db['top_games'])
        while curhrend < last:
            entries = self.docsbetween(curhrstart, curhrend,
                                       self.twitch_db['top_games'])
            games = self.agg_top_games_period(entries, curhrstart, curhrend)
            # Some hours empty due to server failure
            if games:
                self.store_game_ids(games, conn)
                self.store_top_games(games, curhrstart, conn)
            curhrstart += 3600
            curhrend += 3600
        conn.commit()
        conn.close()
        self.client.close()

    def agg_twitch_broadcasts(self):
        """
        Retrieves, aggregates, and stores twitch broadcasts.

        Checks the MongoDB specified in the config file for new twitch
        broadcasts, aggregates them, and stores them in Postgres.  initdb must
        be called before calling this function.

        :return:
        """
        conn = self.postgresconn()
        self.client = MongoClient(self.twitch_db['host'],
                                  self.twitch_db['port'])
        hrstart, hrend, last = self._agg_ts(conn, self.twitch_db['top_streams'])
        while hrend < last:
            entries = self.docsbetween(hrstart, hrend,
                                       self.twitch_db['top_streams'])
            streams = self.agg_twitch_broadcasts_period(entries, hrstart, hrend)
            # Some hours empty due to server failure
            if streams:
                self.store_twitch_channels(streams, conn)
                self.store_broadcasts(streams, hrstart, conn)
            hrstart += 3600
            hrend += 3600
        conn.commit()
        conn.close()
        self.client.close()

    @staticmethod
    def agg_twitch_broadcasts_period(entries, start, end):
        """
        Determines the average number of viewers of each broadcast over the
        specified period.  Entries must be in ascending order based on
        timestamp.

        :param entries: cursor or list,
        :param start: int
        :param end: int
        :return: dict, keys are game_ids and values are dictionaries where
            'v' is the viewercount and 'name' is the game's name.
        """
        # Get documents from cursor
        docs = []
        for doc in entries:
            docs.append(doc)

        # Calculate viewership
        avgviewers = Aggregator.average_viewers(docs, start, end, 'games',
                                                'viewers')
        # Format Results
        for id, viewers in avgviewers.items():
            avgviewers[id] = {'viewers': viewers}
        for doc in docs:
            for channelid, stream in doc['streams'].items():
                chnl = avgviewers[channelid]
                chnl['channel_id'] = stream['broadcaster_id']
                chnl['channel_name'] = stream['display_name']
                chnl['stream_title'] = stream['status']
                chnl['game_name'] = stream['game']
        return avgviewers

    def _agg_ts(self, conn, collname):
        """
        Helper function for aggregation timestamps.

        Calculates the timestamps for curhrstart (the first second of the first
        hour that should aggregated), curhrend (one hour later than
        curhrstart), and last (the first second in the current hour).

        :param conn: psycopg2.connection
        :param collname: str, Name of Mongo collection
        :return: tuple, (curhrstart, curhrend, last)
        """
        # TODO: Rename function
        sechr = 3600
        start = self.last_postgres_update(conn, collname) + sechr
        last = self.epoch_to_hour(time.time())
        earliest_entry = self.first_entry_after(start)
        curhrstart = earliest_entry // sechr*sechr
        curhrend = curhrstart + sechr
        return curhrstart, curhrend, last

    @staticmethod
    def epoch_to_hour(epoch):
        """
        Returns the last second in the most recent hour.

        Example. If the epoch corresponds to 2:16:37, then the integer
        corresponding to 2:00:00 will be returned.

        :param epoch: int or float, Unix Epoch
        :return: int
        """
        seconds_in_hour = 3600
        return int(epoch) // seconds_in_hour * seconds_in_hour

    def postgresconn(self):
        """
        psycopg2.connect wrapper.

        :return: psycopg2.connection
        """
        conn = psycopg2.connect(host=self.postgres['host'],
                                port=self.postgres['port'],
                                user=self.postgres['user'],
                                password=self.postgres['passwd'],
                                dbname=self.postgres['db_name'])
        return conn

if __name__ == '__main__':
    logformat = '%(asctime)s %(levelname)s:%(message)s'
    logging.basicConfig(format=logformat, level=logging.DEBUG,
                        filename='aggregator.log')
    logging.debug("Aggregator Starting.")
    a = Aggregator()
    a.initdb()
    start = time.time()
    a.agg_top_games()
    end = time.time()
    print("Total Time: {:.2f}".format(end-start))