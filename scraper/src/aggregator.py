import time
import psycopg2
import logging
from ruamel import yaml
import pymongo
from pymongo import MongoClient
from datetime import datetime
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
    def db_initialized(conn):
        """
        Returns True if the 'games' table exists.

        :param conn:
        :return: bool
        """
        gamesexists = 'SELECT 1 FROM games'
        try:
            cursor = conn.cursor()
            cursor.execute(gamesexists)
            return True
        except psycopg2.DatabaseError:
            return False

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
            '    channel_name text NOT NULL ' 
            ');'
        )
        twitch_broadcasts = (
            'CREATE TABLE twitch_broadcasts( ' 
            '    epoch integer NOT NULL, '
            '    game_id integer REFERENCES games(game_id), '
            '    viewers integer NOT NULL, '
            '    channel_id integer REFERENCES twitch_channels(channel_id), '
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
            '    org_id REFERENCES orgs(org_id), '
            '    channel_id REFERENCES twitch_channels(channel_id), '
            '    PRIMARY KEY (org_id, channel_id) '
            ');'
        )
        curs = conn.cursor()
        curs.execute(games)
        curs.execute(games_index)
        curs.execute(twitch_top_games)
        #curs.execute(twitch_channels)
        #curs.execute(twitch_broadcasts)
        #curs.execute(orgs)
        #curs.execute(channel_affiliations)

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
            if not self.db_initialized(conn):
                conn.commit()
                logging.info("Postgres Twitch schema not initialized.")
                self.create_tables(conn)
                conn.commit()
                logging.info("Twitch Schema Initialized.")
            conn.close()
            logging.debug("Postgres database ready.")
        except psycopg2.DatabaseError as e:
            logging.warning("Failed to connect to database: {}".format(e))
            raise psycopg2.DatabaseError

    @staticmethod
    def latest_top_games_update(conn):
        """
        Returns unix timestamp of the last aggregation update.

        :return:
        """
        sql = ('SELECT COALESCE(MAX(epoch), 0) '
               'FROM twitch_top_games ')
        cursor = conn.cursor()
        cursor.execute(sql)
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
            {"timestamp": {"$gt": start}}
        ).sort("timestamp", pymongo.ASCENDING)
        if cursor.count() > 0:
            return int(cursor[0]['timestamp'])
        else:
            return 1 << 31

    def mongo_top_games(self, start, end):
        """
        Returns top games entries.

        Returns entries in the twitch_top_games collections from Mongo that
        are timestamped greater than start but less than end.

        :param start: int, Timestamp of the earliest entry
        :param end: int, Timestamp of the last entry
        :return: pymongo.cursor.Cursor
        """
        db = self.client[self.twitch_db['db_name']]
        topgames = db[self.twitch_db['top_games']]
        cursor = topgames.find(
            {"timestamp": {"$gte": start, "$lt": end}}
        ).sort("timestamp", pymongo.ASCENDING)
        return cursor

    @staticmethod
    def agg_top_games_period(entries, start, end):
        """
        Determines the average viewercount of each game over the specified 
        period.  Entries must be in ascending order based on timestamp.
        
        :param entries: cursor,
        :param start: int 
        :param end: int
        :return: dict: keys are game_ids and values are dictionaries where
            'v' is the viewercount and 'name' is the game's name.
        """
        last_timestamp = start
        games = {}
        # Add up the total number of viewer seconds.
        entry = None
        for entry in entries:
            cur_timestamp = entry['timestamp']
            for gameid, game in entry['games'].items():
                if gameid not in games:
                    games[gameid] = {
                        'v': 0,
                        'name': game['name'],
                        'giantbomb_id': game['giantbomb_id']
                    }
                viewersecs = game['viewers']*(cur_timestamp-last_timestamp)
                games[gameid]['v'] += viewersecs
            last_timestamp = cur_timestamp
        # Return if there were no entries
        if not entry:
            return games
        # Add time from last entry
        for gameid, game in entry['games'].items():
            games[gameid]['v'] += game['viewers']*(end-last_timestamp)
        # Convert viewerseconds into the average number of viewers
        for gameid in games:
            games[gameid]['v'] //= (end-start)
        return games

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
        sql = ('INSERT INTO twitch_top_games '
               'VALUES {} '
               'ON CONFLICT DO NOTHING ')
        values = []
        for id, game in games.items():
            tup = (int(id), timestamp, game['v'])
            values.append(cursor.mogrify("(%s,%s,%s)", tup).decode())
        query = sql.format(','.join(values))
        cursor.execute(query)
        logging.debug("Top games stored from: " + self.strtime(timestamp))

    @staticmethod
    def strtime(timestamp):
        tz = pytz.timezone('US/Pacific')
        dt = datetime.fromtimestamp(timestamp, tz)
        return dt.strftime("%Z - %Y/%m/%d, %H:%M:%S")

    def store_game_ids(self, games, conn):
        """
        Stores the game ids specified in games or does nothing if they
        already are stored.  Does not commit.

        :param games: dict, Output from agg_top_games_period
        :param conn: psycog2.Connection, database connection
        :return:
        """
        if not games:
            return
        sql = ('INSERT INTO games (game_id, name, giantbomb_id)'
               'VALUES {} '
               'ON CONFLICT DO NOTHING')
        curs = conn.cursor()
        values = []
        for id, game in games.items():
            tup = (id, game['name'], game['giantbomb_id'])
            values.append(curs.mogrify("(%s, %s, %s)", tup).decode())
        query = sql.format(','.join(values))
        curs.execute(query)

    def agg_top_games(self):
        """

        :return:
        """
        # start is the first second of the next hour that we need to aggregate
        # end is the last second of the most recent full hour
        conn = psycopg2.connect(host=self.postgres['host'],
                                port=self.postgres['port'],
                                user=self.postgres['user'],
                                password=self.postgres['passwd'],
                                dbname=self.postgres['db_name'])
        self.client = MongoClient(self.twitch_db['host'], self.twitch_db['port'])
        sechr = 3600
        start = self.latest_top_games_update(conn) + sechr
        end = self.epoch_to_hour(time.time())
        earliest_entry = self.first_entry_after(start)
        curhrstart = earliest_entry//sechr*sechr
        curhrend = curhrstart + sechr
        while curhrend < end:
            entries = self.mongo_top_games(curhrstart, curhrend)
            games = self.agg_top_games_period(entries, curhrstart, curhrend)
            # Some hours empty due to server failure
            if games:
                self.store_game_ids(games, conn)
                self.store_top_games(games, curhrstart, conn)
            curhrstart += sechr
            curhrend += sechr
        conn.commit()
        conn.close()
        self.client.close()

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

    def agg_broadcasts(self):
        pass

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
    print("Total Time: {}".format(int(end-start)))