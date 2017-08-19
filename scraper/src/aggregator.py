import time
import logging
from ruamel import yaml
import pymongo
from pymongo import MongoClient
from datetime import datetime
import pytz
from .models import *
from .dbinterface import PostgresManager


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
        self.esports_games = set(config['twitch']['esports_channels'].keys())
        self.postgres['user'] = keys['postgres']['user']
        self.postgres['password'] = keys['postgres']['passwd']

        self.client = None

    def first_entry_after(self, start, collname):
        """
        Returns the timestamp of the first entry after start.

        The instance variable client must be initialized to a MongoClient.

        :param start: int
        :param collname: str
        :return: int
        """
        db = self.client[self.twitch_db['db_name']]
        topgames = db[collname]
        cursor = topgames.find(
            {'timestamp': {'$gt': start}}
        ).sort('timestamp', pymongo.ASCENDING)
        if cursor.count() > 0:
            return int(cursor[0]['timestamp'])
        else:
            return 1 << 31

    def docsbetween(self, start, end, collname):
        """
        Returns cursor to entries with timestamps between start and end.

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
    def average_viewers(entries, start, end):
        """
        Returns the average viewer count over the specified period.

        :param entries: list[Aggregatable], Entries to be aggregated
        :param start: int, Start of aggregation period
        :param end: int, End of aggregation period
        :return: dict, {id: average_viewers}, Average viewer of each item.
        """
        # Need entries in ascending order
        entries.sort()

        last_timestamp = start
        entry, res = None, {}
        for entry in entries:
            for name, viewers in entry.viewercounts().items():
                if name not in res:
                    res[name] = 0
                res[name] += viewers * (entry.timestamp() - last_timestamp)
            last_timestamp = entry.timestamp()
        if not entry:
            return res

        # Need to count the time from the last entry to the end of the period
        for name, viewers in entry.viewercounts().items():
            res[name] += viewers * (end - last_timestamp)

        for name in res:
            res[name] //= (end-start)
        return res

    @staticmethod
    def strtime(timestamp):
        tz = pytz.timezone('US/Pacific')
        dt = datetime.fromtimestamp(timestamp, tz)
        return dt.strftime("%Z - %Y/%m/%d, %H:%M:%S")

    @staticmethod
    def store_twitch_channel(streams, conn):
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
        query = ('INSERT INTO twitch_channel (channel_id, name) '
                 'VALUES {} '
                 'ON CONFLICT DO NOTHING')
        curs = conn.cursor()
        values = []
        for chid, stream in streams.items():
            tup = (chid, stream['name'])
            values.append(curs.mogrify("(%s, %s)", tup).decode())
        query = query.format(','.join(values))
        curs.execute(query)

    def store_broadcasts(self, streams, timestamp, conn):
        """
        Stores top game entries using conn.

        There is no error checking in this function so the games id field
        must already exist in the games table.  Call store_games on the
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
        # TODO: Figure out source of inconsistent naming data
        if name not in self.esports_games:
            for game in self.esports_games:
                if name.lower() == game.lower():
                    name = game
                    break
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
        curhrstart, curhrend, last = self._agg_ts(conn,
                                                  'twitch_game_viewer_count',
                                                  self.twitch_db['top_games'])
        while curhrend < last:
            docs = self.docsbetween(curhrstart, curhrend,
                                       self.twitch_db['top_games'])
            games = [TwitchGamesAPIResponse(doc) for doc in docs]
            vcs = self.average_viewers(games, curhrstart, curhrend)

            # Some hours empty due to server failure
            if games:
                self.store_games(games, conn)
                self.store_top_games(vcs, curhrstart, conn)
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
        hrstart, hrend, last = self._agg_ts(conn,
                                            'twitch_broadcasts',
                                            self.twitch_db['top_streams'])
        while hrend < last:
            entries = self.docsbetween(hrstart, hrend,
                                       self.twitch_db['top_streams'])
            streams = self.agg_twitch_broadcasts_period(entries, hrstart, hrend)
            # Some hours empty due to server failure
            if streams:
                self.store_twitch_channel(streams, conn)
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
        avgviewers = Aggregator.average_viewers(docs, start, end, 'streams',
                                                'viewers')
        # Format Results
        for id, viewers in avgviewers.items():
            avgviewers[id] = {'viewers': viewers}
        for doc in docs:
            for channelid, stream in doc['streams'].items():
                chnl = avgviewers[channelid]
                chnl['channel_id'] = stream['broadcaster_id']
                chnl['name'] = stream['display_name']
                chnl['stream_title'] = stream['status']
                chnl['game_name'] = stream['game']
        return avgviewers

    def _agg_ts(self, conn, table_name, collname):
        """
        Helper function for aggregation timestamps.

        Calculates the timestamps for curhrstart (the first second of the first
        hour that should aggregated), curhrend (one hour later than
        curhrstart), and last (the first second in the current hour).

        :param conn: psycopg2.connection
        :param table_name: str, Name of Postgres table
        :param collname: str, Name of Mongo collection
        :return: tuple, (curhrstart, curhrend, last)
        """
        # TODO: Rename function
        sechr = 3600
        start = self.last_postgres_update(conn, table_name) + sechr
        last = self.epoch_to_hour(time.time())
        earliest_entry = self.first_entry_after(start, collname)
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

if __name__ == '__main__':
    logformat = '%(asctime)s %(levelname)s:%(message)s'
    logging.basicConfig(format=logformat, level=logging.DEBUG,
                        filename='aggregator.log')
    logging.debug("Aggregator Starting.")
    a = Aggregator()
    a.initdb()
    start = time.time()
    #a.agg_top_games()
    a.agg_twitch_broadcasts()
    end = time.time()
    print("Total Time: {:.2f}".format(end-start))