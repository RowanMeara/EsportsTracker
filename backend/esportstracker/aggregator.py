import time
import logging
from ruamel import yaml
from datetime import datetime
import pytz
import requests

from .dbinterface import PostgresManager, MongoManager
from .models.mongomodels import *
from .models.postgresmodels import *
from .classifiers import YoutubeIdentifier


class Aggregator:
    """ Aggregates data from MongoDB instance and stores the results."""
    def __init__(self, configpath, keypath):
        self.aggregation_interval = 3600
        self.config_path = configpath
        with open(configpath) as f:
            config = yaml.safe_load(f)
        with open(keypath) as f:
            keys = yaml.safe_load(f)
        self.twitch_db = config['twitch']['db']
        self.youtube_db = config['youtube']['db']
        self.postgres = config['postgres']
        self.esportsgames = set([g['name'] for g in config['esportsgames']])
        self.postgres['user'] = keys['postgres']['user']
        self.postgres['password'] = keys['postgres']['passwd']
        mongo_cfg = config['aggregator']['mongodb']
        self.mongo_host = mongo_cfg['host']
        self.mongo_port = mongo_cfg['port']
        self.mongo_name = mongo_cfg['db_name']
        self.mongo_ssl = mongo_cfg['ssl']
        self.twitchgamescol = 'twitch_top_games'
        self.twitchstreamscol = 'twitch_streams'
        self.ytstreamscol = 'youtube_streams'
        self.mongo_user = None
        self.mongo_pwd = None
        if 'mongodb' in keys:
            self.mongo_user = keys['mongodb']['read']['user']
            self.mongo_pwd = keys['mongodb']['read']['pwd']

    @staticmethod
    def strtime(timestamp):
        """
        Timestamp to US/Pacific string.

        :param timestamp: int, unix epoch
        :return: str,
        """
        tz = pytz.timezone('US/Pacific')
        dt = datetime.fromtimestamp(timestamp, tz)
        return dt.strftime("%Z - %Y/%m/%d, %H:%M:%S")

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

    def refreshwebcache(self):
        """
        Calls the webserver's API route which refreshes the Node.js cache.

        :return:
        """
        user = self.postgres['user']
        pwd = self.postgres['password']
        url = f'http://localhost:3000/api/refreshcache?user={user}&pwd={pwd}'
        try:
            requests.get(url)
        except requests.exceptions.ConnectionError:
            logging.warning('Cache refresh error.')
            return

    def _agg_ts(self, man, mongo, table_name, collname):
        """
        Helper function for aggregation timestamps.

        Calculates the timestamps for curhrstart (the first second of the first
        hour that should aggregated), curhrend (one hour later than
        curhrstart), and last (the first second in the current hour).

        :param man: PostgresManager
        :param mongo: MongoManager
        :param table_name: str, Name of Postgres table
        :param collname: str, Name of Mongo collection
        :return: tuple, (curhrstart, curhrend, last)
        """
        # TODO: Rename function
        sechr = 3600
        start = man.most_recent_epoch(table_name) + sechr
        last = self.epoch_to_hour(time.time())
        earliest_entry = mongo.first_entry_after(start, collname)
        curhrstart = earliest_entry // sechr*sechr
        curhrend = curhrstart + sechr
        return curhrstart, curhrend, last

    def process(self, collection, table, fun):
        """
        Feeds a RowFactory MongoDB docs in 60 minute chunks.

        The documents in the MongoDB collection must have a field named
        timestamp which contains a unix epoch.  Documents are retrieved in
        chunks corresponding to one hour each and then fed to the RowFactory.
        Only documents that have a timestamp greater than the most recent entry
        in the Postgres database are retrieved.
        The resulting rows are stored in the Postgres database.

        :param collection: str, name of the MongoDB collection.
        :param table: str, name of the table to check for the most recent update
            in.
        :param fun: function, a function which takes in MongoDB documents and
            retrieves
        :return:
        """
        # start is the first second of the next hour that we need to aggregate
        # end is the last second of the most recent full hour
        man = PostgresManager.from_config(self.postgres, self.esportsgames)
        mongo = MongoManager(self.mongo_host,
                             self.mongo_port,
                             self.mongo_name,
                             self.mongo_user,
                             self.mongo_pwd,
                             self.mongo_ssl)
        # TODO: Convert the Manager.
        curhrstart, curhrend, last = self._agg_ts(man, mongo,
                                                  table,
                                                  collection)
        while curhrend <= last:
            docs = mongo.docsbetween(curhrstart, curhrend,
                                     collection)
            rows = fun(docs, curhrstart, curhrend)
            for onetype in rows:
                man.store_rows(onetype, True)
            curhrstart += 3600
            curhrend += 3600
            if curhrstart % 36000 == 0:
                man.commit()
        man.commit()
        mongo.client.close()

    def run(self):
        """
        Aggregates viewer count data.

        Runs forever aggregating viewer data that has been retrieved from the
        MongoDB instance and stores the aggregated data in the Postgres
        instance.  Runs on the 1st minute of every hour because the data is
        aggregated in complete hours.  Calls the refresh cache route on the
        Node.js server once complete.

        :return:
        """
        while True:
            start = time.time()
            self.process(self.twitchgamescol, 'twitch_game_vc',
                         RowFactory.twitch_game_viewer_counts)
            self.process(self.twitchstreamscol, 'twitch_stream',
                         RowFactory.twitch_streams)
            self.process(self.ytstreamscol, 'youtube_stream',
                         RowFactory.youtube_streams)
            end = time.time()
            print("Total Time: {:.2f}".format(end - start))
            self.refreshwebcache()
            timesleep = 3660 - (int(end) % 3600)
            time.sleep(timesleep)


class RowFactory:
    """Generates Postgres objects from Mongo docs."""
    @staticmethod
    def average_viewers(entries, start, end):
        """
        Returns the average viewer count over the specified period.

        If one Aggregatabale, is missing from an entry that appeared in an
        earlier entry, its viewcount is treated as zero for that entry.

        :param entries: list[Aggregatable], Entries to be aggregated
        :param start: int, Start of aggregation period
        :param end: int, End of aggregation period
        :return: dict, {id: average_viewers}, Average viewer of each item.
        """
        # Need entries in ascending order.
        entries.sort()

        last_timestamp = start
        entry, res = None, {}
        for entry in entries:
            for name, viewers in entry.viewercounts().items():
                if name not in res:
                    res[name] = 0
                res[name] += viewers * (entry.gettimestamp() - last_timestamp)
            last_timestamp = entry.gettimestamp()
        if not entry:
            return res

        # Need to count the time from the last entry to the end of the period.
        for name, viewers in entry.viewercounts().items():
            res[name] += viewers * (end - last_timestamp)

        for name in res:
            res[name] //= (end-start)
        return res

    @staticmethod
    def twitch_game_viewer_counts(docs, start, end):
        """
        Creates database rows from API responses.

        All documents must have a timestamp value between start and end.  The
        start parameter is start of the aggregation period and end
        is the first second in the next period.

        :param docs: cursor, raw mongodb docs.
        :param start: int, unix epoch.
        :param end: int, unix epoch.
        :return: list(list(Row)), the rows to insert grouped by type.
        """
        if not docs:
            return []
        apiresp = [TwitchGamesAPIResponse.fromdoc(doc) for doc in docs]
        games = Game.from_docs(apiresp)
        vcs = RowFactory.average_viewers(apiresp, start, end)
        vcs = TwitchGameVC.from_vcs(vcs, start)
        return [games, vcs]

    @staticmethod
    def twitch_streams(docs, start, end):
        """
        Creates database rows from API responses.

        All documents must have a timestamp value between start and end.  The
        start parameter is start of the aggregation period and end
        is the first second in the next period.

        :param docs: cursor, raw mongodb docs.
        :param start: int, unix epoch.
        :param end: int, unix epoch.
        :return: list(list(Row)), the rows to insert grouped by type.
        """
        apiresp = [TwitchStreamsAPIResponse.fromdoc(doc) for doc in docs]
        # Need to sort responses by game
        sortedbygame = {}
        for resp in apiresp:
            if resp.game_id not in sortedbygame:
                sortedbygame[resp.game_id] = []
            sortedbygame[resp.game_id].append(resp)
        if not sortedbygame:
            return []

        vcbygame = []
        for game, resp in sortedbygame.items():
            vcbygame.append(RowFactory.average_viewers(resp, start, end))
        vcs = {}
        for vc in vcbygame:
            vcs.update(vc)
        channels = TwitchChannel.from_api_resp(apiresp)
        streams = TwitchStream.from_vcs(apiresp, vcs, start)
        return [channels, streams]

    @staticmethod
    def youtube_streams(docs, start, end):
        """
        Creates database rows from API responses.

        All documents must have a timestamp value between start and end.  The
        start parameter is start of the aggregation period and end
        is the first second in the next period.

        :param docs: cursor, raw mongodb docs.
        :param start: int, unix epoch.
        :param end: int, unix epoch.
        :return: list(list(Row)), the rows to insert grouped by type.
        """
        yti = YoutubeIdentifier()
        ls = [YTLivestreams.fromdoc(doc) for doc in docs]
        # Some hours empty due to server failure
        if not ls:
            return []
        allstreams = [s for streams in ls for s in streams.streams]
        channels = YoutubeChannel.fromstreams(allstreams)
        vcs = RowFactory.average_viewers(ls, start, end)
        streams = YoutubeStream.from_vcs(ls, vcs, start)
        for stream in streams:
            yti.classify(stream)
        return [channels, streams]
