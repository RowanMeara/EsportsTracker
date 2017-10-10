import time
from ruamel import yaml
from datetime import datetime
import pytz
import logging
from .dbinterface import PostgresManager, MongoManager
from .models.postgresmodels import *
from .models.mongomodels import *
from .classifiers import YoutubeIdentifier
import requests

class Aggregator:
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
        self.yti = YoutubeIdentifier()
        self.mongo_user = None
        self.mongo_pwd = None
        if 'mongodb' in keys:
            self.mongo_user = keys['mongodb']['read']['user']
            self.mongo_pwd = keys['mongodb']['read']['pwd']

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
        # Need entries in ascending order
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

    def agg_twitch_games(self):
        """
        Aggregates and stores twitch game info.

        Checks the MongoDB specified in the config file for new top games
        entries, aggregates them, and stores them in Postgres.  initdb Must
        be called before calling this function.

        :return: None
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
        curhrstart, curhrend, last = self._agg_ts(man, mongo,
                                                  'twitch_game_vc',
                                                  self.twitchgamescol)
        while curhrend <= last:
            docs = mongo.docsbetween(curhrstart, curhrend,
                                     self.twitchgamescol)
            apiresp = [TwitchGamesAPIResponse.fromdoc(doc) for doc in docs]
            vcs = self.average_viewers(apiresp, curhrstart, curhrend)
            vcs = TwitchGameVC.from_vcs(vcs, curhrstart)
            # Some hours empty due to server failure
            if apiresp:
                games = Game.api_responses_to_games(apiresp).values()
                man.store_rows(games, 'game')
                man.store_rows(vcs, 'twitch_game_vc')
            curhrstart += 3600
            curhrend += 3600
            if curhrstart % 36000 == 0:
                man.commit()
        man.commit()
        mongo.client.close()

    def agg_twitch_broadcasts(self):
        """
        Retrieves, aggregates, and stores twitch broadcasts.

        Checks the MongoDB specified in the config file for new twitch
        broadcasts, aggregates them, and stores them in Postgres.

        :return:
        """
        man = PostgresManager.from_config(self.postgres, self.esportsgames)
        mongo = MongoManager(self.mongo_host,
                             self.mongo_port,
                             self.twitch_db['db_name'],
                             self.mongo_user,
                             self.mongo_pwd,
                             self.mongo_ssl)
        hrstart, hrend, last = self._agg_ts(man, mongo,
                                            'twitch_stream',
                                            self.twitchstreamscol)
        while hrend <= last:
            docs = mongo.docsbetween(hrstart, hrend,
                                     self.twitchstreamscol)
            apiresp = [TwitchStreamsAPIResponse.fromdoc(doc) for doc in docs]
            # Need to sort responses by game
            sortedbygame = {}
            for resp in apiresp:
                if resp.gameid not in sortedbygame:
                    sortedbygame[resp.gameid] = []
                sortedbygame[resp.gameid].append(resp)

            # Some hours empty due to server failure
            if apiresp:
                vcbygame = []
                for game, resp in sortedbygame.items():
                    vcbygame.append(self.average_viewers(resp, hrstart, hrend))
                vcs = {}
                for vc in vcbygame:
                    vcs.update(vc)
                channels = TwitchChannel.from_api_resp(apiresp).values()
                man.store_rows(channels, 'twitch_channel')
                streams = TwitchStream.from_vcs(apiresp, vcs, hrstart, man)
                man.store_rows(streams, 'twitch_stream')
            hrstart += 3600
            hrend += 3600
            if hrstart % 36000 == 0:
                man.commit()
        man.commit()
        mongo.client.close()

    def agg_youtube_streams(self):
        """
        Retrieves, aggregates, and stores youtube broadcasts.

        Checks the MongoDB specified in the config file for new youtube
        broadcasts, aggregates them, and stores them in Postgres.

        :return:
        """
        man = PostgresManager.from_config(self.postgres, self.esportsgames)
        mongo = MongoManager(self.mongo_host,
                             self.mongo_port,
                             self.youtube_db['db_name'],
                             self.mongo_user,
                             self.mongo_pwd,
                             self.mongo_ssl)
        hrstart, hrend, last = self._agg_ts(man, mongo,
                                            'youtube_stream',
                                            self.ytstreamscol)
        while hrend <= last:
            docs = mongo.docsbetween(hrstart, hrend,
                                     'youtube_streams')
            ls = [YTLivestreams.fromdoc(doc) for doc in docs]
            # Some hours empty due to server failure
            if ls:
                allstreams = [s for streams in ls for s in streams.streams]
                channels = YoutubeChannel.fromstreams(allstreams).values()
                man.store_rows(channels, 'youtube_channel')
                vcs = self.average_viewers(ls, hrstart, hrend)
                streams = YoutubeStream.from_vcs(ls, vcs, hrstart)
                for stream in streams:
                    self.yti.classify(stream)
                man.store_rows(streams, 'youtube_stream')
            hrstart += 3600
            hrend += 3600
            if hrstart % 36000 == 0:
                man.commit()
        man.commit()
        mongo.client.close()

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
        Calls the route which refreshes the Node.js cache.

        :return:
        """
        # TODO: Figure out a better way to do this.
        user = self.postgres['user']
        pwd = self.postgres['password']
        url = f'http://localhost:3000/api/refreshcache?user={user}&pwd={pwd}'
        try:
            requests.get(url)
        except requests.exceptions.ConnectionError:
            logging.warning('Cache refresh error.')
            return

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
            self.agg_twitch_games()
            self.agg_twitch_broadcasts()
            self.agg_youtube_streams()
            end = time.time()
            print("Total Time: {:.2f}".format(end - start))
            self.refreshwebcache()
            timesleep = 3660 - (int(end) % 3600)
            time.sleep(timesleep)
