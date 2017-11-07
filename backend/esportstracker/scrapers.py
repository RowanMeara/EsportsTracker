import yaml
import requests
import time
import logging
import sys
import pymongo.errors

from .apiclients import YouTubeAPIClient, TwitchAPIClient
from .dbinterface import MongoManager, PostgresManager
from .models.mongomodels import YTLivestreams, TwitchStreamsAPIResponse
from .models.postgresmodels import TwitchChannel


# noinspection PyTypeChecker
class TwitchScraper:
    """
    Scrapes and stores Twitch viewership information.
    """
    def __init__(self, config_path, key_path):
        """
        Constructor for TwitchScraper.

        :param config_path: Path to the config file.  See repository for
            examples.
        :param key_path: Path to a yaml file which contains a 'twitchclientid'
            field and a 'twitchclientsecret' field.
        """
        self.config_path = config_path
        with open(config_path) as f:
            config = yaml.safe_load(f)
            self.esportsgames = config['esportsgames']

            config = config['twitch']
            self.update_interval = config['update_interval']
            dbname = config['db']['db_name']
            dbport = config['db']['port']
            dbhost = config['db']['host']
            apihost = config['api']['host']
        with open(key_path) as f:
            keys = yaml.safe_load(f)
            clientid = keys['twitchclientid']
            secret = keys['twitchsecret']
            user, pwd = None, None
            if 'mongodb' in keys:
                user = keys['mongodb']['write']['user']
                pwd = keys['mongodb']['write']['pwd']

        self.apiclient = TwitchAPIClient(apihost, clientid, secret)
        self.mongo = MongoManager(dbhost, dbport, dbname, user, pwd, False)
        self.mongo.check_indexes()

    def scrape_top_games(self):
        """
        Makes a twitch API Request and stores the result in MongoDB.

        Retrieves and returns the current viewership and number of broadcasting
        channels for each of the top 100 games by viewer count.

        :return: requests.Response, The response to the API request.
        """
        apiresult = self.apiclient.gettopgames()
        m = self.mongo.store(apiresult)
        logging.debug(apiresult)
        logging.debug(m)


    def _scrape_streams(self, game):
        """
        Retrieves stream data for one game.

        Scrapes viewership data from all of the twitch channels that are
        designated as esports channels for that game. If a channel is not
        live or playing a different game, api results are not stored for that
        channel. Requests are issued synchronously due to twitch's rate limit.
        Assume that if an esports channel is live, it is among the current 100
        most popular channels for its respective game.

        :param game: dict: id of the desired game, must match config file.
        :return:
        """
        apiresult = self.apiclient.topstreams(game['id'])
        if apiresult:
            m = self.mongo.store(apiresult)
            logging.debug(apiresult)
            logging.debug(m)
        else:
            logging.warning(f'No API result for game {game}')

    def scrape_esports_games(self):
        for game in self.esportsgames:
            self._scrape_streams(game)

    def scrape(self):
        """
        Runs forever scraping and storing Twitch data.

        :return:
        """
        while True:
            start_time = time.time()
            try:
                self.scrape_top_games()
                self.scrape_esports_games()
                tot_time = time.time() - start_time
                logging.debug('Elapsed time: {:.2f}s'.format(tot_time))
            except requests.exceptions.ConnectionError:
                logging.warning('Twitch API Failed')
            except pymongo.errors.ServerSelectionTimeoutError:
                logging.warning(
                    'Database Error: {}'.format(sys.exc_info()[0]))
            time_to_sleep = self.update_interval - (time.time() - start_time)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)


class TwitchChannelScraper:
    """ Retrieves twitch channel information. """
    def __init__(self, config_path, key_path):
        self.config_path = config_path
        with open(config_path) as f:
            config = yaml.safe_load(f)
            cfg = config
            config = config['twitch_channel_scraper']
            self.update_interval = config['update_interval']
            dbname = config['mongodb']['db_name']
            dbport = config['mongodb']['port']
            dbhost = config['mongodb']['host']
            ssl = config['mongodb']['ssl']
            apihost = config['api']['host']
        with open(key_path) as f:
            keys = yaml.safe_load(f)
            clientid = keys['twitchclientid']
            secret = keys['twitchsecret']
            user, pwd = None, None
            if 'mongodb' in keys:
                user = keys['mongodb']['write']['user']
                pwd = keys['mongodb']['write']['pwd']

        self.apiclient = TwitchAPIClient(apihost, clientid, secret)
        self.mongo = MongoManager(dbhost, dbport, dbname, user, pwd, ssl)
        self.mongo.check_indexes()
        self.postgres = cfg['postgres']
        self.esportsgames = set([g['name'] for g in cfg['esportsgames']])
        self.postgres['user'] = keys['postgres']['user']
        self.postgres['password'] = keys['postgres']['passwd']
        self.pg = PostgresManager.from_config(self.postgres, self.esportsgames)

    def store_channel_info(self, channel_id):
        """
        Gets channel from MongoDB and stores info in Postgres.

        :param channel_id: int, Twitch channel id.
        :return:
        """
        doc = self.mongo.get_twitch_channel(channel_id)
        row = TwitchChannel(**doc.todoc())
        update_fields = ['display_name', 'description', 'followers', 'login',
                         'broadcaster_type', 'type', 'offline_image_url',
                         'profile_image_url']
        self.pg.update_rows(row, update_fields)

    def get_missing_channels(self, channel_ids):
        """
        Checks the channel_ids against the Mongo database and gets any missing
        channels from the Twitch API.

        :param channel_ids: list(channel_ids), list of channel_ids.
        :return: int, the number of channels that were new.
        """
        new_channel_count = 0
        start = time.time()
        for channel_id in channel_ids:
            if self.mongo.contains_channel(channel_id):
                self.store_channel_info(channel_id)
                continue
            new_channel_count += 1
            docs = self.apiclient.channelinfo(channel_id)
            if not docs:
                logging.debug(f'Channel {channel_id} no longer exists')
                # Mark channels that no longer exist so that we only attempt
                # to retrieve them once.
                channel = TwitchChannel(channel_id, description='BANNED')
                self.pg.update_rows(channel, 'description')
                continue
            res = self.mongo.store(docs.values())
            if new_channel_count % 30 == 0:
                tot = time.time() - start
                logging.debug(res)
                logging.debug(
                    'Retrieved {} channels in {:.2f}s -- {:.2f}c/s'.format(
                        new_channel_count, tot, new_channel_count/tot
                ))
                self.pg.commit()
            self.store_channel_info(channel_id)
        self.pg.commit()
        return new_channel_count

    def scrape(self):
        """
        Runs forever scraping channels.

        This should not be run on a server that also uses the Twitch API for
        something else because this will use all of the IP address's API quota.

        :return:
        """
        while True:
            start_time = time.time()
            try:
                channel_ids = self.pg.null_twitch_channels(50000)
                self.get_missing_channels(channel_ids)
                tot_time = time.time() - start_time
                logging.debug('Elapsed time: {:.2f}s'.format(tot_time))
            except requests.exceptions.ConnectionError:
                logging.warning('Twitch API Failed')
            except pymongo.errors.ServerSelectionTimeoutError:
                logging.warning(
                    'Database Error: {}'.format(sys.exc_info()[0]))
            time_to_sleep = self.update_interval - (time.time() - start_time)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)


class YouTubeScraper:
    """
    Class for scraping and storing YouTube livestream data.
    """
    def __init__(self, config_path, key_file_path):
        """
        YouTubeScraper constructor

        :param config_path: str, path to config file.
        :param key_file_path: str, path to key file.
        """
        with open(key_file_path) as f:
            keys = yaml.load(f)
        with open(config_path) as f:
            config = yaml.load(f)['youtube']

        dbport = config['db']['port']
        dbhost = config['db']['host']
        dbname = config['db']['db_name']
        user, pwd = None, None
        if 'mongodb' in keys:
            user = keys['mongodb']['write']['user']
            pwd = keys['mongodb']['write']['pwd']
        self.db = MongoManager(dbhost, dbport, dbname, user, pwd, False)
        self.db.check_indexes()

        clientid = keys['youtubeclientid']
        secret = keys['youtubesecret']
        apihost = config['api']['base_url']
        self.apiclient = YouTubeAPIClient(apihost, clientid, secret)

        self.update_interval = config['update_interval']

    def scrapelivestreams(self):
        """
        Retrieves and store livestream information.

        Retrieves the top 100 livestreams from YouTube Gaming and stores
        information about them in the MongoDB database.

        :return: None
        """
        res = self.apiclient.most_viewed_gaming_streams(100)
        doc = YTLivestreams(res, int(time.time()))
        mongores = self.db.store(doc)
        logging.debug(mongores)
        logging.debug(doc)

    def scrape(self):
        while True:
            start_time = time.time()
            try:
                self.scrapelivestreams()
            except requests.exceptions.ConnectionError:
                logging.warning('Youtube API Failed')
            except pymongo.errors.ServerSelectionTimeoutError:
                logging.warning('Database Error: {}. Time: {}'.format(
                    sys.exc_info()[0], time.time()))
            total_time = time.time() - start_time
            logging.debug('Elapsed time: {:.2f}s'.format(total_time))
            time_to_sleep = self.update_interval - (time.time() - start_time)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
