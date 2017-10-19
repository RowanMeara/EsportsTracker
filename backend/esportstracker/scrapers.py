import yaml
import requests
import time
import logging
import sys
import pymongo.errors

from .apiclients import YouTubeAPIClient, TwitchAPIClient
from .dbinterface import MongoManager
from .models.mongomodels import YTLivestreams

DEBUG = True


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
        self.gamescol = 'twitch_top_games'
        self.streamscol = 'twitch_streams'
        self.channelscol = 'twitch_channels'
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
        m = self.mongo.store(apiresult, self.gamescol)
        if DEBUG:
            print(apiresult)
            print(m)


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
        m = self.mongo.store(apiresult, self.streamscol)
        if DEBUG:
            print(apiresult)
            print(m)

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
                if DEBUG:
                    print("Elapsed time: {:.2f}s".format(
                        time.time() - start_time))
            except requests.exceptions.ConnectionError:
                logging.warning("Twitch API Failed")
            except pymongo.errors.ServerSelectionTimeoutError:
                logging.warning(
                    "Database Error: {}".format(sys.exc_info()[0]))
            time_to_sleep = self.update_interval - (time.time() - start_time)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)


class TwitchChannelScraper(TwitchScraper):
    """ Retrieves twitch channel information. """
    def __init__(self, config_path, key_path):
        super().__init__(config_path, key_path)
        self.latest_checked = 0

    def retrieve_channels(self):
        """
        Retrieves channel information.

        Checks the twitch_streams table and retrieves channel information for
        any channels that are referenced in the twitch_stream collection but do
        not exist in the twitch_channel collection.  Streams are checked in
        chronological order. The timestamp of the last checked stream is
        remembered so that future calls are much faster.

        :return:
        """


    def scrape(self):
        """
        Runs forever scraping channels.

        :return:
        """


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
        mongores = self.db.store(doc, 'youtube_streams')
        if DEBUG:
            print(mongores)
            print(doc)

    def scrape(self):
        while True:
            start_time = time.time()
            try:
                self.scrapelivestreams()
            except requests.exceptions.ConnectionError:
                logging.warning("Youtube API Failed")
            except pymongo.errors.ServerSelectionTimeoutError:
                logging.warning("Database Error: {}. Time: {}".format(
                    sys.exc_info()[0], time.time()))
            if DEBUG:
                print("Elapsed time: {:.2f}s".format(time.time() - start_time))
            time_to_sleep = self.update_interval - (time.time() - start_time)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
