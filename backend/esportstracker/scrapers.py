import yaml
import requests
import time
import logging
import sys
import pymongo.errors
from abc import ABC, abstractmethod

from .apiclients import YouTubeAPIClient, TwitchAPIClient
from .dbinterface import MongoManager, PostgresManager
from .models.mongomodels import YTLivestreams, TwitchStreamsAPIResponse
from .models.postgresmodels import TwitchChannel


class Scraper(ABC):
    """
    ABC for Scraper instances.

    Scrapers should retrieve some information using the API
    """
    @abstractmethod
    def run(self):
        """
        Performs one set of scraping actions (ie pull current viewer counts from
        Twitch).

        Called at regular intervals from the scrape function.
        :return:
        """

    def scrape(self):
        """
        Calls the run function at regular intervals and sleeps in between.

        run() is called every update_interval minutes.
        :return:
        """
        while True:
            start_time = time.time()
            try:
                self.run()
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


# noinspection PyTypeChecker
class TwitchScraper(Scraper):
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
        with open(key_path) as f:
            keys = yaml.safe_load(f)
            user, pwd = None, None
            if 'mongodb' in keys:
                user = keys['mongodb']['write']['user']
                pwd = keys['mongodb']['write']['pwd']

        self.apiclient = TwitchAPIClient(config['api']['host'],
                                         keys['twitchclientid'],
                                         keys['twitchsecret'])
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

        Scrapes viewership data from all esports titles defined in the config
        file.

        :param game: dict: name and id of the game in the format
            {id: int, name: str}.
        :return: None
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

    def run(self):
        self.scrape_top_games()
        self.scrape_esports_games()


class TwitchChannelScraper(Scraper):
    """ Retrieves twitch channel information. """
    def __init__(self, config_path, key_path):
        self.config_path = config_path
        with open(config_path) as f:
            config = yaml.safe_load(f)
            esg = set([game['name'] for game in config['esportsgames']])
            postgres = config['postgres']
            config = config['twitch_channel_scraper']
            self.update_interval = config['update_interval']
            mongo = config['mongodb']
        with open(key_path) as f:
            keys = yaml.safe_load(f)
            postgres['user'] = keys['postgres']['user']
            postgres['password'] = keys['postgres']['passwd']
            user, pwd = None, None
            if 'mongodb' in keys:
                user = keys['mongodb']['write']['user']
                pwd = keys['mongodb']['write']['pwd']

        self.apiclient = TwitchAPIClient(config['api']['host'],
                                         keys['twitchclientid'],
                                         keys['twitchsecret'])
        self.mongo = MongoManager(mongo['host'], mongo['port'],
                                  mongo['db_name'], user, pwd, mongo['ssl'])
        self.mongo.check_indexes()

        self.pg = PostgresManager.from_config(postgres, esg)

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
        self.pg.commit()

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
                # TODO: Consider storing banned status in the Mongo Database.
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
            self.store_channel_info(channel_id)
        return new_channel_count

    def run(self):
        channel_ids = self.pg.null_twitch_channels(50000)
        self.get_missing_channels(channel_ids)


class YouTubeScraper(Scraper):
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
        self.update_interval = config['update_interval']

        user, pwd = None, None
        if 'mongodb' in keys:
            user = keys['mongodb']['write']['user']
            pwd = keys['mongodb']['write']['pwd']
        self.db = MongoManager(config['db']['host'],
                               config['db']['port'],
                               config['db']['db_name'],
                               user, pwd, False)
        self.db.check_indexes()

        self.apiclient = YouTubeAPIClient(config['api']['base_url'],
                                          keys['youtubeclientid'],
                                          keys['youtubesecret'])

    def run(self):
        """
        Retrieves and stores YouTube livestream information.

        Retrieves the top 100 livestreams from YouTube Gaming and stores
        information about them in the MongoDB database.

        :return: None
        """
        res = self.apiclient.most_viewed_gaming_streams(100)
        doc = YTLivestreams(res, int(time.time()))
        mongores = self.db.store(doc)
        logging.debug(mongores)
        logging.debug(doc)