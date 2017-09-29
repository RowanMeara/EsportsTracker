import yaml
import requests
import time
import logging
import sys
from pymongo import MongoClient
import pymongo

from .apiclients import YouTubeAPIClient
from .dbinterface import MongoManager
from .models.mongomodels import YTLivestreams

DEBUG = True


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
