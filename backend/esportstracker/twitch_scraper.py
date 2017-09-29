from ruamel import yaml
import requests
import sys
import os
import json
import time
import pymongo
from pymongo import MongoClient
import logging
import traceback

DEBUG = True


# noinspection PyTypeChecker
class TwitchScraper:
    """
    Makes twitch api calls and stores the partially filtered results in a
    MongoDB collection.  Data is stored in a raw form, and more processing is
    needed before using due to the significant amount of information collected.
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
            self.db_name = config['db']['db_name']
            self.db_top_streams = config['db']['top_streams']
            self.db_top_games = config['db']['top_games']
            self.db_port = config['db']['port']
            self.db_host = config['db']['host']
            self.top_games_url = config['api']['top_games']
            self.live_streams_url = config['api']['live_streams']
            self.api_version_url = config['api']['version']
            self.user_id_url = config['api']['user_ids']

        with open(key_path) as f:
            keys = yaml.safe_load(f)
            self.client_id = keys['twitchclientid']
            self.secret = keys['twitchsecret']
            if 'mongodb' in keys:
                self.mongo_user = keys['mongodb']['write']['user']
                self.mongo_pwd = keys['mongodb']['write']['pwd']

        # Install twitch authentication
        client_header = {'Client-ID': self.client_id}
        api_version_header = {'Accept': self.api_version_url}

        self.session = requests.Session()
        self.session.headers.update(client_header)
        self.session.headers.update(api_version_header)

    def twitch_api_request(self, url, params=None):
        """
        Makes a twitch api request.

        Makes a twitch api request and logs failed requests so that they can be
        addressed and performs other error checks.  If the request fails,
        the request is tried twice more with a ten second wait between each
        request.  The twitch api has been somewhat prone to unreliability in
        my experience.

        :param url: str, Twitch api request
        :param params: dict, params to pass to requests.get
        :return: requests.Response
        """
        for i in range(3):
            if params:
                api_result = self.session.get(url, params=params)
            else:
                api_result = self.session.get(url)
            api_result.encoding = 'utf8'
            if api_result.status_code == requests.codes.okay:
                return api_result
            elif i == 2:
                logging.WARNING("Twitch API subrequest failed: {}".format(
                    api_result.status_code))
                # TODO: Raise more appropriate error
                raise ConnectionError
            time.sleep(10)
        # TODO: Implement a more sophisticated failure mechanism

    def gamename_to_id(self, gamename):
        """
        Returns the Twitch id corresponding to a game.

        Uses the Twitch API to return the game id for the game with the given
        name.

        :param gamename: str, The name of the game
        :return:
        """
        p = {'query': gamename}
        res = self.twitch_api_request(self.game_ids_url, params=p)
        games = json.loads(res.text)['games']
        for game in games:
            if game['name'] == gamename:
                return int(game['_id'])

        raise Exception

    def scrape_top_games(self):
        """
        Makes a twitch API Request and stores the result in MongoDB.

        Retrieves and returns the current viewership and number of broadcasting
        channels for each of the top 100 games by viewer count.

        :return: requests.Response, The response to the API request.
        """
        api_result = self.twitch_api_request(self.top_games_url)
        self.store_top_games(api_result)

    def scrape_esports_channels(self, game):
        """
        Retrieves channel data for one game.

        Scrapes viewership data from all of the twitch channels that are
        designated as esports channels for that game. If a channel is not
        live or playing a different game, api results are not stored for that
        channel. Requests are issued synchronously due to twitch's rate limit.
        Assume that if an esports channel is live, it is among the current 100
        most popular channels for its respective game.

        :param game: str: Name of the desired game, must match config file.
        :return:
        """
        api_result = self.twitch_api_request(self.live_streams_url.format(game))
        self.store_esports_channels(api_result, game)

    def store_esports_channels(self, api_result, game):
        """
        Stores a Twitch livestream information API request.

        :param api_result: requests.Response, Result from twitch API.
        :param game: str, Matches a game specified in the config file.
        :return:
        """
        db_entry = {'timestamp': int(time.time()), 'game': game}
        streams, json_result = {}, json.loads(api_result.text)

        for stream in json_result['streams']:
            streams[str(stream['channel']['_id'])] = {
                'display_name':     stream['channel']['display_name'],
                'viewers':          stream['viewers'],
                'game':             stream['game'],
                'status':           stream['channel']['status'],
                'broadcaster_id':   stream['channel']['_id']
            }
        db_entry['streams'] = streams
        if DEBUG:
            print(db_entry)
        db = self.get_mongoclient()
        collection = db[self.db_top_streams]
        db_result = collection.insert_one(db_entry)
        if DEBUG:
            print(db_result.inserted_id)

    def store_top_games(self, api_result):
        db_entry = {'timestamp': int(time.time())}
        games, json_result = {}, json.loads(api_result.text)
        for game in json_result['top']:
            games[str(game['game']['_id'])] = {
                'name':         game['game']['name'],
                'viewers':      game['viewers'],
                'channels':     game['channels'],
                'id':           game['game']['_id'],
                'giantbomb_id': game['game']['giantbomb_id']
            }
        db_entry['games'] = games

        db = self.get_mongoclient()
        collection = db[self.db_top_games]
        db_result = collection.insert_one(db_entry)
        if DEBUG:
            print(db_entry)
            print(db_result.inserted_id)

    def check_indexes(self):
        """
        Checks if the twitch collections have indexes on timestamp.

        :return:
        """
        db = self.get_mongoclient()
        for collname in [self.db_top_games, self.db_top_streams]:
            coll = db[collname]
            indexes = coll.index_information()
            if 'timestamp_1' not in indexes:
                logging.info('Index not found for collection: ' + collname)
                logging.info('Creating index on collection' + collname)
                coll.create_index('timestamp')

    def scrape(self):
        """
        Runs forever scraping and storing Twitch data.

        :return:
        """
        self.check_indexes()
        while True:
            start_time = time.time()
            try:
                self.scrape_top_games()
                for game in self.esportsgames:
                    self.scrape_esports_channels(game)
                if DEBUG:
                    print("Elapsed time: {:.2f}s".format(time.time() - start_time))
            except requests.exceptions.ConnectionError:
                logging.warning("Twitch API Failed")
            except pymongo.errors.ServerSelectionTimeoutError:
                logging.warning("Database Error: {}".format(sys.exc_info()[0]))
            time_to_sleep = self.update_interval - (time.time() - start_time)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

    def get_mongoclient(self):
        """
        MongoClient wrapper.

        :return: psycopg2.MongoClient
        """
        client = MongoClient(self.db_host, self.db_port)
        if self.mongo_user:
            client[self.db_name].authenticate(self.mongo_user,
                                              self.mongo_pwd,
                                              source='admin')
        return client[self.db_name]