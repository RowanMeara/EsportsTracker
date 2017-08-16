from ruamel import yaml
import requests
import sys
import os
import json
import time
import pymongo
from pymongo import MongoClient
import logging

DEBUG = True


# noinspection PyTypeChecker
class TwitchScraper:
    """
    Makes twitch api calls and stores the partially filtered results in a
    MongoDB collection.  Data is stored in a raw form, and more processing is
    needed before using due to the significant amount of information collected.
    """
    def __init__(self, config_path='src/scraper_config.yml',
                 key_path='keys.yml'):
        """
        Constructor for TwitchScraper.

        :param config_path: Path to the config file.  See repository for
            examples.
        :param key_path: Path to a yaml file which contains a 'twitchclientid'
            field and a 'twitchclientsecret' field.
        """
        self.config_path = config_path
        with open(config_path) as f:
            config = yaml.safe_load(f)['twitch']
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
            self.game_ids_url = config['api']['game_ids']
            self.esports_channels = config['esports_channels']
            self.games = self.esports_channels.keys()
        with open(key_path) as f:
            keys = yaml.safe_load(f)
            self.client_id = keys['twitchclientid']
            self.secret = keys['twitchsecret']

        # Install twitch authentication
        client_header = {'Client-ID': self.client_id}
        api_version_header = {'Accept': self.api_version_url}

        self.session = requests.Session()
        self.session.headers.update(client_header)
        self.session.headers.update(api_version_header)

    def check_userids(self):
        """
        Retrieves user IDs for channels that are only specified by name in
        the config file.

        Gets the userids associated with the channel names specified in the
        config file.  The user IDs are then dumped into the config file for
        later use.  Nothing is written if there are no ids to retrieve.
        Allows channels to be specified in the config file as:

        - name: channel_display_name

        :return: Boolean, True if user IDs were retrieved.
        """
        # TODO: Make function clearer
        new_ids_found = False
        with open(self.config_path) as f:
            data = yaml.safe_load(f)
            esports_channels = data['twitch']['esports_channels']
        for game_name, game in esports_channels.items():
            display_names = []
            if not game:
                continue
            for broadcaster_org, channels in game.items():
                for channel in channels:
                    if 'id' not in channel:
                        display_names.append(channel['name'])
            if not display_names:
                continue

            # Make API request to retrieve user IDs
            url = self.user_id_url.format(','.join(display_names))
            resp = self.twitch_api_request(url)
            api_resp_channels = json.loads(resp.text)['users']
            ids = {}
            for channel in api_resp_channels:
                ids[channel['name']] = channel['_id']

            # Modify the config that is going to be dumped
            for broadcaster_org, channels in game.items():
                for channel in channels:
                    if channel['name'] in ids:
                        new_ids_found = True
                        channel['id'] = ids[channel['name']]

        if new_ids_found:
            with open(self.config_path, 'w') as f:
                yaml.safe_dump(data, default_flow_style=False, stream=f)
            self.esports_channels = data['twitch']['esports_channels']
            return True
        return False

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

        broadcasters = []
        for bc_name, bc_channels in self.esports_channels[game].items():
            for channel in bc_channels:
                if 'id' in channel:
                    broadcasters.append(channel['id'])
        # TODO: investigate the issue surrounding wrong channels.
        for stream in json_result['streams']:
            broadcaster_id = str(stream['channel']['_id'])
            if broadcaster_id not in broadcasters:
                pass
                # TODO: Should I only track esports broadcasts or filter them
                # continue
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
        db = MongoClient(self.db_host, self.db_port)[self.db_name]
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

        db = MongoClient(self.db_host, self.db_port)[self.db_name]
        collection = db[self.db_top_games]
        db_result = collection.insert_one(db_entry)
        if DEBUG:
            print(db_entry)
            print(db_result.inserted_id)

    def scrape(self):
        """
        Runs forever scraping and storing Twitch data.

        :return:
        """
        self.check_userids()
        while True:
            start_time = time.time()
            try:
                a.scrape_top_games()
                for game in self.games:
                    a.scrape_esports_channels(game)
                if DEBUG:
                    print("Elapsed time: {:.2f}s".format(time.time() - start_time))
            except requests.exceptions.ConnectionError:
                logging.warning("Twitch API Failed")
            except pymongo.errors.ServerSelectionTimeoutError:
                logging.warning("Database Error: {}".format(sys.exc_info()[0]))
            time_to_sleep = self.update_interval - (time.time() - start_time)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)


if __name__ == "__main__":
    fmt = '%(asctime)s %(levelname)s:%(message)s'
    logging.basicConfig(format=fmt, filename='twitch.log',
                        level=logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    while True:
        try:
            a = TwitchScraper()
            a.scrape()
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fn = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            err = "{}. File: {}, line {}"
            logging.warning(err.format(exc_type, fn, exc_tb.tb_lineno))
            # TODO: Remove magic number
            time.sleep(60)

    #a = TwitchScraper()
    #a.scrape()