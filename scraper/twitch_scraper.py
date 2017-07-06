import yaml
import requests
import json
import time
from pymongo import MongoClient
import logging

DEBUG = False


class TwitchScraper:
    """
    Makes twitch api calls and stores the partially filtered results in a
    MongoDB collection.  Data is stored in a raw form, and more processing is
    needed before using due to the significant amount of information collected.
    """
    def __init__(self, config_path='scraper_config.yml', key_path='../keys.yml'):
        with open(config_path) as f:
            config = yaml.load(f)['twitch']
            self.db_name = config['db']['db_name']
            self.db_top_streams = config['db']['top_streams']
            self.db_top_games = config['db']['top_games']
            self.db_port = config['db']['port']
            self.db_host = config['db']['host']
            self.top_games_url = config['api']['top_games']
            self.live_streams_url = config['api']['live_streams']
            self.api_version_url = config['api']['version']
            self.user_id_url = config['api']['user_ids']
            self.esports_channels = config['esports_channels']
        with open(key_path) as f:
            keys = yaml.load(f)
            self.client_id = keys['twitchclientid']
            self.secret = keys['twitchsecret']

        # Install twitch authentication
        client_header = {'Client-ID': self.client_id}
        api_version_header = {'Accept': self.api_version_url}

        self.session = requests.Session()
        self.session.headers.update(client_header)
        self.session.headers.update(api_version_header)

    def get_userids(self):
        """
        Converts the user names specified in the config file into user IDs.

        Gets the userids associated with the channel names specified in the
        config file.  The IDs are then dumped for later use.
        :return:
        """
        return


    def twitch_api_request(self, url):
        """
        Makes a twitch api request.

        Makes a twitch api request and logs failed requests so that they can be
        addressed and performs other error checks.  If the request fails,
        the request is tried twice more with a ten second wait between each
        request.  The twitch api has been somewhat prone to unreliability in
        my experience.

        :param url: str, Twitch api request
        :return: requests.Response
        """
        for i in range(3):
            api_result = self.session.get(url)
            if api_result.status_code == requests.codes.okay:
                return api_result
            elif i == 2:
                logging.WARNING("Twitch API subrequest failed: {}".format(
                    api_result.status_code))
                raise ConnectionError
            time.sleep(10)
        # TODO: Implement a more sophisticated failure mechanism

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
        with open('scraper_config.yml') as f:
            channel_list = yaml.load(f)['twitch']['esports_channels'][game]

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
            broadcaster_name = stream['channel']['display_name'].lower()
            if broadcaster_name not in self.esports_channels[game]:
                pass
                # TODO: Should I only track esports broadcasts or filter them
                #       out later?
                # continue
            # TODO: Convert to a pure broadcast_id format.
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
            games[str(game['game']['giantbomb_id'])] = {
                'name':     game['game']['name'],
                'viewers':  game['viewers'],
                'channels': game['channels']
            }
        db_entry['games'] = games

        db = MongoClient(self.db_host, self.db_port)[self.db_name]
        collection = db[self.db_top_games]
        db_result = collection.insert_one(db_entry)
        if DEBUG:
            print(db_result.inserted_id)

    def aggregate_top_game_results(self, results):
        # Aggregates the specified MongoDB results and returns a dictionary with
        # the results

        return


if __name__ == "__main__":
    a = TwitchScraper()
    logging.basicConfig(filename='twitch.log', level=logging.WARNING)
    while True:
        start_time = time.time()
        try:
            a.scrape_top_games()
            a.scrape_esports_channels('League of Legends')
            a.scrape_esports_channels('Dota 2')
            a.scrape_esports_channels('Overwatch')
            a.scrape_esports_channels('Counter-Strike: Global Offensive')
            if DEBUG:
                print("Elapsed time: {:.2f}s".format(time.time() - start_time))
        except ConnectionError as e:
            logging.warning("Twitch API Failed: {}".format(time.time()))
        time.sleep(300 - (time.time() - start_time))
