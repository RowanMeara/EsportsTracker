import yaml
import requests
import json
import time
import urllib
from pymongo import MongoClient

DEBUG = True

class TwitchScraper:
    """
    Makes twitch api calls and stores partially filtered results in a MongoDB collection.
    """
    def __init__(self):
        with open('twitch_config.yml') as f:
            config = yaml.load(f)
            self.db_port = config['db']['port']
            self.db_host = config['db']['host']
            self.top_games_url = config['api']['top_games']
            self.live_streams_url = config['api']['live_streams']
            self.api_version_url = config['api']['version']
            self.esports_channels = config['esports_channels']
        with open('../keys.yml') as f:
            keys = yaml.load(f)
            self.client_id = keys['twitchclientid']
            self.secret = keys['twitchsecret']

        self.client_header = {'Client-ID': '{}'.format(self.client_id)}
        self.api_version_header = {'Accept': '{}'.format(self.api_version_url)}

        self.session = requests.Session()
        self.session.headers.update(self.client_header)
        self.session.headers.update(self.api_version_header)

    def scrape_top_games(self):
        """
        Retrieves and stores the current viewership and number of broadcasting channels
        for each of the top 100 games by viewer count.  Results are stored in a MongoDB collection.

        :return:
        """
        api_result = self.session.get(self.top_games_url)

        db_entry = {'timestamp': int(time.time())}
        games, json_result = {}, json.loads(api_result.text)
        for game in json_result['top']:
            games[str(game['game']['giantbomb_id'])] = {
                'name':     game['game']['name'],
                'viewers':  game['viewers'],
                'channels': game['channels']
            }
        db_entry['games'] = games

        db = MongoClient(self.db_host, self.db_port).twitch_stats
        collection = db.top_games
        db_result = collection.insert_one(db_entry)
        if DEBUG:
            print(db_result.inserted_id)

    def scrape_esports_channels(self, game):
        """
        Scrapes viewership data from all of the twitch channels that are designated as esports channels for that game.
        If the a channel is not live or playing a different game, that channels results are not stored.  Requests are
        issued synchronously due to twitch's rate limit.  Assume that if an esports channel is live, it is among the
        current 100 most popular channels for its respective game.

        :param game: str: name of the desired game, see config file
        :return:
        """
        with open('twitch_config.yml') as f:
            channel_list = yaml.load(f)['esports_channels'][game]
        api_result = self.session.get(self.live_streams_url.format(game))
        if DEBUG:
            print(api_result.text)

        db_entry = {'timestamp': int(time.time()), 'game': game}
        streams, json_result = {}, json.loads(api_result.text)
        for stream in json_result['streams']:
            if not stream['channel']['display_name'].lower() in self.esports_channels[game]:
                pass

            # Recording the id twice in preparation for moving to a pure broadcaster id format.
            streams[str(stream['channel']['_id'])] = {
                'display_name':     stream['channel']['display_name'],
                'viewers':          stream['viewers'],
                'game':             stream['game'],
                'status':           stream['channel']['status'],
                'broadcaster_id':   stream['channel']['_id']
            }
        db_entry['streams'] = streams
        db = MongoClient(self.db_host, self.db_port).twitch_stats
        collection = db.esports_streams
        db_result = collection.insert_one(db_entry)
        if DEBUG:
            print(db_result.inserted_id)


if __name__ == "__main__":
    a = TwitchScraper()
    while True:
        start_time = time.time()
        # a.scrape_top_games()
        a.scrape_esports_channels('League of Legends')
        if DEBUG:
            print("Elapsed time: {:.2f}s".format(time.time() - start_time))
        time.sleep(300 - (time.time() - start_time))
