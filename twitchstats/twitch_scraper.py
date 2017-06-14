import yaml
import requests
import json
import time
from pymongo import MongoClient

DEBUG = False


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
            self.api_version_url = config['api']['version']
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

        :return: True
        """
        api_result = self.session.get(self.top_games_url)

        db_entry = {'timestamp': int(time.time())}
        games, json_result = {}, json.loads(api_result.text)
        for game in json_result['top']:
            games[str(game['game']['giantbomb_id'])] = {
                'name': game['game']['name'],
                'viewers': game['viewers'],
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
        If the a channel is not live or playing a different game, that channels results are not stored.

        :param game: str name of the desired games, must match the one in the config file.
        :return:
        """


if __name__ == "__main__":
    a = TwitchScraper()
    while True:
        start_time = time.time()
        a.scrape_top_games()
        if DEBUG:
            print("Elapsed time: {:2f}".format(time.time() - start_time))
        time.sleep(300 - (time.time() - start_time))
