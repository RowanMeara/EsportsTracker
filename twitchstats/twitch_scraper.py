import yaml
import os
import requests
import json
import time
from pymongo import MongoClient


class TwitchScraper:
    top_games_url = 'https://api.twitch.tv/kraken/games/top/?limit=20'

    def __init__(self):
        """
        Loads secret.
        """
        os.chdir('..')
        with open('keys.yml') as f:
            keys = yaml.load(f)
            self.client_id = keys['twitchclientid']
            self.secret = keys['twitchsecret']
        self.client_header = {'Client-ID': '{}'.format(self.client_id)}
        self.api_version_header = {'Accept': 'application/vnd.twitchtv.v5+json'}
        self.session = requests.Session()
        self.session.headers.update(self.client_header)
        self.session.headers.update(self.api_version_header)

    def top_games(self):
        """
        Makes an asynchronous twitch API request that returns the 'top games' and their player counts.
        Returns before the request is complete.

        :return: True
        """
        r = self.session.get(self.top_games_url)
        print(r.text)
        db = MongoClient('localhost', 27017).twitch_stats
        collection = db.top_games
        json_result = json.loads(r.text)
        result = collection.insert_one(json_result)
        print(result.inserted_id)



if __name__ == "__main__":
    a = TwitchScraper()
    while True:
        a.top_games()
        time.sleep(300)
    print("Done.")