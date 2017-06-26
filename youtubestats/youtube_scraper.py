import time
import yaml
import json
import os
import sys
import requests

DEBUG = True

class YoutubeScraper:
    def __init__(self, config_file_path='youtube_config.yml', key_file_path='../keys.yml'):
        with open(key_file_path) as f:
            keys = yaml.load(f)
            self.client_id = keys['youtubeclientid']
            self.secret = keys['youtubesecret']
        with open(config_file_path) as f:
            config = yaml.load(f)
            self.base_url = config['api']['base_url']

        self.base_params = {'Client-ID': self.client_id, 'key': self.secret}
        self.session = requests.session()

    def _bundle(self, params):
        """Combines the parameter dictionary with the key dictionary."""
        return {**self.base_params, **params}

    def get_channel_ids(self, usernames):
        """
        Gets Youtube IDs from usernames.

        :param usernames: list, list of youtube usernames.
        :return: dictionary, usernames are the keys and youtube IDs are values.
        """
        url = self.base_url + '/channels'
        results = {}
        for username in usernames:
            params = {'part': 'id', 'forUsername': username}
            api_result = self.session.get(url, params=self._bundle(params))
            # TODO: Add error checking
            json_result = json.loads(api_result.text)
            results[username] = json_result['items'][0]['id']
        return results

    def get_top_livestreams(self, game):
        """
        Retrieves the top live streams for each game and their view counts.

        Youtube allows you to search by game and order by view count but retrieving the view
        :param game: string, Must match a game name in youtube config file.
        :return:
        """
        url = self.base_url + '/search'
        params = {
            'part': 'snippet',
            'maxResults': 5,
            'order': 'viewCount',
            'type': 'video',
            'eventType': 'live'
        }
        api_result = self.session.get(url, params=self._bundle(params))
        print(api_result)
        # print(api_result.text)
        json_result = json.loads(api_result.text)
        broadcasts = {}
        for broadcast in json_result['items']:
            broadcasts[broadcast['snippet']['channelId']] = broadcast['snippet']['title']
        print(broadcasts)

    def test(self):

        return

    def get_channel_livestreams(self):
        api_thing ="https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={CHANNEL_ID}&maxResults=10&order=date&type=video&key={YOUR_API_KEY}"
        self.sessions.get(api_thing())


if __name__ == "__main__":
    a = YoutubeScraper()
    #a.get_channel_ids(['LoLChampSeries'])
    #a.get_viewers('UCvqRdlKsE5Q8mf8YXbdIJLw')
    #a.get_viewers('UC2wKfjlioOCLP4xQMOWNcgg')
    a.get_top_livestreams('hello')