import time
import yaml
import json
import os
import sys
import collections
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
        most_viewed_livestreams_url = self.base_url + '/search'
        params = {
            'part': 'snippet',
            'maxResults': 5,
            'order': 'viewCount',
            'type': 'video',
            'eventType': 'live'
        }
        api_result = self.session.get(most_viewed_livestreams_url, params=self._bundle(params))
        print(api_result)
        # print(api_result.text)
        json_result = json.loads(api_result.text)
        broadcasts = {}
        video_ids = [k['id']['videoId'] for k in json_result['items']]
        view_counts = self.get_livestream_view_count(video_ids)
        for broadcast in json_result['items']:
            broadcasts[broadcast['snippet']['channelId']] = {
                'title': broadcast['snippet']['title'],
                'broadcaster_name': broadcast['snippet']['channelTitle'],
                'broadcast_id': broadcast['id']['videoId'],
                'concurrent_viewers': view_counts[broadcast['id']['videoId']]
            }
        return(broadcasts)
        # Get the view count now that we have retrieved the most viewed things


    def get_livestream_view_count(self, broadcast_ids):
        """
        Gets the number of current viewers of the specified broadcasts.

        If one or more of the broadcast_id's are invalid, the returned
        dictionary will have a value of -1 for those ids.  If it is a valid
        broadcast but it is over the value will be 0 instead.

        :param broadcast_ids: list, A list of up to 50 youtube video_id's
            corresponding to current live broadcasts.
        :return: dict, Video_id's are keys and values are the viewer count.
        """

        api_url = self.base_url + '/videos'
        params = {
            'part': 'liveStreamingDetails',
            'id': ','.join(broadcast_ids)
        }
        api_result = self.session.get(api_url, params=self._bundle(params))
        json_result = json.loads(api_result.text)
        res = {k:(-1) for k in broadcast_ids}
        for broadcast in json_result['items']:
            if 'concurrentViewers' in broadcast['liveStreamingDetails']:
                res[broadcast['id']] = broadcast['liveStreamingDetails']['concurrentViewers']
            else:
                res[broadcast['id']] = 0
        return res

if __name__ == "__main__":
    a = YoutubeScraper()
    #a.get_channel_ids(['LoLChampSeries'])
    #a.get_viewers('UCvqRdlKsE5Q8mf8YXbdIJLw')
    #a.get_viewers('UC2wKfjlioOCLP4xQMOWNcgg')
    print(a.get_top_livestreams('hello'))
    #a.get_livestream_view_count(['wNBhjL6uQXM', 'E2FU-A0bARo'])