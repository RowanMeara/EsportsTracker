import yaml
import json
import requests
import time
from pymongo import MongoClient

DEBUG = False


class YoutubeScraper:
    def __init__(self, config_file_path='youtube_config.yml',
                 key_file_path='../keys.yml'):
        with open(key_file_path) as f:
            keys = yaml.load(f)
            self.client_id = keys['youtubeclientid']
            self.secret = keys['youtubesecret']
        with open(config_file_path) as f:
            config = yaml.load(f)
            self.db_port = config['db']['port']
            self.db_host = config['db']['host']
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

    def get_top_livestreams(self):
        """
        Retrieves the top 50 live streams and their view counts.

        Youtube allows you to search by game and order by view count but
        retrieving the exact view count requires a separate api call.

        :return:
        """
        most_viewed_livestreams_url = self.base_url + '/search'
        params = {
            'part': 'snippet',
            'maxResults': 50,
            'order': 'viewCount',
            'type': 'video',
            'eventType': 'live',
            'regionCode': 'US',
            'relevantLanguage': 'en'
        }
        api_result = self.session.get(most_viewed_livestreams_url,
                                      params=self._bundle(params))
        print(api_result)
        json_result = json.loads(api_result.text)
        broadcasts = {}
        video_ids = [k['id']['videoId'] for k in json_result['items']]
        view_counts = self.get_livestream_view_count(video_ids)
        for broadcast in json_result['items']:
            broadcasts[broadcast['snippet']['channelId']] = {
                'timestamp': time.time(),
                'title': broadcast['snippet']['title'],
                'broadcaster_name': broadcast['snippet']['channelTitle'],
                'broadcast_id': broadcast['id']['videoId'],
                'concurrent_viewers': view_counts[broadcast['id']['videoId']]
            }
        return(broadcasts)

    def store_top_livestreams(self, top_livestreams):
        db = MongoClient(self.db_host, self.db_port).esports_stats
        collection = db.youtube_streams
        db_result = collection.insert_one(top_livestreams)
        if DEBUG:
            print(db_result.inserted_id)

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
            'part': 'liveStreamingDetails,topicDetails,snippet,contentDetails',
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
    while True:
        start_time = time.time()
        res = a.get_top_livestreams()
        a.store_top_livestreams(res)
        if DEBUG:
            print(res)
            print("Elapsed time: {:.2f}s".format(time.time() - start_time))
        time.sleep(300 - (time.time() - start_time))
