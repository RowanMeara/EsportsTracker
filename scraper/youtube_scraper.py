import yaml
import json
import requests
import time
import logging
import sys
import math
from pymongo import MongoClient

DEBUG = True


class YoutubeScraper:
    def __init__(self, config_path='scraper_config.yml',
                 key_file_path='../keys.yml'):
        with open(key_file_path) as f:
            keys = yaml.load(f)
            self.client_id = keys['youtubeclientid']
            self.secret = keys['youtubesecret']
        with open(config_path) as f:
            config = yaml.load(f)['youtube']
            self.update_interval = config['update_interval']
            self.db_port = config['db']['port']
            self.db_host = config['db']['host']
            self.db_name = config['db']['db_name']
            self.db_streams = config['db']['top_streams']
            self.base_url = config['api']['base_url']
            # Max number is 50
            self.res_per_request = config['api']['num_results_per_request']
            self.gaming_category_id = 20

        self.base_params = {'Client-ID': self.client_id, 'key': self.secret}
        self.session = requests.session()

    def make_api_request(self, url, params):
        for i in range(3):
            api_result = self.session.get(url, params=self._bundle(params))
            if api_result.status_code == requests.codes.okay:
                return api_result
            elif i == 2:
                logging.WARNING("Youtube API subrequest failed: {}".format(
                    api_result.status_code))
                raise ConnectionError
            time.sleep(10)
        # TODO: Implement a more sophisticated failure mechanism

    def _bundle(self, params):
        """
        Combines the params dictionary and the YoutubeScraper's key dictionary.
        """
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
            api_result = self.make_api_request(url, params=self._bundle(params))
            # TODO: Add error checking
            json_result = json.loads(api_result.text)
            results[username] = json_result['items'][0]['id']
        return results

    def get_top_livestreams(self, num_pages=1):
        """
        Retrieves top gaming live streams and their view counts.

        Currently youtube restricts the maximum number of results to 100 even
        with pagination.  This may be because the API is potentially pulling
        from an internal playlist rather than doing a search even though the
        search api call is used. Youtube's live playlists only contain 100
        channels.
        Youtube allows you to search by game and order by view count but
        retrieving the exact view count requires a separate api call.

        :param num_pages: int, The total number of results returned will be
            the number of pages multiplied by the results per request. Must
            be at least 1.
        :return: dict
        """
        most_viewed_livestreams_url = self.base_url + '/search'
        params = {
            'part': 'snippet',
            'maxResults': self.res_per_request,
            'order': 'viewCount',
            'type': 'video',
            'videoCategoryId': self.gaming_category_id,
            'eventType': 'live',
            'regionCode': 'US',
            'relevantLanguage': 'en',
            'safeSearch': 'none'
        }
        raw_broadcasts = []
        for i in range(num_pages):
            api_result = self.make_api_request(most_viewed_livestreams_url, params)
            json_result = json.loads(api_result.text)
            params['pageToken'] = json_result['nextPageToken']
            raw_broadcasts += json_result['items']
            # The Youtube API seems to limit results to 100 while still
            # providing the pageToken.  Check if the result is empty to avoid
            # unnecessary api calls.
            if len(json_result['items']) < self.res_per_request:
                break
        if DEBUG:
            print("API result: {}".format(api_result))

        broadcasts = {}
        video_ids = [k['id']['videoId'] for k in raw_broadcasts]
        view_counts = self.get_livestream_view_count(video_ids)
        for broadcast in raw_broadcasts:
            broadcasts[broadcast['snippet']['channelId']] = {
                'timestamp': time.time(),
                'title': broadcast['snippet']['title'],
                'broadcaster_name': broadcast['snippet']['channelTitle'],
                'broadcast_id': broadcast['id']['videoId'],
                'concurrent_viewers': view_counts[broadcast['id']['videoId']]
            }
        return broadcasts

    def store_top_livestreams(self, top_livestreams):
        db = MongoClient(self.db_host, self.db_port)[self.db_name]
        collection = db[self.db_streams]
        db_result = collection.insert_one(top_livestreams)
        if DEBUG:
            print(db_result.inserted_id)
            print("Inserted: ", top_livestreams)

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
        params = []
        for i in range(math.ceil(len(broadcast_ids)/self.res_per_request)):
            start = i*self.res_per_request
            end = min((i+1)*self.res_per_request, len(broadcast_ids))
            request_ids = broadcast_ids[start:end]
            request_params = {
                'part': 'liveStreamingDetails,topicDetails,snippet,contentDetails',
                'id': ','.join(request_ids)
            }
            params.append(request_params)

        broadcasts = []
        for req_params in params:
            api_result = self.make_api_request(api_url, req_params)
            json_result = json.loads(api_result.text)
            broadcasts += json_result['items']

        view_counts = {bc_id: -1 for bc_id in broadcast_ids}
        for broadcast in broadcasts:
            if 'concurrentViewers' in broadcast['liveStreamingDetails']:
                viewers = broadcast['liveStreamingDetails']['concurrentViewers']
                view_counts[broadcast['id']] = viewers
            else:
                view_counts[broadcast['id']] = 0
        return view_counts

    def scrape(self):
        while True:
            start_time = time.time()
            try:
                res = a.get_top_livestreams(5)
                a.store_top_livestreams(res)
            except ConnectionError:
                logging.warning("Youtube API Failed: {}".format(time.time()))
            if DEBUG:
                print("Elapsed time: {:.2f}s".format(time.time() - start_time))
            time_to_sleep = self.update_interval - (time.time() - start_time)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

if __name__ == "__main__":
    fmt = '%(asctime)s %(levelname)s:%(message)s'
    logging.basicConfig(format=fmt, filename='youtube.log',
                        level=logging.DEBUG)
    a = YoutubeScraper()
    while True:
        try:
            a.scrape()
        except:
            logging.warning("Unexpected error: {}. Time: {}".format(
                sys.exc_info()[0], time.time()))
