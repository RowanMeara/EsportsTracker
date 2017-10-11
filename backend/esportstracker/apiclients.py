import requests
import time
import logging
import json
import math
from .models.mongomodels import YTLivestream
from .models.mongomodels import TwitchGamesAPIResponse, TwitchStreamsAPIResponse

DEBUG = True


class TwitchAPIClient:
    """
    Makes Twitch API requests.
    """
    API_WINDOW_LENGTH = 60
    DEFAULT_REQUEST_LIMIT = 30
    API_MAX_RESULTS = 100
    def __init__(self, host, clientid, secret):
        """
        TwitchAPIClient Constructor.

        The Twitch API has a rate limit of 30 requests per 60 second period per
        clientID per IP address.

        :param host: str, host url of the api.
        :param clientid: str, Twitch API client id.
        :param secret: str, Twitch API secret.
        """
        self.apiv5host = host + '/kraken'
        self.apiv6host = host + '/helix'
        self.secret = secret

        self.session = requests.session()
        headers = {'Accept': 'application/vnd.twitchtv.v5+json',
                   'Client-ID': clientid}
        self.session.headers.update(headers)

        self.req_remaining = self.DEFAULT_REQUEST_LIMIT
        self.rate_reset = None

        self.gameidcache = {}

    def _request(self, url, params):
        """
        Makes an API request.

        Manages Twitch API rate limits and sleeps until more requests are
        available if they are not.  Rather than rate limiting API requests by
        the clientID, Twitch limits them by clientID and IP address combination.

        :param url: str, the request url.
        :param params: dict, query strings to add to the request.
        :return: requests.Response
        """
        if self.req_remaining == 0:
            sleeptime = self.rate_reset - time.time()
            if sleeptime > 0:
                time.sleep(sleeptime)
        for i in range(3):
            api_result = self.session.get(url, params=params)
            # Requests does not default to utf-8 encoding and a small percentage
            # of the time the utf-8 header is missing.
            api_result.encoding = 'utf8'
            if api_result.status_code == requests.codes.okay:
                # The Twitch API capitalization is inconsistent.
                headers = {**api_result.headers}
                headers = {str(k).lower(): v for k, v in headers.items()}

                # The rate limit headers are not sent for v5 API requests.
                if 'ratelimit-remaining' in headers:
                    self.req_remaining = int(headers['ratelimit-remaining'])
                    self.rate_reset = int(headers['ratelimit-reset'])
                return api_result
            elif i == 2:
                logging.WARNING("Twitch API request failed: {}".format(
                    api_result.status_code))
                raise ConnectionError
            time.sleep(self.API_WINDOW_LENGTH)

    def getgameid(self, gamename):
        """
        Returns the Twitch id corresponding to a game.

        Uses the Twitch API to return the game id for the game with the given
        name.

        :param gamename: str, The name of the game
        :return:
        """
        if gamename.lower() in self.gameidcache:
            return self.gameidcache[gamename.lower()]
        url = self.apiv5host + '/search/games'
        res = self._request(url, {'query': gamename})
        games = json.loads(res.text)['games']
        for game in games:
            if game['name'] == gamename:
                self.gameidcache[gamename.lower()] = int(game['_id'])
                return int(game['_id'])

        # TODO: Raise a better exception
        print(gamename)
        raise Exception

    def gettopgames(self, limit=100):
        """
        Gets the current top games.

        :param limit: int, number of games to return (max 100).
        :return: models.mongomodels.TwitchGamesAPIResponse
        """
        url = self.apiv5host + '/games/top/'
        res = self._request(url, {'limit': limit})
        return TwitchGamesAPIResponse.fromapiresponse(res)

    def getdisplaynames(self, userids):
        """
        Gets the display name of the given user(s).

        If the number of user ids is greater than 100, it will take multiple api
        calls to retrieve the necessary data.

        :param userids: list(int), the twitch user ids.
        :return: dict, keys are user_ids and values are display names.
        """
        res = {}
        curbatch = []
        url = self.apiv6host + '/users/'
        params = {}
        for userid in userids:
            curbatch.append(userid)
            if len(curbatch) < self.API_MAX_RESULTS:
                continue
            params['id'] = ','.join(curbatch)
            res = self._request(url, params).text
            users = json.loads(res)['data']
            for user in users:
                res[int(user['id'])] = user['display_name']
            curbatch = []
        return res

    def topstreams(self, gameid=None):
        """
        Gets the 100 most popular livestreams.

        Gets streams streaming the specified game, or all if gameid is not
        specified.

        :param gameid: int, specifies which games to get.
        :return: models.mongomodels.TwitchStreamsAPIResponse
        """
        url = self.apiv6host + '/streams/'
        params = {
            'first': self.API_MAX_RESULTS
        }
        if gameid:
            params['game_id'] = gameid
        # Each response includes 20 streams.
        responses = [self._request(url, params)]
        # TODO: Decide whether to support variable numbers of results.
        for i in range(0):
            res = json.loads(responses[i].text)
            if 'cursor' not in res['pagination']:
                break
            params['after'] = res['pagination']['cursor']
            responses.append(self._request(url, params))
        rawstreams = []
        for response in responses:
            res = json.loads(response.text)
            rawstreams += res['data']
        return TwitchStreamsAPIResponse.fromapiresponse(rawstreams)


class YouTubeAPIClient:
    """
    Makes YouTube API requests.
    """
    def __init__(self, host, clientid, secret):
        self.host = host
        self.id = clientid
        self.secret = secret
        self.base_params = {'Client-ID': self.id, 'key': self.secret}
        self.session = requests.session()
        self.session.headers.update({'Accept-Encoding': 'gzip'})
        self.idcache = {}
        self.maxres = 50
        self.gaming_category_id = 20

    def _request(self, url, params):
        """
        Makes an API request.

        :param url: str, the request url.
        :param params: dict, query strings to add to the request.
        :return: requests.Response
        """
        for i in range(3):
            params = {**self.base_params, **params}
            api_result = self.session.get(url, params=params)
            # requests does not default to utf-8 encoding.
            api_result.encoding = 'utf8'
            if api_result.status_code == requests.codes.okay:
                return api_result
            elif i == 2:
                logging.WARNING("YouTube API request failed: {}".format(
                    api_result.status_code))
                raise ConnectionError
            time.sleep(10)

    def getid(self, username):
        """
        Gets the YouTube id corresponding to the given username.

        Results are cached so that multiple requests for the same username will
        result in only one api call.  If there is no corresponding id, an empty
        string is returned.

        :param username: str, username to retrieve the id of.
        :return: str, a YouTube id.
        """
        if username in self.idcache:
            return self.idcache[username]

        url = self.host + '/channels'
        params = {'part': 'id', 'forUsername': username}
        api_result = self._request(url, params)
        try:
            json_result = json.loads(api_result.text)
            self.idcache[username] = json_result['items'][0]['id']
            return self.idcache[username]
        except KeyError:
            logging.info(f'YouTube ID for: {username} does not exist.')
            return ''

    class LivestreamDetails:
        def __init__(self, viewers, language, tags, vidid):
            self.viewers = viewers
            self.language = language
            self.tags = tags
            self.vidid = vidid

    def _bcids_to_urls(self, vidids):
        """
        Creates request parameters for retrieving livestream details.

        Returns multiple sets of parameters for when the number of ids is
        greater than YouTube's 50 result limit.

        :param vidids: list(str), list of youtube video ids.
        :return: list(dict)
        """
        params = []
        numcalls = math.ceil(len(vidids) / self.maxres)
        part = 'liveStreamingDetails,topicDetails,snippet,contentDetails'
        for i in range(numcalls):
            start = i * self.maxres
            end = min((i + 1) * self.maxres, len(vidids))
            request_ids = vidids[start:end]
            request_params = {
                'part': part,
                'id': ','.join(request_ids)
            }
            params.append(request_params)
        return params

    def _livestream_details(self, bcids):
        """
        Gets the viewers, language, and tags of the specified broadcasts.

        Broadcasts that have ended will have a viewer count of 0 and invalid
        ids will be ignored.

        :param bcids: list, A list of up to 50 YouTube video ids
            corresponding to current live broadcasts.
        :return: dict, keys video ids and values are LiveStreamDetails.
        """
        api_url = self.host + '/videos'
        mparams = self._bcids_to_urls(bcids)

        broadcasts = []
        for params in mparams:
            api_result = self._request(api_url, params)
            json_result = json.loads(api_result.text)
            broadcasts += json_result['items']

        details = {}
        for bc in broadcasts:
            viewers = bc['liveStreamingDetails'].get('concurrentViewers', 0)
            lang = bc['snippet'].get('defaultAudioLanguage', 'unknown')
            tags = bc['snippet'].get('tags', [])
            vidid = bc['id']
            details[vidid] = self.LivestreamDetails(viewers, lang, tags, vidid)
        return details

    def most_viewed_gaming_streams(self, num=100):
        """
        Retrieves top gaming live streams and their viewer counts.

        Currently youtube restricts the maximum number of results to 100 even
        with pagination.  This may be because the API is potentially pulling
        from an internal playlist rather than doing a search even though the
        search api call is used. I believe this is the case because YouTube's
        live playlists only contain 100 channels and the search returns empty
        results rather than getting rid of the page token parameter as
        specified in the api documentation.
        YouTube allows you to search by game and order by view count but
        retrieving the exact view count requires a separate api call.

        :param num: int, The total number of livestreams to return.
        :return: list(Livestream)
        """
        url = self.host + '/search'
        params = {
            'part': 'snippet',
            'maxResults': min(num, self.maxres),
            'order': 'viewCount',
            'type': 'video',
            'videoCategoryId': self.gaming_category_id,
            'eventType': 'live',
            'regionCode': 'US',
            'relevantLanguage': 'en',
            'safeSearch': 'none'
        }
        numpages = num // self.maxres
        raw_broadcasts = []
        for i in range(numpages):
            api_result = self._request(url, params)
            json_result = json.loads(api_result.text)
            raw_broadcasts += json_result['items']
            # The YouTube API seems to limit results to 100 while still
            # providing the pageToken.  Check if the result is empty to avoid
            # unnecessary api calls.
            if (len(json_result['items']) < self.maxres or
                'nextPageToken' not in json_result):
                break
            params['pageToken'] = json_result['nextPageToken']

        if DEBUG:
            print(f"API result: {api_result}")

        broadcasts = []
        vidids = [k['id']['videoId'] for k in raw_broadcasts]
        lsdets = self._livestream_details(vidids)
        for broadcast in raw_broadcasts:
            # Sometimes the broadcaster chooses to hide the viewer count,
            # the API fails, or the broadcast is no longer live.
            if broadcast['id']['videoId'] not in lsdets:
                continue
            det = lsdets[broadcast['id']['videoId']]

            params = {
                'title': broadcast['snippet']['title'],
                'channame': broadcast['snippet']['channelTitle'],
                'chanid': broadcast['snippet']['channelId'],
                'vidid': broadcast['id']['videoId'],
                'viewers': det.viewers,
                'language': det.language,
                'tags': det.tags
            }
            broadcasts.append(YTLivestream(**params))
        return broadcasts
