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
    def __init__(self, host, id, secret):
        """
        TwitchAPIClient Constructor.

        :param host: str, host url of the api.
        :param id: str, Twitch API client id.
        :param secret: str, Twitch API secret.
        """
        self.apiv5host = host + '/kraken'
        self.apiv6host = host + '/helix'
        self.secret = secret

        self.session = requests.session()
        headers = {'Accept': 'application/vnd.twitchtv.v5+json',
                   'Client-ID': id}
        self.session.headers.update(headers)

        self.req_remaining = 1
        self.rate_reset = None

    def _request(self, url, params):
        """
        Makes an API request.

        Tries the request three times before giving with a ten second wait
        between attempts.  Sleeps until more api requests are available if they
        are not.

        :param url: str, the request url.
        :param params: dict, query strings to add to the request.
        :return: requests.Response
        """
        if self.req_remaining == 0:
            time.sleep(self.rate_reset - time.time())
        for i in range(3):
            api_result = self.session.get(url, params=params)
            # requests does not default to utf-8 encoding.
            api_result.encoding = 'utf8'
            if api_result.status_code == requests.codes.okay:
                headers = {**api_result.headers}
                headers = {str(k).lower(): v for k, v in headers.items()}
                # Twitch API capitalization is inconsistent
                if 'ratelimit-remaining' not in headers:
                    self.req_remaining = 30
                else:
                    self.req_remaining = int(headers['ratelimit-remaining'])
                    self.rate_reset = int(headers['ratelimit-reset'])
                return api_result
            elif i == 2:
                logging.WARNING("Twitch API request failed: {}".format(
                    api_result.status_code))
                raise ConnectionError
            time.sleep(10)

    def getgameid(self, gamename):
        """
        Returns the Twitch id corresponding to a game.

        Uses the Twitch API to return the game id for the game with the given
        name.

        :param gamename: str, The name of the game
        :return:
        """
        url = self.apiv5host + '/search/games'
        res = self._request(url, {'query': gamename})
        games = json.loads(res.text)['games']
        for game in games:
            if game['name'] == gamename:
                return int(game['_id'])

        # TODO: Raise a better exception
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

    def topstreams(self, gameid=None):
        """
        Gets the 100 most popular livestreams.

        Gets streams streaming the specified game, or all if gameid is not
        specified.

        :param gameid: int, specifies which games to get.
        :return: models.mongomodels.TwitchStreamsAPIResponse
        """
        url = self.apiv6host + '/streams/'
        params = {}
        if gameid:
            params['game_id'] = gameid
        # Each response includes 20 streams.
        responses = [self._request(url, params)]

        for i in range(4):
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
    Makes Youtube API requests.
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
                logging.WARNING("Youtube API request failed: {}".format(
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
        for i in range(numcalls):
            start = i * self.maxres
            end = min((i + 1) * self.maxres, len(vidids))
            request_ids = vidids[start:end]
            request_params = {
                'part': 'liveStreamingDetails,topicDetails,snippet,contentDetails',
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
        Youtube allows you to search by game and order by view count but
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
            params['pageToken'] = json_result['nextPageToken']
            raw_broadcasts += json_result['items']
            # The Youtube API seems to limit results to 100 while still
            # providing the pageToken.  Check if the result is empty to avoid
            # unnecessary api calls.
            if len(json_result['items']) < self.maxres:
                break
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
