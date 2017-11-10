from abc import ABC, abstractmethod
import time
import json


class Aggregatable(ABC):
    """
    ABC for an API response that has viewer counts to be aggregated.
    """
    @abstractmethod
    def gettimestamp(self):
        """
        Return a unix epoch.
        """
        pass

    @abstractmethod
    def viewercounts(self):
        """
        Return a dictionary where the values are viewer counts, and the keys are
        ids.
        """
        pass

    def __lt__(self, other):
        return self.gettimestamp() < other.gettimestamp()


class MongoDoc(ABC):
    """
    Abstract base clase for MongoDB Documents.
    """
    @abstractmethod
    def todoc(self):
        """
        Method for creating the dict to insert into MongoDB.
        """
        pass

    def __str__(self):
        return str(self.todoc())

    @staticmethod
    @abstractmethod
    def fromdoc(doc):
        """ Constructor that takes a MongoDB document."""


class TwitchGamesAPIResponse(Aggregatable, MongoDoc):
    """
    Model representing Twitch top games API response.
    """
    COLLECTION = 'twitch_top_games'

    def __init__(self, timestamp, games):
        """
        Constructor.

        :param timestamp: int, epoch.
        :param games: list(TwitchGameSnapshot)
        """
        self.timestamp = timestamp
        self.games = games

    @staticmethod
    def fromdoc(doc):
        games = {}
        for gid, game in doc['games'].items():
            games[int(gid)] = TwitchGameSnapshot(**game)
        return TwitchGamesAPIResponse(doc['timestamp'], games)

    @staticmethod
    def fromapiresponse(response):
        """
        Constructor for api response
        :param response: requests.Response, the api response.
        :return: TwitchGamesAPIResponse
        """
        res = json.loads(response.text)
        timestamp = int(time.time())
        games = {}
        for game in res['top']:
            params = {
                'name': game['game']['name'],
                'viewers': int(game['viewers']),
                'channels': int(game['channels']),
                'id': int(game['game']['_id']),
                'giantbomb_id': int(game['game']['giantbomb_id'])
            }
            games[game['game']['_id']] = TwitchGameSnapshot(**params)
        return TwitchGamesAPIResponse(timestamp, games)

    def todoc(self):
        return {
            'timestamp': self.timestamp,
            'games': {str(gid): vars(snap) for gid, snap in self.games.items()}
        }

    def viewercounts(self):
        return {game.id: game.viewers for game in self.games.values()}

    def gettimestamp(self):
        return self.timestamp


class TwitchGameSnapshot:
    """
    One game from a TwitchGamesAPIResponse
    """
    def __init__(self, name, id, viewers, channels, giantbomb_id):
        self.name = name
        self.id = int(id)
        self.viewers = int(viewers)
        self.channels = int(channels)
        self.giantbomb_id = int(giantbomb_id)

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class TwitchStreamsAPIResponse(Aggregatable, MongoDoc):
    """
    Model representing Twitch Streams API response.
    """
    COLLECTION = 'twitch_streams'

    def __init__(self, timestamp, streams, game_id):
        self.timestamp = int(timestamp)
        self.streams = streams
        self.game_id = int(game_id)

    @staticmethod
    def fromdoc(doc):
        streams = {}
        for cid, stream in doc['streams'].items():
            streams[int(cid)] = TwitchStreamSnapshot(**stream)
        return TwitchStreamsAPIResponse(doc['timestamp'], streams, doc['game_id'])

    def todoc(self):
        return {
            'timestamp': self.timestamp,
            'game_id': self.game_id,
            'streams':
                {str(cid): vars(snap) for cid, snap in self.streams.items()}
        }

    @staticmethod
    def fromapiresponse(rawstreams, minviewers=10):
        """
        Constructor for api response.

        Returns None if no streams are entered.

        :param response: requests.Response, the api response.
        :param minviewers: int, does not include streams with fewer viewers.
        :return: TwitchStreamsAPIResponse
        """
        timestamp = int(time.time())
        if not rawstreams:
            return None
        streams = {}
        gameid = None
        for stream in rawstreams:
            gameid = int(stream['game_id'])
            if stream['viewer_count'] < minviewers:
                continue
            params = {
                'viewers':          int(stream['viewer_count']),
                'game_id':          gameid,
                'language':         stream['language'],
                'stream_type':      stream['type'],
                'title':            stream['title'],
                'stream_id':        int(stream['id']),
                'broadcaster_id':   int(stream['user_id']),
            }
            streams[stream['user_id']] = TwitchStreamSnapshot(**params)
        return TwitchStreamsAPIResponse(timestamp, streams, gameid)

    def viewercounts(self):
        return {s.broadcaster_id: s.viewers for s in self.streams.values()}

    def gettimestamp(self):
        return self.timestamp


class TwitchStreamSnapshot:
    """
    One stream from a TwitchStreamsAPIResponse
    """
    def __init__(self, viewers, game_id, language, stream_type, title, stream_id,
                 broadcaster_id):
        self.viewers = int(viewers)
        self.game_id = game_id
        self.language = language
        self.stream_type = stream_type
        self.title = title
        self.stream_id = stream_id
        self.broadcaster_id = broadcaster_id

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class TwitchChannelDoc(MongoDoc):
    """
    Twitch User API response
    """
    COLLECTION = 'twitch_channels'

    def __init__(self, channel_id, broadcaster_type, description, display_name,
                 login, offline_image_url, profile_image_url, type, followers):
        if type not in ['staff', 'admin', 'global_mod', '']:
            raise ValueError
        # TODO: raise better exception.
        if broadcaster_type not in ['partner', 'affiliate', '']:
            raise ValueError
        self.broadcaster_type = broadcaster_type
        self.description = description
        self.display_name = display_name
        self.channel_id = int(channel_id)
        self.login = login
        self.offline_image_url = offline_image_url
        self.profile_image_url = profile_image_url
        self.followers = followers
        self.type = type

    @staticmethod
    def fromapiresponse(resp):
        data = json.loads(resp.text)['data']
        users = []
        for user in data:
            params = {
                'broadcaster_type': user['broadcaster_type'],
                'description': user['description'],
                'display_name': user['display_name'],
                'channel_id': user['id'],
                'login': user['login'],
                'offline_image_url': user['offline_image_url'],
                'profile_image_url': user['profile_image_url'],
                'type': user['type'],
                'followers': user['view_count']
            }
            users.append(TwitchChannelDoc(**params))
        return users

    @staticmethod
    def fromdoc(doc):
        del doc['_id']
        return TwitchChannelDoc(**doc)

    def todoc(self):
        return vars(self).copy()


class YouTubeChannelDoc(MongoDoc):
    """
    YouTube channels API response
    """
    COLLECTION = 'youtube_channels'

    def __init__(self, channel_id, display_name, description, published_at,
                 thumbnail_url, keywords=None, default_language=None, country=None):
        self.channel_id = channel_id
        self.display_name = display_name
        self.description = description
        self.published_at  = published_at
        self.keywords = keywords
        self.thumbnail_url = thumbnail_url
        self.default_language = default_language
        self.country = country

    @staticmethod
    def fromapiresponse(resp):
        data = json.loads(resp.text)['items']
        channels = []
        for channel in data:
            bsc = channel['brandingSettings']['channel']
            snip = channel['snippet']
            params = {
                'channel_id': channel['id'],
                'description': snip['description'],
                'display_name': snip['title'],
                'published_at': snip['publishedAt'],
                'thumbnail_url': snip['thumbnails']['default']['url']
            }
            if 'keywords' in bsc:
                params['keywords'] = bsc['keywords']
            if 'defaultLanguage' in bsc:
                params['default_language'] = bsc['defaultLanguage']
            if 'country' in bsc:
                params['country'] = bsc['country']
            channels.append(YouTubeChannelDoc(**params))
        return channels

    @staticmethod
    def fromdoc(doc):
        del doc['_id']
        return YouTubeChannelDoc(**doc)

    def todoc(self):
        return vars(self).copy()


class YTLivestreams(Aggregatable, MongoDoc):
    """
    Model representing document in youtube_top_streams Mongo collection.
    """
    COLLECTION = 'youtube_streams'

    def __init__(self, streams, timestamp):
        """
        YTLivestreams constructor.

        :param streams: list(YTLivestream), list of individual streams.
        :param timestamp: int, epoch timestamp of snapshot.
        """
        if not type(timestamp) == int:
            raise TypeError
        self.timestamp = timestamp
        self.streams = streams

    @staticmethod
    def fromdoc(doc):
        """
        Constructor for MongoDB document.

        :param doc: dict, MongoDB document.
        :return: YTLivestreams
        """
        streams = []
        for stream in doc['streams']:
            streams.append(YTLivestream(**stream))
        return YTLivestreams(streams, doc['timestamp'])

    def todoc(self):
        return {
            'timestamp': self.timestamp,
            'streams': [vars(x) for x in self.streams]
        }

    def viewercounts(self):
        return {s.vidid: s.viewers for s in self.streams}

    def gettimestamp(self):
        return int(self.timestamp)


class YTLivestream:
    """
    Represents a YouTube livestream.
    """
    def __init__(self, title, channame, chanid, vidid, viewers, language, tags):
        """
        Constructor for Livestream.

        :param title: str, title of the broadcast.
        :param channame: str, name of the channel that is broadcasting.
        :param chanid: str, id of the channel that is broadcasting.
        :param vidid: str, YouTube video id of the broadcast.
        :param viewers: int, number of concurrent viewers.
        :param language: str, two letter language code or 'unknown'.
        :param tags: list(str), list of tags associated with the video.
        """
        self.title = title
        self.channame = channame
        self.chanid = chanid
        self.vidid = vidid
        self.viewers = int(viewers)
        self.language = language
        self.tags = tags

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()