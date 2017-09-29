from abc import ABC, abstractmethod
import time



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


class TwitchGamesAPIResponse(Aggregatable):
    """
    Model representing Twitch top games API response.
    """
    __slots__ = ['ts', 'games']

    def __init__(self, mongodoc):
        self.ts = mongodoc['timestamp']
        self.games = {}
        for gid, game in mongodoc['games'].items():
            self.games[gid] = TwitchGameSnapshot(game)

    def viewercounts(self):
        return {game.id: game.viewers for k, game in self.games.items()}

    def gettimestamp(self):
        return self.ts


class TwitchGameSnapshot:
    """
    One game from a TwitchGamesAPIResponse
    """
    __slots__ = ['name', 'id', 'viewers', 'channels', 'giantbomb_id']

    def __init__(self, game):
        self.name = game['name']
        self.viewers = game['viewers']
        self.id = game['id']
        self.channels = game['channels']
        self.giantbomb_id = game['giantbomb_id']


class TwitchStreamsAPIResponse(Aggregatable):
    """
    Model representing Twitch streams API response.
    """
    __slots__ = ['ts', 'streams', 'game']

    def __init__(self, mongodoc):
        self.ts = mongodoc['timestamp']
        self.streams = {}
        self.game = mongodoc['game']
        for gid, stream in mongodoc['streams'].items():
            self.streams[gid] = TwitchStreamSnapshot(stream)

    def viewercounts(self):
        return {s.broadcaster_id: s.viewers for s in self.streams.values()}

    def gettimestamp(self):
        return self.ts


class TwitchStreamSnapshot:
    """
    One game from a TwitchGamesAPIResponse
    """
    __slots__ = ['name', 'viewers', 'game', 'stream_title', 'broadcaster_id']

    def __init__(self, stream):
        self.name = stream['display_name']
        self.viewers = stream['viewers']
        self.game = stream['game']
        self.stream_title = stream['status']
        self.broadcaster_id = stream['broadcaster_id']


class MongoDoc(ABC):
    """
    Abstract base clase for MongoDB Documents.
    """
    def to_dict(self):
        pass

    def __str__(self):
        return str(self.to_dict())

    @abstractmethod
    def fromdoc(self, doc):
        """ Constructor that takes a MongoDB document."""


class YTLivestreams(Aggregatable, MongoDoc):
    """
    Model representing document in youtube_top_streams Mongo collection.
    """
    def __init__(self, streams, timestamp):
        if not type(timestamp) == int:
            raise TypeError
        self.timestamp = timestamp
        self.streams = streams

    def fromdoc(self, doc):
        pass

    def to_dict(self):
        return {
            'timestamp': self.timestamp,
            'streams': [vars(x) for x in self.streams]
        }

    def viewercounts(self):
        return {s.vidid: s.viewers for s in self.streams.values()}

    def gettimestamp(self):
        return int(self.ts)


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
        self.viewers = viewers
        self.language = language
        self.tags = tags

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()