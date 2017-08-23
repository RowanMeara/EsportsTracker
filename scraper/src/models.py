from abc import ABC, abstractmethod


class Aggregatable(ABC):
    @abstractmethod
    def timestamp(self):
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
        return self.timestamp() < other.timestamp()


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

    def timestamp(self):
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

    def timestamp(self):
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


class YoutubeStreamsAPIResponse(Aggregatable):
    """
    Model representing document in youtube_top_streams Mongo collection.
    """
    __slots__ = ['ts', 'streams']

    def __init__(self, mongodoc):
        self.ts = mongodoc['timestamp']
        self.streams = {}
        for chanid, stream in mongodoc['broadcasts'].items():
            self.streams[chanid] = YoutubeStreamSnapshot(stream, chanid)

    def viewercounts(self):
        return {s.broadcaster_id: s.viewers for s in self.streams.values()}

    def timestamp(self):
        return self.ts


class YoutubeStreamSnapshot:
    """
    One stream in a YoutubeStreamsAPIResponse
    """
    __slots__ = ['name', 'viewers', 'stream_title', 'broadcaster_id',
                 'language', 'tags']
    def __init__(self, stream, chanid):
        self.name = stream['broadcaster_name']
        self.broadcaster_id = chanid
        self.viewers = stream['concurrent_viewers']
        self.language = stream['language']
        self.tags = stream['tags']
        self.stream_title = stream['title']


class Row(ABC):
    @abstractmethod
    def to_row(self):
        """
        Returns a tuple that can be inserted into Postgres.
        """
        pass


class Game(Row):
    """
    A row in the game table.

    Represents a game
    """
    __slots__ = ['game_id', 'name', 'giantbomb_id']

    def __init__(self, game_id, name, giantbomb_id):
        self.game_id = int(game_id)
        self.name = name
        self.giantbomb_id = int(giantbomb_id)

    @staticmethod
    def api_responses_to_games(resps):
        """
        Creates Game objects for each unique game.

        :param resps: list[TwitchGamesAPIResponse],
        :return: list[Game]
        """
        games = {}
        for resp in resps:
            for snp in resp.games.values():
                if snp.id not in games:
                    games[snp.id] = Game(snp.id, snp.name, snp.giantbomb_id)
        return games

    def to_row(self):
        return self.game_id, self.name, self.giantbomb_id


class TwitchGameVC(Row):
    """
    A row in the twitch_top_game table.

    The average viewer count of a game in a given hour.
    """
    __slots__ = ['game_id', 'epoch', 'viewers']

    def __init__(self, game_id, epoch, viewers):
        self.game_id = game_id
        self.epoch = epoch
        self.viewers = viewers

    @staticmethod
    def from_vcs(vcs, timestamp):
        vs = []
        for gid, viewers in vcs.items():
            vs.append(TwitchGameVC(gid, timestamp, viewers))
        return vs

    def to_row(self):
        return self.game_id, self.epoch, self.viewers


class TwitchChannel(Row):
    """
    A mapping between a Twitch channel name and its id.
    """
    __slots__ = ['channel_id', 'name']

    def __init__(self, channel_id, name):
        self.channel_id = channel_id
        self.name = name

    @staticmethod
    def from_api_resp(resps):
        """
        Creates TwitchChannel objects for each unique game.

        :param resps: list[TwitchStreamsAPIResponse],
        :return: list[Game]
        """
        streams = {}
        for resp in resps:
            for snp in resp.streams.values():
                if snp.broadcaster_id not in streams:
                    channel = TwitchChannel(snp.broadcaster_id, snp.name)
                    streams[snp.broadcaster_id] = channel
        return streams

    def to_row(self):
        return self.channel_id, self.name


class TwitchStream(Row):
    """
    A row in the twitch_stream table.

    The title and number of viewers of a stream for a given hour.
    """
    __slots__ = ['channel_id', 'epoch', 'game_id', 'viewers', 'stream_title']

    def __init__(self, channel_id, epoch, game_id, viewers, stream_title):
        self.channel_id = channel_id
        self.epoch = epoch
        self.game_id = game_id
        self.viewers = viewers
        self.stream_title = stream_title

    @staticmethod
    def from_vcs(api_resp, vcs, timestamp, man):
        # Combine api_resp so that we can look across all api responses
        comb = {}
        for resp in api_resp:
            for snp in resp.streams.values():
                if snp.broadcaster_id not in comb:
                    comb[snp.broadcaster_id] = snp

        ts = []
        for sid, viewers in vcs.items():
            chid = sid
            ep = timestamp
            gid = man.game_name_to_id(comb[sid].game)
            vc = viewers
            tit = comb[sid].stream_title
            ts.append(TwitchStream(chid, ep, gid, vc, tit))
        return ts

    def to_row(self):
        return (self.channel_id, self.epoch, self.game_id, self.viewers,
                self.stream_title)


class YoutubeChannel(Row):
    """
    A mapping between a Youtube channel name and its id.
    """
    __slots__ = ['channel_id', 'name', 'main_language']

    def __init__(self, channel_id, name, main_language="unknown"):
        self.channel_id = channel_id
        self.name = name
        self.main_language = main_language

    @staticmethod
    def from_api_resp(resps):
        """
        Creates YoutubeChannel objects for each unique game.

        :param resps: list[TwitchStreamsAPIResponse],
        :return: list[Game]
        """
        streams = {}
        for resp in resps:
            for snp in resp.streams.values():
                if snp.broadcaster_id not in streams:
                    channel = YoutubeChannel(snp.broadcaster_id, snp.name,
                                             snp.main_language)
                    streams[snp.broadcaster_id] = channel
        return streams

    def to_row(self):
        return self.channel_id, self.name, self.main_language


class YoutubeStream(Row):
    """
    A row in the youtube_stream table.

    The title and number of viewers of a stream for a given hour.
    """
    __slots__ = ['channel_id', 'epoch', 'game_id', 'viewers', 'stream_title',
                 'language', 'tags']

    def __init__(self, channel_id, epoch, game_id, viewers, stream_title,
                 language, tags):
        self.channel_id = channel_id
        self.epoch = epoch
        self.game_id = game_id
        self.viewers = viewers
        self.stream_title = stream_title
        self.language = language
        self.tags = tags

    @staticmethod
    def from_vcs(api_resp, vcs, timestamp, man):
        # Combine api_resp so that we can look across all api responses
        comb = {}
        for resp in api_resp:
            for snp in resp.streams.values():
                if snp.broadcaster_id not in comb:
                    comb[snp.broadcaster_id] = snp

        ts = []
        for sid, viewers in vcs.items():
            chid = sid
            ep = timestamp
            gid = man.game_name_to_id(comb[sid].game)
            vc = viewers
            tit = comb[sid].stream_title
            ts.append(TwitchStream(chid, ep, gid, vc, tit))
        return ts

    def to_row(self):
        return (self.channel_id, self.epoch, self.game_id, self.viewers,
                self.stream_title)
