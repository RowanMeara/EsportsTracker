from abc import ABC, abstractmethod
from esportstracker.classifiers import classify_language


class Row(ABC):
    @abstractmethod
    def to_row(self):
        """
        Returns a tuple that can be inserted into Postgres.
        """
        pass

    def __str__(self):
        return str(self.to_row())


class Game(Row):
    """
    A row in the game table.

    See schema.png.
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
    __slots__ = ['channel_id', 'name', 'affiliation']

    def __init__(self, channel_id, name=None, affiliation=None):
        self.channel_id = channel_id
        self.name = name
        self.affiliation = affiliation

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
                    channel = TwitchChannel(snp.broadcaster_id)
                    streams[snp.broadcaster_id] = channel
        return streams

    def to_row(self):
        return self.channel_id, self.name, self.affiliation


class TwitchStream(Row):
    """
    A row in the twitch_stream table.

    The title and number of viewers of a stream for a given hour.
    """
    __slots__ = ['channel_id', 'epoch', 'game_id', 'viewers', 'title',
                 'language', 'stream_id', 'stream_type']

    def __init__(self, channel_id, epoch, game_id, viewers, title, language,
                 stream_id, stream_type):
        self.channel_id = channel_id
        self.epoch = epoch
        self.game_id = game_id
        self.viewers = viewers
        self.title = title
        self.language = language
        self.stream_id = stream_id
        self.stream_type = stream_type

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
                self.title, self.language, self.stream_id, self.stream_type)


class YoutubeChannel(Row):
    """
    A mapping between a Youtube channel name and its id.
    """
    __slots__ = ['channel_id', 'name', 'main_language', 'description',
                 'affiliation']

    def __init__(self, channel_id, name, main_language='unknown',
                 description=None, aff=None):
        self.channel_id = channel_id
        self.name = name
        self.main_language = main_language
        self.description = description
        self.affiliation = aff

    @staticmethod
    def fromstreams(streams):
        """
        Creates YoutubeChannel objects for each unique channel.

        :param resps: list[YTLivestream], list of livestream objects
        :return: list[Game]
        """
        channels = {}
        for stream in streams:
                if stream.chanid not in channels:
                    channel = YoutubeChannel(stream.chanid, stream.channame,
                                             stream.language)
                    channels[channel.channel_id] = channel
        return channels

    def to_row(self):
        return (self.channel_id, self.name, self.main_language,
                self.description, self.affiliation)


LANGUAGE_DETECTION = True


class YoutubeStream(Row):
    """
    A row in the youtube_stream table.

    The title and number of viewers of a stream for a given hour.
    """
    __slots__ = ['video_id', 'epoch', 'channel_id', 'game_id', 'viewers', 'stream_title',
                 'language', 'tags']

    def __init__(self, video_id, channel_id, epoch, game_id, viewers,
                 stream_title, language, tags):
        self.video_id = video_id
        self.epoch = epoch
        self.channel_id = channel_id
        self.game_id = game_id
        self.viewers = viewers
        self.stream_title = stream_title
        self.language = language
        self.tags = tags

    @staticmethod
    def from_vcs(api_resp, vcs, timestamp):
        """
        Creates rows from mongodocs.

        :param api_resp: list(YTLivestreams), livestream objects.
        :param vcs: int, aggregated viewercounts.
        :param timestamp: int, epoch.
        :return:
        """
        # Combine api_resp so that we can look across all api responses
        comb = {}
        for resp in api_resp:
            for snp in resp.streams:
                if snp.vidid not in comb:
                    comb[snp.vidid] = snp

        ys = []
        for videoid, viewers in vcs.items():
            stream = comb[videoid]
            l = {
                'video_id': videoid,
                'epoch': timestamp,
                'channel_id': stream.chanid,
                'game_id': None,
                'viewers': viewers,
                'stream_title': stream.title,
                'language': stream.language,
                'tags': stream.tags
            }
            ys.append(YoutubeStream(**l))
        return ys

    @staticmethod
    def from_row(row):
        return YoutubeStream(row[0], row[1], row[2], row[3], row[4], row[5],
                             row[6])

    def to_row(self):
        if LANGUAGE_DETECTION and self.language == 'unknown':
            if type(self.tags) == list:
                info = self.stream_title + ' '.join(self.tags)
            else:
                info = self.stream_title + self.tags
            self.language = 'd_' + classify_language(info)
        return (self.video_id, self.epoch, self.channel_id, self.game_id,
                self.viewers, self.stream_title, self.language, str(self.tags))


class TournamentOrganizer(Row):
    """
    A row in the esports_org table.
    """
    def __init__(self, name):
        self.name = name

    def to_row(self):
        return self.name,

