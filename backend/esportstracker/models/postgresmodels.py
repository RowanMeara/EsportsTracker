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

    def __repr__(self):
        return self.__str__()


class Game(Row):
    """
    A row in the game table.

    See schema.png.
    """
    TABLE_NAME = 'game'
    __slots__ = ['game_id', 'name', 'giantbomb_id']

    def __init__(self, game_id, name, giantbomb_id):
        self.game_id = int(game_id)
        self.name = name
        self.giantbomb_id = int(giantbomb_id)

    @staticmethod
    def from_docs(resps):
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
        return list(games.values())

    def to_row(self):
        return self.game_id, self.name, self.giantbomb_id


class TwitchGameVC(Row):
    """
    A row in the twitch_top_game table.

    The average viewer count of a game in a given hour.
    """
    TABLE_NAME = 'twitch_game_vc'
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
    TABLE_NAME = 'twitch_channel'

    def __init__(self, channel_id, display_name=None, description=None,
                 followers=None, login=None, broadcaster_type=None, type=None,
                 offline_image_url=None, profile_image_url=None,
                 affiliation=None):
        """
        TwitchChannel Constructor

        :param channel_id: int, Twitch user id.
        :param display_name: str, user's display name.
        :param description: str, user's channel description.
        :param followers: int, user's follower count.
        :param login: str, user's login name.
        :param broadcaster_type: str, either partner, affiliate, or None.
        :param type: str, either staff, admin, global_mod, or None.
        :param offline_image_url: text, URL of the user's offline image.
        :param profile_image_url: text, URL of the user's profile image.
        :param affiliation: text, name of the affiliated esports organization.
        """
        self.channel_id = channel_id
        self.display_name = display_name
        self.description = description
        self.followers = followers
        self.login = login
        self.broadcaster_type = broadcaster_type
        self.type = type
        self.offline_image_url = offline_image_url
        self.profile_image_url = profile_image_url
        self.affiliation = affiliation

    @staticmethod
    def from_api_resp(resps):
        """
        Creates TwitchChannel objects for each unique game.

        Notice that returned objects only contain the channel's id.

        :param resps: list[TwitchStreamsAPIResponse],
        :return: list[Game]
        """
        streams = {}
        for resp in resps:
            for snp in resp.streams.values():
                if snp.broadcaster_id not in streams:
                    channel = TwitchChannel(snp.broadcaster_id)
                    streams[snp.broadcaster_id] = channel
        return list(streams.values())

    def to_row(self):
        return self.channel_id, self.display_name, self.description,\
               self.followers, self.login, self.broadcaster_type, self.type,\
               self.offline_image_url, self.profile_image_url, self.affiliation


class TwitchStream(Row):
    """
    A row in the twitch_stream table.

    The title and number of viewers of a stream for a given hour.
    """
    TABLE_NAME = 'twitch_stream'
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
    def from_vcs(api_resp, vcs, timestamp):
        """
        Creates TwitchStream objects from viewercounts and API resp objects.

        Used in aggregation.

        :param api_resp: list(mongomodels.TwitchStreamsAPIResponse), the
            responses.
        :param vcs:   {channel_id, viewercount}, the viewercounts of the api
            responses.
        :param timestamp: int, unix epoch of the row.
        :return: list(TwitchStream), list of rows to insert.
        """
        comb = {}
        for resp in api_resp:
            for snapshot in resp.streams.values():
                if snapshot.broadcaster_id not in comb:
                    comb[snapshot.broadcaster_id] = snapshot

        ts = []
        for chanid, viewers in vcs.items():
            params = {
                'channel_id':chanid,
                'epoch': timestamp,
                'game_id': comb[chanid].game_id,
                'viewers': viewers,
                'title': comb[chanid].title,
                'language': comb[chanid].language,
                'stream_id': comb[chanid].stream_id,
                'stream_type': comb[chanid].stream_type
            }
            ts.append(TwitchStream(**params))
        return ts

    def to_row(self):
        return (self.channel_id, self.epoch, self.game_id, self.viewers,
                self.title, self.language, self.stream_id, self.stream_type)


class YouTubeChannel(Row):
    """
    A mapping between a YouTube channel name and its id.
    """
    TABLE_NAME = 'youtube_channel'
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
        Creates YouTubeChannel objects for each unique channel.

        :param streams: list[YTLivestream], list of livestream objects
        :return: list[YouTubeChannel]
        """
        channels = {}
        for stream in streams:
                if stream.chanid not in channels:
                    channel = YouTubeChannel(stream.chanid, stream.channame,
                                             stream.language)
                    channels[channel.channel_id] = channel
        return list(channels.values())

    @staticmethod
    def fromdoc(doc):
        """

        :param doc: TwitchChannelDoc, the mongodb document.
        :return: YouTubeChannel
        """
        # TODO: Test.
        return YouTubeChannel(**doc.to_doc())


    def to_row(self):
        return (self.channel_id, self.name, self.main_language,
                self.description, self.affiliation)


LANGUAGE_DETECTION = True


class YouTubeStream(Row):
    """
    A row in the youtube_stream table.

    The title and number of viewers of a stream for a given hour.
    """
    TABLE_NAME = 'youtube_stream'
    __slots__ = ['video_id', 'epoch', 'channel_id', 'game_id', 'viewers',
                 'title', 'language', 'tags']

    def __init__(self, video_id, epoch, channel_id, game_id, viewers, title,
                 language, tags):
        self.video_id = video_id
        self.epoch = epoch
        self.channel_id = channel_id
        self.game_id = game_id
        self.viewers = viewers
        self.title = title
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
            params = {
                'video_id': videoid,
                'epoch': timestamp,
                'channel_id': stream.chanid,
                'game_id': None,
                'viewers': viewers,
                'title': stream.title,
                'language': stream.language,
                'tags': stream.tags
            }
            ys.append(YouTubeStream(**params))
        return ys

    @staticmethod
    def from_row(row):
        return YouTubeStream(row[0], row[1], row[2], row[3], row[4], row[5],
                             row[6], row[7])

    def to_row(self):
        if LANGUAGE_DETECTION and self.language == 'unknown':
            if type(self.tags) == list:
                info = self.title + ' '.join(self.tags)
            else:
                info = self.title + self.tags
            self.language = 'd_' + classify_language(info)
        return (self.video_id, self.epoch, self.channel_id, self.game_id,
                self.viewers, self.title, self.language, str(self.tags))


class TournamentOrganizer(Row):
    """
    A row in the esports_org table.
    """
    TABLE_NAME = 'tournament_organizer'

    def __init__(self, name):
        self.name = name

    def to_row(self):
        return self.name,
