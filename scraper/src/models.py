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


class Game:
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


class TwitchGameViewerCount:
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
            vs.append(TwitchGameViewerCount(gid, timestamp, viewers))
        return vs

    def to_row(self):
        return self.game_id, self.epoch, self.viewers


class TwitchChannel:
    """
    A mapping between a Twitch channel name and its id.
    """
    __slots__ = ['channel_id', 'name']

    def __init__(self, channel_id, name):
        self.channel_id = id
        self.name = name

    def to_row(self):
        return self.channel_id, self.name