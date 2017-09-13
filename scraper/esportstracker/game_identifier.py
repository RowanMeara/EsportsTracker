from esportstracker.models import YoutubeStream
from dbinterface import PostgresManager


class YoutubeIdentifier:
    def __init__(self):
        """
        Predicts the game_id of YoutubeStream objects.
        """
        self.channels = {
            # Lol Esports
            'UCvqRdlKsE5Q8mf8YXbdIJLw': 21779,
            'UC48rkTlXjRd6pnqqBkdV0Mw': 21779,
            'UCHKuLpFy9q8XDp0i9WNHkDw': 21779,

            # The International
            'UCTQKT5QqO3h7y32G8VzuySQ': 29595
        }
        lol = ['LCK', 'LCS', 'CBLoL', 'League of Legends']
        csgo = ['CSGO', 'CS GO', 'CS:GO', 'Counter Strike']
        pubg = ['PUBG', 'Playerunknown', 'battlegrounds']
        dota2 = ['Dota 2', 'dota']
        hearthstone = ['Hearthstone']
        overwatch = ['Overwatch']
        heroes = ['Heroes of the Storm', 'HOTS']
        rocketleague = ['Rocket League']
        sc2 = ['sc2', 'Starcraft']
        smite = ['Smite']
        streetfighter = ['Street Fighter V', 'Street Fighter 5']
        codiw = ['COD', 'Call of Duty']
        melee = ['smash']
        self.keywords = {
            493057: pubg,
            21779: lol,
            16282: hearthstone,
            29595: dota2,
            32399: csgo,
            488552: overwatch,
            32959: heroes,
            30921: rocketleague,
            490422: sc2,
            32507: smite,
            488615: streetfighter,
            491437: codiw,
            16282: melee
        }

    def classify(self, yts):
        """
        Determines the game of a youtube_stream.

        Takes a YoutubeStream object and attempts to classify the game that
        is playing based on the stream's contents. If a high-confidence match is
        found, the stream's game_id instance variable is modified.

        :param yts: YoutubeStream
        :return: None
        """
        if yts.channel_id in self.channels.keys():
            yts.game_id = self.channels[yts.channel_id]
            return
        title = yts.stream_title.lower()
        tags = ' '.join(yts.tags)
        for gid, kws in self.keywords.items():
            for kw in kws:
                if kw.lower() in title or kw.lower() in tags:
                    yts.game_id = gid
                    return
