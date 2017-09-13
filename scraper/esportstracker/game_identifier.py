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
        lolkeywords = ['LCK', 'LCS', 'CBLoL', 'League of Legends']
        csgokeywords = ['CSGO', 'CS GO', 'CS:GO', 'Counter Strike']
        pubgkeywords = ['PUBG', 'Playerunknown', 'battlegrounds']
        dota2keywords = ['Dota 2', 'dota']
        self.keywords = {
            21779: lolkeywords,
            32399: csgokeywords,
            493057: pubgkeywords,
            29595: dota2keywords
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
        for gid, kws in self.keywords.items():
            for kw in kws:
                if kw.lower() in yts.stream_title.lower():
                    yts.game_id = gid
                    return
