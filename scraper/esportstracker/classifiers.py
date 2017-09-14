import langid
import pycld2 as cld2


class YoutubeIdentifier:
    """
    Identifies the game that a Youtuber is broadcasting.
    """
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
            'UCTQKT5QqO3h7y32G8VzuySQ': 29595,

            # Garena RoV
            'UCy19QXxbCHh8qVVCbuGk-ig': 495931
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
        codbo = ['Black Ops']
        codiw = ['COD', 'Call of Duty', 'IW']
        melee = ['smash']
        destiny2 = ['destiny 2']
        minecraft = ['Minecraft']
        fifa = ['FIFA']
        nba = ['NBA 2K17']
        gtav = ['GTA V', 'GTA 5']
        self.keywords = {
            493057: pubg,
            21779: lol,
            138585: hearthstone,
            29595: dota2,
            32399: csgo,
            488552: overwatch,
            32959: heroes,
            30921: rocketleague,
            490422: sc2,
            32507: smite,
            488615: streetfighter,
            489401: codbo,
            16282: melee,
            497057: destiny2,
            27471: minecraft,
            493091: fifa,
            491437: codiw,
            493112: nba,
            32982: gtav
        }

    def classify(self, yts):
        """
        Determines the game of a youtube stream.

        Takes a YoutubeStream object and attempts to classify the game that
        based on the stream's channel, title, and tags. If a high-confidence
        match is found, the stream's game_id instance variable is modified.

        :param yts: YoutubeStream
        :return: None
        """
        if yts.channel_id in self.channels.keys():
            yts.game_id = self.channels[yts.channel_id]
            return
        if isinstance(yts.tags, list):
            titletags = yts.stream_title + ' '.join(yts.tags)
        else:
            titletags = yts.stream_title + yts.tags
        for gid, kws in self.keywords.items():
            for kw in kws:
                if kw.lower() in titletags.lower():
                    yts.game_id = gid
                    return


def classify_language(title):
    """
    Classifies the language of a stream title.

    :param title: str, title to be classified.
    :return: str, language code.
    """
    langidcode = langid.classify(title)[0]
    dn, c, r, = cld2.detect(title)
    cldcode = r[0][1]
    if langidcode == cldcode or cldcode == 'un':
        return langidcode
    elif langidcode == 'en':
        return cldcode
    else:
        return langidcode
