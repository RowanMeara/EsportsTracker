import langid
import pycld2 as cld2


class YouTubeGameClassifier:
    """
    Identifies the game that a Youtuber is broadcasting.
    """
    def __init__(self):
        """
        Predicts the game_id of YouTubeStream objects.
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
        codww2 = ['WW2', 'WWII']
        codiw = ['Infinite Warfare', 'IW']
        melee = ['smash']
        destiny2 = ['destiny 2']
        minecraft = ['Minecraft', 'BEDWARS']
        fifa17 = ['FIFA 17', 'FIFA 2K17', 'FIFA2K17', 'FIFA17']
        fifa18 = ['FIFA 18', 'FIFA 2K18', 'FIFA2K18', 'FIFA18']
        nba17 = ['NBA 2K17', 'NBA 17', 'NBA2K17', 'NBA17']
        nba18 = ['NBA 2K18', 'NBA 18', 'NBA2K18', 'NBA18']
        madden17 = ['MADDEN 17', 'MADDEN NFL 17']
        madden18 = ['MADDEN 18', 'MADDEN NFL 18']
        gtav = ['GTA V', 'GTA 5', 'Grand Theft Auto', 'GTA']
        shadowofmordor = ['Shadow of Mordor']
        shadowofwar = ['Shadow of War']
        cuphead = ['Cuphead']
        doom = ['Doom']
        fortnite = ['Fortnite']
        fracturedbutwhole = ['Fractured But Whole']
        warframe = ['Warframe']
        # TODO: handle twitch not having a game like in the case of Roblox.
        roblox = ['Roblox']
        clashroyale = ['Clash Royale']
        # Youtube is infested with TV restreams pretending to be games
        # because YouTube Gaming appears to lack any kind of manual moderation.
        # Twitch has traditionally broadcast TV shows in the 'Twitch Presents'
        # category so it is used as the 'game' for all of YouTube Gaming's
        # unauthorized restreams.
        tv = ['Family Guy', 'South Park', 'Rick and Morty', 'Full Episodes',
              'Futurama', 'SpongeBob SquarePants', 'The Simpsons', '24/7 Music',
              'Dance Music', 'All Radio Stations']
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
            496712: codww2,
            16282: melee,
            497057: destiny2,
            27471: minecraft,
            493091: fifa17,
            495589: fifa18,
            491437: codiw,
            493112: nba17,
            495056: nba18,
            32982: gtav,
            458619: shadowofmordor,
            496000: shadowofwar,
            459064: cuphead,
            33214: fortnite,
            490378: fracturedbutwhole,
            66170: warframe,
            491168: clashroyale,
            6715: doom,
            1510279983: madden17,
            496349: madden18,

            498860: tv
        }

    def classify_game(self, yts):
        """
        Determines the game of a youtube stream.

        Takes a YouTubeStream object and attempts to classify_game the game that
        based on the stream's channel, title, and tags. The stream's game_id
        variable is updated.

        :param yts: YouTubeStream
        :return: None
        """
        if yts.channel_id in self.channels.keys():
            yts.game_id = self.channels[yts.channel_id]
            return
        if isinstance(yts.tags, list):
            titletags = yts.title + ' '.join(yts.tags)
        else:
            titletags = yts.title + yts.tags
        for gid, kws in self.keywords.items():
            for kw in kws:
                if kw.lower() in titletags.lower():
                    yts.game_id = gid
                    return
        yts.game_id = None


def classify_language(title):
    """
    Classifies the language of a stream title.

    :param title: str, title to be classified.
    :return: str, ISO 2 letter language code.
    """
    langidcode = langid.classify(title)[0]

    # Sometimes cld2 believes valid utf-8 is invalid.
    # I believe the utf-8 is valid because encoding and decoding the string
    # while using the 'ignore' invalid characters option did not help.
    try:
        dn, c, r, = cld2.detect(title)
        cldcode = r[0][1]
    except cld2.error:
        cldcode = 'un'
    if langidcode == cldcode or cldcode == 'un':
        return langidcode
    elif langidcode == 'en':
        return cldcode
    else:
        return langidcode
