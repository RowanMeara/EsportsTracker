import pytest
import os

from esportstracker.aggregator import Aggregator
from esportstracker.models.mongomodels import TwitchGamesAPIResponse

config_path = 'res/test_scraper_config.yml'
key_path = 'res/test_keys.yml'


def test_average_viewers():
    entry1 = {'timestamp': 1900}
    entry1['games'] = {
        'game1': {
            'viewers': 400,
            'name': 'onetorulethemall',
            'giantbomb_id': 0,
            'id': 1,
            'channels': 10
        },
        'game2': {
            'viewers': 1,
            'name': 'twotorulethemall',
            'giantbomb_id': 1,
            'id': 2,
            'channels': 10
        }
    }
    entry2 = {'timestamp': 1800}
    entry2['games'] = {
        'game1': {
            'viewers': 400,
            'name': 'onetorulethemall',
            'giantbomb_id': 0,
            'id': 1,
            'channels': 10
        },
        'game2': {
            'viewers': 1,
            'name': 'twotorulethemall',
            'giantbomb_id': 1,
            'id': 2,
            'channels': 10
        }
    }
    entries = [entry1, entry2]
    entries = [TwitchGamesAPIResponse(entry) for entry in entries]
    games = Aggregator.average_viewers(entries, 0, 2000)
    assert games[1] == 400
    assert games[2] == 1

    entry3 = {'timestamp': 2000}
    entry3['games'] = {
        'game3': {
            'viewers': 200,
            'name': 'third_game',
            'giantbomb_id': 2,
            'id': 3,
            'channels': 10
        }
    }
    entries.append(TwitchGamesAPIResponse(entry3))
    games = Aggregator.average_viewers(entries, 1500, 2000)
    assert games[1] == 320
    assert games[2] == 0
    assert games[3] == 40
    games = Aggregator.average_viewers(entries, 1800, 2200)
    assert games[1] == 100
    assert games[2] == 0
    assert games[3] == 150
