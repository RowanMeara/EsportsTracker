import pytest
import os
from scraper.aggregator import Aggregator


config_path = 'tests/res/test_scraper_config.yml'
key_path = 'tests/res/test_keys.yml'


def test_agg_top_games_period():
    entry1 = {'timestamp': 1800}
    entry1['games'] = {
        'game1': {
            'viewers': 400,
            'name': 'onetorulethemall',
            'giantbomb_id': 0
        },
        'game2': {
            'viewers': 1,
            'name': 'twotorulethemall',
            'giantbomb_id': 1
        }
    }
    entry2 = {'timestamp': 1900}
    entry2['games'] = {'game1': {'viewers': 400}, 'game2': {'viewers': 1}}
    entries = [entry1, entry2]
    games = Aggregator.agg_top_games_period(entries, 0, 2000)
    assert games['game1']['v'] == 400
    assert games['game2']['v'] == 1
    assert games['game1']['name'] == 'onetorulethemall'
    assert games['game1']['giantbomb_id'] == 0

    entry3 = {'timestamp': 2000}
    entry3['games'] = {
        'game3': {
            'viewers': 200,
            'name': 'third_game',
            'giantbomb_id': 2
        }
    }
    entries.append(entry3)
    games = Aggregator.agg_top_games_period(entries, 1500, 2000)
    assert games['game1']['v'] == 320
    assert games['game2']['v'] == 0
    assert games['game3']['v'] == 40
    games = Aggregator.agg_top_games_period(entries, 1800, 2200)
    assert games['game1']['v'] == 100
    assert games['game2']['v'] == 0
    assert games['game3']['v'] == 150
    assert games['game3']['name'] == 'third_game'
