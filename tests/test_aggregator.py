import pytest
import os
from scraper.aggregator import Aggregator

config_path = 'tests/res/test_scraper_config.yml'
key_path = 'tests/res/test_keys.yml'


def test_agg_top_games_period():
    entry1 = {'timestamp': 1800}
    entry1['games'] = {'game1': {'viewers':400 }, 'game2': {'viewers': 1}}
    entry2 = {'timestamp': 1900}
    entry2['games'] = {'game1': {'viewers':400 }, 'game2': {'viewers': 1}}
    entries = [entry1, entry2]
    games = Aggregator.agg_top_games_period(entries, 0, 2000)
    assert games['game1'] == 400
    assert games['game2'] == 1

