import pytest
import os

from esportstracker import twitch_scraper

TEST_DIRECTORY = os.path.dirname(__file__)
CONFIG_PATH = TEST_DIRECTORY + '/res/test_scraper_config.yml'
KEY_PATH = TEST_DIRECTORY +'/res/test_keys.yml'


def test_init():
    return
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print("SEXY")
    print(dir_path)
    scraper = twitch_scraper.TwitchScraper(CONFIG_PATH, KEY_PATH)
    assert scraper.update_interval == 20
    assert scraper.db_name == 'test'
    assert scraper.db_top_streams == 'test_twitch_streams'
    assert scraper.db_top_games == 'test_twitch_top_games'
    assert scraper.db_port == 27017
    assert scraper.db_host == 'localhost'
    assert scraper.top_games_url == 'http://retrievetopgames.test'
    assert scraper.live_streams_url == 'http://retrievelivestreams.test/?game={}'
    assert scraper.api_version_url == 'application/vnd.twitchtv.v5+json'
    assert scraper.user_id_url == 'http://retrieveuserids.test/?login={}'
    assert scraper.client_id == 'twitch_client'
    assert scraper.secret == 'twitch_secret'
    assert scraper.session.headers['Client-ID'] == 'twitch_client'
    assert scraper.session.headers['Accept'] == scraper.api_version_url


def test_twitch_api_request():
    return
    scraper = twitch_scraper.TwitchScraper(CONFIG_PATH, KEY_PATH)

    pass

