import logging
import os
import time
import sys
import traceback
import argparse
from setproctitle import setproctitle

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_PATH[0:len(DIR_PATH)-len('esportstracker/')])

from esportstracker.twitch_scraper import TwitchScraper
from esportstracker.youtube_scraper import YoutubeScraper
from esportstracker.aggregator import Aggregator

__DEBUG__ = False


def config_logger(fname):
    fmt = '%(asctime)s %(levelname)s:%(message)s'
    logging.basicConfig(format=fmt, filename=fname, level=logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def run_twitchscraper():
    config_logger('twitch.log')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    cfgpath = dir_path + '/config/config.yml'
    keypath = dir_path + '/../keys.yml'

    while True:
        try:
            a = TwitchScraper(cfgpath, keypath)
            a.scrape()
        # TODO: Change
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fn = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            err = "{}. File: {}, line {}. Full: {}"
            logging.warning(err.format(exc_type, fn, exc_tb.tb_lineno,
                                       traceback.format_exc()))
            # TODO: Remove magic number
            time.sleep(60)


def run_youtubescraper():
    config_logger('youtube.log')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    cfgpath = dir_path + '/config/config.yml'
    keypath = dir_path + '/../keys.yml'
    #a = YoutubeScraper(cfgpath, keypath, ytgamespath)
    #a.scrape()
    while True:
        try:
            a = YoutubeScraper(cfgpath, keypath)
            a.scrape()
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fn = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            err = "{}. File: {}, line {}. Full: {}"
            logging.warning(err.format(exc_type, fn, exc_tb.tb_lineno,
                                       traceback.format_exc()))
            # TODO: Make more informative than line 209
            time.sleep(60)


def run_aggregator():
    config_logger('aggregator.log')
    logging.debug("Aggregator Starting.")
    dir_path = os.path.dirname(os.path.realpath(__file__))
    cfgpath = dir_path + '/config/config.yml'
    keypath = dir_path + '/../keys.yml'
    print('Starting Aggregator')
    while True:
        try:
            a = Aggregator(cfgpath, keypath)
            start = time.time()
            a.agg_twitch_games()
            a.agg_twitch_broadcasts()
            a.agg_youtube_streams()
            end = time.time()
            print("Total Time: {:.2f}".format(end - start))

            # Refresh on the 1st and 31st minute of each hour
            x = (int(end) % 1800)
            timesleep = 1860 - x if x > 0 else 60 - x
            time.sleep(timesleep)
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fn = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            err = "{}. File: {}, line {}. Full: {}"
            logging.warning(err.format(exc_type, fn, exc_tb.tb_lineno,
                                       traceback.format_exc()))
            # TODO: Remove magic number
            time.sleep(60)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--twitch', action='store_true', default=False)
    parser.add_argument('--youtube', action='store_true', default=False)
    parser.add_argument('--aggregator', action='store_true', default=False)
    parser.add_argument('--debug', action='store_true', default=False)
    args = parser.parse_args()
    if args.debug:
        __DEBUG__ = True
    if args.twitch:
        setproctitle('TwitchScraper')
        run_twitchscraper()
    elif args.aggregator:
        setproctitle('AggregatorScraper')
        run_aggregator()
    elif args.youtube:
        setproctitle('YoutubeScraper')
        run_youtubescraper()
    print("No arguments provided.")
