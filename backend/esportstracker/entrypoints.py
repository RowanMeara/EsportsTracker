import logging
import os
import time
import sys
import traceback
import argparse
from setproctitle import setproctitle

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_PATH[0:len(DIR_PATH)-len('esportstracker/')])

from esportstracker.scrapers import TwitchScraper, TwitchChannelScraper
from esportstracker.scrapers import YouTubeScraper
from esportstracker.aggregator import Aggregator


def config_logger(fname=None, level=logging.WARNING):
    fmt = '%(asctime)s %(levelname)s:%(message)s'
    if fname:
        logging.basicConfig(format=fmt, filename=fname, level=level)
    else:
        logging.basicConfig(format=fmt, level=level)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def run(fun, logname, production=True):
    """
    Wrapper for entrypoint functions.

    :param f: function, a function that takes a config path and a key path
        argument.
    :param logname: str, filename of the log to output in the current directory.
    :param production: bool, indicates if the environment is production.
    :return:
    """
    if production:
        config_logger(logname, logging.WARNING)
    else:
        config_logger(None, logging.DEBUG)
    dir_path = os.path.dirname(os.path.realpath(__file__))
    cfgpath = dir_path + '/config/config.yml'
    keypath = dir_path + '/../keys.yml'
    if production:
        while True:
            try:
                fun(cfgpath, keypath)
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fn = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                err = "{}. File: {}, line {}. Full: {}"
                logging.warning(err.format(exc_type, fn, exc_tb.tb_lineno,
                                           traceback.format_exc()))
                time.sleep(60)
    else:
        fun(cfgpath, keypath)


def run_twitchscraper(cfgpath, keypath):
    scraper = TwitchScraper(cfgpath, keypath)
    scraper.scrape()


def run_twitch_channel_scraper(cfgpath, keypath):
    scraper = TwitchChannelScraper(cfgpath, keypath)
    scraper.scrape()


def run_youtubescraper(cfgpath, keypath):
    scraper = YouTubeScraper(cfgpath, keypath)
    scraper.scrape()


def run_aggregator(cfgpath, keypath):
    aggregator = Aggregator(cfgpath, keypath)
    aggregator.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--twitch', action='store_true', default=False)
    parser.add_argument('--twitchchannel', action='store_true', default=False)
    parser.add_argument('--youtube', action='store_true', default=False)
    parser.add_argument('--aggregator', action='store_true', default=False)
    parser.add_argument('--debug', action='store_true', default=False)
    args = parser.parse_args()

    production = False if args.debug else True
    if args.twitch:
        setproctitle('TwitchScraper')
        run(run_twitchscraper, 'twitch.log', production)
    if args.twitchchannel:
        setproctitle('TwitchChannelScraper')
        run(run_twitch_channel_scraper, 'twitch_channel.log', production)
    elif args.aggregator:
        setproctitle('AggregatorScraper')
        run(run_aggregator, 'aggregator.log', production)
    elif args.youtube:
        setproctitle('YoutubeScraper')
        run(run_youtubescraper, 'youtube.log', production)
    print("No arguments provided.")
