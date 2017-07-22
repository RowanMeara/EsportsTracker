import time
import psycopg2
from ruamel import yaml
from pymongo import MongoClient

class Aggregator:
    def __init__(self, config_path='scraper_config.yml'):
        self.aggregation_interval = 3600
        self.config_path = config_path
        with open(config_path) as f:
            config = yaml.safe_load(f)
        self.twitch_db = config['twitch']['db']
        self.youtube_db = config['youtube']['db']

    def gettwitchtopgames(self, timestart, timeend):
        """
        Retrieves a cursor to Twitch top game entries.

        Entries that are retrieved have a timestamp between timestart and
        timeend.  The timestamp is in Unix time.

        :return:
        """
        client = MongoClient(self.twitch_db['host'], self.twitch_db['port'])
        db = client[self.twitch_db['db_name']]
        topgames = db[self.twitch_db['top_games']]
        cursor = topgames.find(
            {"timestamp": {"$gt": timestart, "$lt": timeend}}
        )
        return cursor

    def aggregatetopgames(self, cursor):
        """


        :param cursor: MongoClient Cursor, Result of a find() query.
        :return:
        """
        games = {}

    def aggregate_time(self, timestamps):
        """
        Returns the combined viewer hours.

        Adds the timestamps together accounting for the irregular intervals
        between them.  Timestamps that are further apart than fifteen minutes
        will result in inaccurate data as only the 15 minutes after the
        timestamp will be counted.

        :param timestamps: list, List of tuples where the first entry is the
            unix timestamp and the second entry is the viewer count.
        :return:
        """
        pass

if __name__ == '__main__':
    a = Aggregator()
    a.twitchtopgames(0.0, time.time())