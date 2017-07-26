import time
import psycopg2
import logging
from ruamel import yaml
from pymongo import MongoClient


class Aggregator:
    def __init__(self, configpath='scraper_config.yml', keypath='../keys.yml'):
        self.aggregation_interval = 3600
        self.config_path = configpath
        with open(configpath) as f:
            config = yaml.safe_load(f)
        with open(keypath) as f:
            keys = yaml.safe_load(f)
        self.twitch_db = config['twitch']['db']
        self.youtube_db = config['youtube']['db']
        self.postgres = config['postgres']
        self.postgres['user'] = keys['postgres']['user']
        self.postgres['passwd'] = keys['postgres']['passwd']

    @staticmethod
    def db_initialized(conn):
        gamesexists = 'SELECT 1 FROM games'
        try:
            cursor = conn.cursor()
            cursor.execute(gamesexists)
            return True
        except psycopg2.DatabaseError:
            return False

    @staticmethod
    def init_db_tables(conn):
        """
        Initializes the games and twitch_top_games tables.


        :param conn: psycopg2 connection
        :return:
        """
        initgames = (
            'CREATE TABLE games( '
            '    giantbomb_id integer PRIMARY KEY, '
            '    name text NOT NULL'
            '); '
        )
        inittopgames = (
            'CREATE TABLE twitch_top_games( '
            '    giantbomb_id integer REFERENCES games(giantbomb_id), '
            '    epoch integer NOT NULL, '
            '    viewers integer NOT NULL, '
            '    PRIMARY KEY (giantbomb_id, epoch)'
            '); '
        )
        curs = conn.cursor()
        curs.execute(initgames)
        curs.execute(inittopgames)

    def initdb(self):
        """
        Initializes the Postgres database.

        Initializes the database using the schema shown in the schema
        design file (PNG).
        """
        try:
            conn = psycopg2.connect( host=self.postgres['host'],
                                     port=self.postgres['port'],
                                     user=self.postgres['user'],
                                     password=self.postgres['passwd'],
                                     dbname=self.postgres['db_name'])
            if not self.db_initialized(conn):
                conn.commit()
                logging.info("Postgres Twitch schema not initialized.")
                self.init_db_tables(conn)
                conn.commit()
                logging.info("Twitch Schema Initialized.")
            conn.close()
            logging.debug("Postgres database ready.")
        except psycopg2.DatabaseError as e:
            logging.warning("Failed to connect to database: {}".format(e))



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
    logformat = '%(asctime)s %(levelname)s:%(message)s'
    logging.basicConfig(format=logformat, level=logging.DEBUG,
                        filename='aggregator.log')
    a = Aggregator()
    a.initdb()
    #a.twitchtopgames(0.0, time.time())