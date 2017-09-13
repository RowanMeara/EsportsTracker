from esportstracker.models import *
import psycopg2
import logging
from psycopg2 import sql
from psycopg2 import extras
import pymongo
from pymongo import MongoClient
from collections import OrderedDict


class PostgresManager:
    def __init__(self, host, port, user, password, dbname, esports_games):
        """
        Class for interacting with the Postgres instance.

        Initializes
        :param host: str,
        """

        self.conn = psycopg2.connect(host=host, port=port, user=user,
                                     password=password, dbname=dbname)
        self.tablenames = {'game', 'twitch_game_vc', 'twitch_stream',
                           'twitch_channel', 'channel_affiliation',
                           'esports_org', 'youtube_channel', 'youtube_stream'}
        self.index_names = {'game_name_idx'}
        self.esports_games = esports_games.copy()
        self.gamename_cache = {}
        self.initdb()

    @staticmethod
    def from_config(dbconfig, esports_games):
        return PostgresManager(dbconfig['host'], dbconfig['port'], dbconfig[
            'user'], dbconfig['password'], dbconfig['db_name'], esports_games)

    def commit(self):
        self.conn.commit()

    def initdb(self):
        """
        Creates tables if they don't exist.

        Initializes the database using the schema shown in the schema
        design file (PNG).
        """
        try:

            if not all(self.table_exists(t) for t in self.tablenames):
                logging.info('Initializing missing tables.')
                self.create_tables()
                self.conn.commit()
                logging.info('Postgres initialized.')
            logging.debug('Postgres ready.')
        except psycopg2.DatabaseError as e:
            logging.warning('Failed to initialize database: {}'.format(e))
            raise psycopg2.DatabaseError

    def create_tables(self):
        """
        Creates Postgres tables.


        :param conn: psycopg2 connection
        :return:
        """
        tables = OrderedDict()
        indexes = OrderedDict()
        tables['game'] = (
            'CREATE TABLE game( '
            '    game_id integer PRIMARY KEY, '
            '    name text NOT NULL UNIQUE, '
            '    giantbomb_id integer '
            ');'
        )
        indexes['game_name_idx'] = (
            'CREATE INDEX IF NOT EXISTS game_name_idx '
            'ON game '
            'USING HASH (name);'
        )
        tables['twitch_game_vc'] = (
            'CREATE TABLE twitch_game_vc( '
            '    game_id integer REFERENCES game(game_id), '
            '    epoch integer NOT NULL, '
            '    viewers integer NOT NULL, '
            '    PRIMARY KEY (game_id, epoch) '
            ');'
        )
        tables['twitch_channel'] = (
            'CREATE TABLE twitch_channel( '
            '    channel_id integer PRIMARY KEY, ' 
            '    name text NOT NULL ' 
            ');'
        )
        tables['twitch_stream'] = (
            'CREATE TABLE twitch_stream( ' 
            '    channel_id integer REFERENCES twitch_channel(channel_id), '
            '    epoch integer NOT NULL, '
            '    game_id integer REFERENCES game(game_id), '
            '    viewers integer NOT NULL, '
            '    stream_title text, '
            '    PRIMARY KEY (channel_id, epoch)'
            ');'
        )
        tables['esports_org'] = (
            'CREATE TABLE esports_org( '
            '    org_id integer PRIMARY KEY, '
            '    name text NOT NULL '
            ');'
        )
        tables['channel_affiliation'] = (
            'CREATE TABLE channel_affiliation( '
            '    org_id integer REFERENCES esports_org(org_id), '
            '    channel_id integer REFERENCES twitch_channel(channel_id), '
            '    PRIMARY KEY (channel_id) '
            ');'
        )
        tables['youtube_channel'] = (
            'CREATE TABLE youtube_channel( '
            '    channel_id text PRIMARY KEY, ' 
            '    name text NOT NULL,'
            '    main_language text ' 
            ');'
        )
        tables['youtube_stream'] = (
            'CREATE TABLE youtube_stream( ' 
            '    channel_id text REFERENCES youtube_channel(channel_id), '
            '    epoch integer NOT NULL, '
            '    game_id integer, '
            '    viewers integer NOT NULL, '
            '    stream_title text, '
            '    language text, '
            '    tags text, '
            '    PRIMARY KEY (channel_id, epoch)'
            ');'
        )
        curs = self.conn.cursor()
        for tname, query in tables.items():
            if not self.table_exists(tname):
                curs.execute(query)
                logging.info('Created Table:' + tname)
        for iname, query in indexes.items():
            curs.execute(query)

    def table_exists(self, table):
        """
        Returns True if the table exists in the database.

        :param table: str, name of the table. Must be in the list of known
            table names.
        :return: bool
        """
        if table not in self.tablenames:
            return False
        exists = ('SELECT count(*) '
                  '    FROM information_schema.tables'
                  '    WHERE table_name = \'{}\'')
        cursor = self.conn.cursor()
        cursor.execute(exists.format(table))
        return cursor.fetchone()[0] > 0

    def last_postgres_update(self, table):
        """
        Returns the largest entry in the epoch column.

        The specified table must contain a column named epoch and 0 is returned
        if the column is empty.

        :param conn: psycopg2.connection, Database to connect to.
        :param table: str, Name of table.
        :return: int, Epoch corresponding to last update.
        """
        query = ('SELECT COALESCE(MAX(epoch), 0) '
                 'FROM {}')
        query = sql.SQL(query).format(sql.Identifier(table))
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def store_rows(self, rows, tablename, commit=False):
        """
        Stores the rows in the specified table.

        Conflicting rows are ignored

        :param rows: list[Object], Objects must have a to_row method.
        :param tablename: str, name of the table to insert into.
        :return: bool
        """
        if not rows or tablename not in self.tablenames:
            return False
        query = (f'INSERT INTO {tablename} '
                 'VALUES %s '
                 'ON CONFLICT DO NOTHING ')
        rows = [x.to_row() for x in rows]
        template = '({})'.format(','.join(['%s' for _ in range(len(rows[0]))]))
        curs = self.conn.cursor()
        extras.execute_values(curs, query, rows, template, 1000)
        if commit:
            self.conn.commit()
        return True


    # Stream Functions
    def game_name_to_id(self, name):
        """
        Retrieves the id number of the game with the given name.

        Caches responses from previous invocations.

        :param name:
        :return:
        """
        # The Twitch API appears to give inconsistent casing for the names
        # which we must fix.
        if name not in self.esports_games:
            for game in self.esports_games:
                if name.lower() == game.lower():
                    name = game
                    break
        if name in self.gamename_cache:
            return self.gamename_cache[name]
        query = ('SELECT game_id '
                 'FROM game '
                 'WHERE name = %s')
        cursor = self.conn.cursor()
        cursor.execute(query, (name,))
        self.gamename_cache[name] = cursor.fetchone()[0]
        return self.gamename_cache[name]

    def get_yts(self, epoch, limit):
        """
        Gets YoutubeStream objects.

        Gets the first limit number of YoutubeStream objects with epochs
        greater than epoch.

        :param epoch: int, minimum epoch.
        :param limit: int, the maximum number of streams to return.
        :return: YoutubeStream
        """
        query = ('SELECT * '
                 'FROM youtube_stream '
                 'WHERE epoch > %s '
                 'ORDER BY epoch ASC '
                 'LIMIT %s ')
        cursor = self.conn.cursor()
        cursor.execute(query, (epoch, limit))
        rows = cursor.fetchall()
        return list(map(lambda x: YoutubeStream.from_row(x), rows))

    def update_ytstream_game(self, yts):
        """
        Updates a YoutubeStream row.

        :param yts: YoutubeStream, the stream to be updated.
        :return:
        """
        query = ('UPDATE youtube_stream '
                 'SET game_id = %s '
                 'WHERE channel_id = %s AND epoch = %s ')
        cursor = self.conn.cursor()
        cursor.execute(query, (yts.game_id, yts.channel_id, yts.epoch))


class MongoManager:
    def __init__(self, host, port, db_name, user=None,
                 password=None, ssl=True):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.client = MongoClient(self.host, self.port, ssl=ssl)
        if user:
            self.client[self.db_name].authenticate(user, password,
                                                   source='admin')


    def first_entry_after(self, start, collname):
        """
        Returns the timestamp of the first document after start.

        The instance variable client must be initialized to a MongoClient.

        :param start: int, unix epoch.
        :param collname: str, name of the collection to search in.
        :return: int, unix epoch of the document with the smallest timestamp
        greater than start.
        """
        conn = self.client[self.db_name]
        topgames = conn[collname]
        cursor = topgames.find(
            {'timestamp': {'$gt': start}}
        ).sort('timestamp', pymongo.ASCENDING)
        if cursor.count() > 0:
            return int(cursor[0]['timestamp'])
        else:
            return (1 << 31) - 1

    def docsbetween(self, start, end, collname):
        """
        Returns cursor to entries with timestamps between start and end.

        Returns a cursor to documents in the specified Mongo collection that
        have a field 'timestamp' with values greater than or equal to start
        and less than end.

        :param start: int, Timestamp of the earliest entry
        :param end: int, Timestamp of the last entry
        :return: pymongo.cursor.Cursor
        """
        conn = self.client[self.db_name]
        coll = conn[collname]
        cursor = coll.find(
            {'timestamp': {'$gte': start, '$lt': end}}
        ).sort('timestamp', pymongo.ASCENDING)
        return cursor