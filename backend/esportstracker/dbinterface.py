from esportstracker.models.postgresmodels import *
import psycopg2
import logging
from psycopg2 import sql
from psycopg2 import extras
import pymongo
from pymongo import MongoClient
from collections import OrderedDict
import collections

class PostgresManager:
    """
    Class for managing the Postgres instance.
    """
    def __init__(self, host, port, user, password, dbname, esports_games=[]):
        """
        Initializes a PostgresManager.

        The esports_games parameter must be specified for the manager to
        retrieve the id's of games that have improper capitalization.

        :param host: str, host of the database.
        :param port: int, port of the database.
        :param user: str, username.
        :param password: str, password.
        :param dbname: str, name of the database to connect to.
        :param esports_games: list, list of Game objects.
        :return None
        """

        self.conn = psycopg2.connect(host=host, port=port, user=user,
                                     password=password, dbname=dbname)
        self.tablenames = ['game', 'twitch_game_vc', 'tournament_organizer',
                           'twitch_channel', 'twitch_stream',
                           'youtube_channel', 'youtube_stream']
        self.index_names = {'game_name_idx'}
        self.esports_games = esports_games.copy()
        self.gamename_cache = {}
        self.esports_channels = {}
        self.initdb()

    @staticmethod
    def from_config(dbconfig, esports_games):
        return PostgresManager(dbconfig['host'], dbconfig['port'], dbconfig[
            'user'], dbconfig['password'], dbconfig['db_name'], esports_games)

    def commit(self):
        """
        Wrapper for Postgres commit.

        :return: None
        """
        self.conn.commit()

    def initdb(self):
        """
        Initializes the database for use.

        Initializes the database using the schema shown in the schema
        design file (schema.png).  Missing tables and indexes are created.
        """
        try:
            if not all(self.table_exists(t) for t in self.tablenames):
                logging.info('Initializing missing tables.')
                self.create_tables()
                self.conn.commit()
                logging.info('Postgres initialized.')
            self.init_indexes()
            logging.debug('Postgres ready.')
        except psycopg2.DatabaseError as e:
            logging.warning('Failed to initialize database: {}'.format(e))
            raise psycopg2.DatabaseError

    def create_tables(self):
        """
        Creates database tables if they do not exist.

        Initializes the database using the schema shown in the schema
        design file (schema.png).
        """
        tables = OrderedDict()
        tables['game'] = (
            'CREATE TABLE game( '
            '    game_id integer PRIMARY KEY, '
            '    name text NOT NULL UNIQUE, '
            '    giantbomb_id integer '
            ');'
        )
        tables['twitch_game_vc'] = (
            'CREATE TABLE twitch_game_vc( '
            '    game_id integer REFERENCES game(game_id), '
            '    epoch integer NOT NULL, '
            '    viewers integer NOT NULL, '
            '    PRIMARY KEY (game_id, epoch) '
            ');'
        )
        tables['tournament_organizer'] = (
            'CREATE TABLE tournament_organizer( '
            '    org_name text PRIMARY KEY '
            ');'
        )
        tables['twitch_channel'] = (
            'CREATE TABLE twitch_channel( '
            '    channel_id integer PRIMARY KEY, ' 
            '    name text UNIQUE, ' 
            '    affiliation text REFERENCES tournament_organizer(org_name) ' 
            ');'
        )
        tables['twitch_stream'] = (
            'CREATE TABLE twitch_stream( ' 
            '    channel_id integer REFERENCES twitch_channel(channel_id), '
            '    epoch integer NOT NULL, '
            '    game_id integer REFERENCES game(game_id), '
            '    viewers integer NOT NULL, '
            '    title text, '
            '    language text, '
            '    stream_id bigint, '
            '    stream_type text, '
            '    PRIMARY KEY (channel_id, epoch)'
            ');'
        )
        tables['youtube_channel'] = (
            'CREATE TABLE youtube_channel( '
            '    channel_id text PRIMARY KEY, ' 
            '    name text NOT NULL, '
            '    main_language text, '
            '    description text, '
            '    affiliation text REFERENCES tournament_organizer(org_name) ' 
            ');'
        )
        tables['youtube_stream'] = (
            'CREATE TABLE youtube_stream( ' 
            '    video_id text NOT NULL, '
            '    epoch integer NOT NULL, '
            '    channel_id text REFERENCES youtube_channel(channel_id), '
            '    game_id integer, '
            '    viewers integer NOT NULL, '
            '    title text, '
            '    language text, '
            '    tags text, '
            '    PRIMARY KEY (video_id, epoch)'
            ');'
        )

        curs = self.conn.cursor()
        for tname, query in tables.items():
            if not self.table_exists(tname):
                curs.execute(query)
                logging.info('Created Table:' + tname)

    def init_indexes(self):
        """
        Creates database indexes if they do not already exist.

        :return: None
        """
        indexes = OrderedDict()
        indexes['game_name_idx'] = (
            'CREATE INDEX IF NOT EXISTS game_name_idx '
            'ON game '
            'USING HASH (name);'
        )
        indexes['youtube_stream_epoch_idx'] = (
            'CREATE INDEX IF NOT EXISTS youtube_stream_epoch_idx '
            'ON youtube_stream(epoch, game_id) '
        )
        indexes['twitch_stream_epoch_idx'] = (
            'CREATE INDEX IF NOT EXISTS twitch_stream_epoch_idx '
            'ON twitch_stream(epoch, game_id) '
        )
        indexes['twitch_channel_affiliation_idx'] = (
            'CREATE INDEX IF NOT EXISTS twitch_channel_affiliation_idx '
            'ON twitch_channel(affiliation)'
        )
        indexes['youtube_channel_affiliation_idx'] = (
            'CREATE INDEX IF NOT EXISTS youtube_channel_affiliation_idx '
            'ON youtube_channel(affiliation)'
        )
        curs = self.conn.cursor()
        for query in indexes.values():
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

    def most_recent_epoch(self, table):
        """
        Returns the largest entry in the epoch column.

        The specified table must contain a column named epoch and 0 is returned
        if the column is empty.

        :param conn: psycopg2.connection, Database to connect to.
        :param table: str, Name of table.
        :return: int, Epoch corresponding to last update.
        """
        if table not in self.tablenames:
            return 0
        query = ('SELECT COALESCE(MAX(epoch), 0) '
                 'FROM {} ')
        query = sql.SQL(query).format(sql.Identifier(table))
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        return cursor.fetchone()[0]

    def earliest_epoch(self, table):
        """
        Returns the smallest epoch in the table.

        The table must have a column named epoch and be a part of the schema.

        :param table: str, name of table.
        :return: int, timestamp of earliest entry.
        """
        if table not in self.tablenames:
            return 0
        query = ('SELECT COALESCE(MIN(epoch), 0) '
                 'FROM {} ')
        query = sql.SQL(query).format(sql.Identifier(table))
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def _group_rows(self, rows):
        """
        Groups the rows by table.

        Implemented so that the store_rows method can take rows from multiple
        different tables and store them all.

        :param rows: list(Row), list of rows to store.
        :return: dict, keys are table names and values are lists of Row objects.
        """
        res = {tn: [] for tn in self.tablenames}
        for row in rows:
            res[row.TABLE_NAME].append(row)
        return {tn: rows for tn, rows in res.items() if rows}

    def store_rows(self, rows, commit=False):
        """
        Stores the rows in the specified table.

        If the commit variable is not specified, the transaction is not
        committed and the commit method must be called at a later point.  The
        rows must all be of the same type.

        :param rows: list[Row], the rows to be stored.
        :param tablename: str, the name of the table to insert into.
        :param commit: bool, commits if True.
        :param update: bool, updates conflicting rows if true.
        :return: bool
        """
        if not rows or rows[0].TABLE_NAME not in self.tablenames:
            return False
        groups = self._group_rows(rows)
        with self.conn.cursor() as curs:
            for tablename in self.tablenames:
                if tablename not in groups:
                    continue
                group = groups[tablename]
                query = (f'INSERT INTO {tablename} '
                         'VALUES %s '
                         'ON CONFLICT DO NOTHING ')

                rowtups = [x.to_row() for x in group]
                values = ','.join(['%s' for _ in range(len(rowtups[0]))])
                template = '({})'.format(values)
                extras.execute_values(curs, query, rowtups, template, 1000)
        if commit:
            self.conn.commit()
        return True

    def game_name_to_id(self, name):
        """
        Retrieves the twitch id number of the game with the given name.

        Caches responses from previous invocations.  Fixes the case for some
        Twitch games that have inconsistent casing across the API.

        :param name: str, name of the game.
        :return: int, twitch game id number.
        """
        # Twitch API is inconsistent on capitalization so we fix it here.
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
        Gets YoutubeStream objects with the given epoch.

        Retrieves the first limit number of YoutubeStream objects with epochs
        equal to the epoch param.

        :param epoch: int, epoch of entries.
        :param limit: int, the maximum number of streams to return.
        :return: list[YoutubeStream]
        """
        query = ('SELECT * '
                 'FROM youtube_stream '
                 'WHERE epoch = %s '
                 'ORDER BY epoch ASC '
                 'LIMIT %s ')
        cursor = self.conn.cursor()
        cursor.execute(query, (epoch, limit))
        rows = cursor.fetchall()
        return list(map(lambda x: YoutubeStream.from_row(x), rows))

    def update_ytstream_game(self, yts):
        """
        Updates the game_id of a YoutubeStream row.

        :param yts: YoutubeStream, the stream to be updated.
        :return: None
        """
        query = ('UPDATE youtube_stream '
                 'SET game_id = %s '
                 'WHERE channel_id = %s AND epoch = %s ')
        cursor = self.conn.cursor()
        cursor.execute(query, (yts.game_id, yts.channel_id, yts.epoch))

    def update_rows(self, rows, fields_to_update):
        """
        Updates database rows.

        Rows are matched using their primary key and then the selected fields
        are then updated.

        :param rows: Row or list(Row), the rows to update.
        :param fields_to_update: str or list(str), the names of the fields to
            update for each row.
        :return:
        """
        # TODO: complete method


    def set_twitch_affiliations(self, channels):
        """
        Upserts the twitch_channel affiliations.

        :param channels: [TwitchChannel], List of channels to upsert.
        :return:
        """
        self.store_rows(channels, 'twitch_channel')
        query = ('UPDATE twitch_channel '
                 'SET affiliation = %s '
                 'WHERE channel_id = %s ')
        args = [(x.affiliation, x.channel_id) for x in channels]
        cursor = self.conn.cursor()
        cursor.executemany(query, args)

    def set_youtube_affiliations(self, channels):
        """
        Upserts the youtube_channel affiliations.

        :param channels: [TwitchChannel], List of channels to upsert.
        :return:
        """
        self.store_rows(channels, 'youtube_channel')
        query = ('UPDATE youtube_channel '
                 'SET affiliation = %s '
                 'WHERE channel_id = %s ')
        args = [(x.affiliation, x.channel_id) for x in channels]
        cursor = self.conn.cursor()
        cursor.executemany(query, args)


class MongoManager:
    """
    Class for interacting with the MongoDB instance.
    """
    def __init__(self, host, port, db_name, user=None,
                 password=None, ssl=True):
        self.user = user
        self.password = password
        self.cols = ['twitch_top_games', 'twitch_streams', 'youtube_streams',
                     'twitch_channels', 'youtube_channels']
        self.timestamped_collections = self.cols[0:3]
        self.channel_collections = self.cols[3:5]
        self.client = MongoClient(host, port, ssl=ssl)
        self.conn = self.client[db_name]
        if user:
            self.conn.authenticate(user, password, source='admin')

    def check_indexes(self):
        """
        Initializes indexes on the timestamp field.

        :return:
        """
        for collname in self.timestamped_collections:
            coll = self.conn[collname]
            indexes = coll.index_information()
            if 'timestamp_1' not in indexes:
                logging.info('Index not found for collection: ' + collname)
                logging.info('Creating index on collection' + collname)
                coll.create_index('timestamp')
        for collname in self.channel_collections:
            coll = self.conn[collname]
            indexes = coll.index_information()
            if 'channel_id_1' not in indexes:
                logging.info('Index not found for collection: ' + collname)
                logging.info('Creating index on collection' + collname)
                coll.create_index('channel_id')

    def first_entry_after(self, start, collname):
        """
        Returns the timestamp of the first document after start.

        :param start: int, unix epoch.
        :param collname: str, name of the collection to search in.
        :return: int, unix epoch of the document with the smallest timestamp
        greater than start.
        """
        topgames = self.conn[collname]
        cursor = topgames.find(
            {'timestamp': {'$gt': start}}
        ).sort('timestamp', pymongo.ASCENDING)
        if cursor.count() > 0:
            return int(cursor[0]['timestamp'])
        else:
            return (1 << 31) - 1

    def findall(self, collname):
        """
        Returns an iterator to all documents in the collection.

        :param collname: str, name of the collection.
        :return:
        """
        if collname not in self.cols:
            raise KeyError('Collection not found ', collname)
        col = self.conn[collname]
        return col.find().batch_size(1)

    def contains_channel(self, channel_id):
        """
        Checks if twitch_channels contains the given channel.

        :param channel_id: int, the twitch channel's user_id.
        :return: bool, True if the collection contains the channel.
        """
        return self.conn.twitch_channels.count({'channel_id': channel_id})

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
        coll = self.conn[collname]
        cursor = coll.find(
            {'timestamp': {'$gte': start, '$lt': end}}
        ).sort('timestamp', pymongo.ASCENDING)
        return cursor

    def store(self, docs):
        """
        Stores a MongoDoc.

        :param docs: mongomodels.MongoDoc or iterable, the document(s) to be
            stored.
        :param collection: str, the name of the collection to store it in.
        :return: str, result of the insert operation.
        """
        if not isinstance(docs, collections.Iterable):
            docs = [docs]
        for doc in docs:
            if doc.COLLECTION not in self.cols:
                raise pymongo.errors.CollectionInvalid
            collection = self.conn[doc.COLLECTION]
            lastid = collection.insert_one(doc.todoc())
        return lastid
