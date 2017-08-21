from models import *
import psycopg2
import logging
from psycopg2 import sql
from psycopg2 import extras


class PostgresManager:
    def __init__(self, host, port, user, password, dbname):
        """
        Class for interacting with the Postgres instance.

        Initializes
        :param host: str,
        """

        self.conn = psycopg2.connect(host=host, port=port, user=user,
                                     password=password, dbname=dbname)
        self. tables = ['games', 'twitch_game_viewer_count',
                        'twitch_stream', 'twitch_channel',
                        'channel_affiliation', 'org']
        self.initdb()

    @staticmethod
    def from_config(dbconfig):
        return PostgresManager(dbconfig['host'], dbconfig['port'], dbconfig[
            'user'], dbconfig['password'], dbconfig['db_name'])

    def commit(self):
        self.conn.commit()

    def initdb(self):
        """
        Creates tables if they don't exist.

        Initializes the database using the schema shown in the schema
        design file (PNG).
        """
        try:

            if not all(self.table_exists(t) for t in self.tables):
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
        game = (
            'CREATE TABLE game( '
            '    game_id integer PRIMARY KEY, '
            '    name text NOT NULL UNIQUE, '
            '    giantbomb_id integer '
            ');'
        )
        game_index = 'CREATE INDEX game_name_idx ON game USING HASH (name);'
        twitch_game_viewer_count = (
            'CREATE TABLE twitch_game_viewer_count( '
            '    game_id integer REFERENCES game(game_id), '
            '    epoch integer NOT NULL, '
            '    viewers integer NOT NULL, '
            '    PRIMARY KEY (game_id, epoch) '
            ');'
        )
        twitch_channel = (
            'CREATE TABLE twitch_channel( '
            '    channel_id integer PRIMARY KEY, '
            '    name text NOT NULL ' 
            ');'
        )
        twitch_stream = (
            'CREATE TABLE twitch_stream( ' 
            '    channel_id integer REFERENCES twitch_channel(channel_id), '
            '    epoch integer NOT NULL, '
            '    game_id integer REFERENCES game(game_id), '
            '    viewers integer NOT NULL, '
            '    stream_title text, '
            '    PRIMARY KEY (channel_id, epoch)'
            ');'
        )
        esports_org = (
            'CREATE TABLE esports_org( '
            '    org_id integer PRIMARY KEY, '
            '    name text NOT NULL '
            ');'
        )
        channel_affiliation = (
            'CREATE TABLE channel_affiliation( '
            '    org_id integer REFERENCES esports_org(org_id), '
            '    channel_id integer REFERENCES twitch_channel(channel_id), '
            '    PRIMARY KEY (channel_id) '
            ');'
        )
        curs = self.conn.cursor()
        if not self.table_exists('game'):
            curs.execute(game)
            curs.execute(game_index)
            logging.info('Created Table: game')
        if not self.table_exists('twitch_game_viewer_count'):
            curs.execute(twitch_game_viewer_count)
            logging.info('Created Table: twitch_game_viewer_count')
        if not self.table_exists('twitch_channel'):
            curs.execute(twitch_channel)
            logging.info('Created Table: twitch_channel')
        if not self.table_exists('twitch_stream'):
            curs.execute(twitch_stream)
            logging.info('Created Table: twitch_stream')
        if not self.table_exists('esports_org'):
            curs.execute(esports_org)
            logging.info('Created Table: esports_org')
        if not self.table_exists('channel_affiliation'):
            curs.execute(channel_affiliation)
            logging.info('Created Table: channel_affiliation')

    def table_exists(self, table):
        """
        Returns True if the table exists in the database.

        :param table: str, name of the table. Must be in the list of known
            table names.
        :return: bool
        """
        tables = ['game', 'twitch_game_viewer_count', 'twitch_stream',
                  'twitch_channel', 'channel_affiliation', 'esports_org']
        if table not in tables:
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

    def store_twitch_game_viewer_count(self, rows, commit=False):
        """
        Stores top game entries using conn.

        There is no error checking in this function so the game_id field of
        each row must already exist in the game table.
        :param rows: list[TwitchGameViewerCount],
        """
        if not rows:
            return

        query = ('INSERT INTO twitch_game_viewer_count '
                 'VALUES %s '
                 'ON CONFLICT DO NOTHING ')
        rows = [x.to_row() for x in rows]
        curs = self.conn.cursor()
        extras.execute_values(curs, query, rows, '(%s, %s, %s)', 1000)
        if commit:
            self.conn.commit()

    def store_games(self, games, commit=False):
        """
        Stores game entries if they do not already exist.

        :param games: list[Game] or list[TwitchAPIResponse], the list of rows to
        store.
        :param commit: bool, whether to commit after storing rows.
        :return:
        """
        if not games:
            return
        if type(games[0]) == TwitchGamesAPIResponse:
            games = list(Game.api_responses_to_games(games).values())

        query = ('INSERT INTO game (game_id, name, giantbomb_id) '
                 'VALUES %s '
                 'ON CONFLICT DO NOTHING')
        rows = [g.to_row() for g in games]
        curs = self.conn.cursor()
        extras.execute_values(curs, query, rows, '(%s, %s, %s)', 1000)
        if commit:
            self.conn.commit()

class MongoManager:
    def __init__(self):
        pass