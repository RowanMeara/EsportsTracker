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
        self.tablenames = {'game', 'twitch_game_vc', 'twitch_stream',
                           'twitch_channel', 'channel_affiliation',
                           'esports_org'}
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
        game = (
            'CREATE TABLE game( '
            '    game_id integer PRIMARY KEY, '
            '    name text NOT NULL UNIQUE, '
            '    giantbomb_id integer '
            ');'
        )
        game_index = 'CREATE INDEX game_name_idx ON game USING HASH (name);'
        twitch_game_vc = (
            'CREATE TABLE twitch_game_vc( '
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
        if not self.table_exists('twitch_game_vc'):
            curs.execute(twitch_game_vc)
            logging.info('Created Table: twitch_game_vc')
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
    def game_name_to_id(self, conn, name):
        # TODO: Figure out source of inconsistent naming data
        if name not in self.esports_games:
            for game in self.esports_games:
                if name.lower() == game.lower():
                    name = game
                    break
        query = ('SELECT game_id '
                 'FROM games '
                 'WHERE name = %s')
        cursor = conn.cursor()
        cursor.execute(query, (name,))
        return cursor.fetchone()[0]


class MongoManager:
    def __init__(self):
        pass