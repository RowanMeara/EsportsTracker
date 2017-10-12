import yaml
import sqlalchemy
from sqlalchemy import Column, ForeignKeyConstraint, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy.engine.url import URL


Base = declarative_base()


def connect(host, port, dbname, username, password):
    """
    Connects to the database and creates missing schema elements.

    See schema.png for a schema diagram.

    :param host: str, the database host.
    :param port: str or int, the database port.
    :param dbname: str, the name of the database.
    :param username: str, the username.
    :param password: str, the user's password.
    :return: sqlalchemy engine.
    """
    params = {
        'drivername': 'postgres',
        'host': host,
        'port': port,
        'username': username,
        'password': password,
        'database': dbname
    }
    engine = sqlalchemy.create_engine(URL(**params))
    Base.metadata.create_all(engine)
    return engine


class Game(Base):
    __tablename__ = 'game'
    game_id = Column('game_id', pg.INTEGER, primary_key=True)
    name = Column('name', pg.TEXT, unique=True, nullable=False)
    giantbomb_id = Column('giantbomb_id', pg.INTEGER, nullable=True)


class GameViewerCounts(Base):
    __tablename__ = 'twitch_game_vc'
    __table_args__ = (
        PrimaryKeyConstraint('game_id', 'epoch'),
        ForeignKeyConstraint(['game_id'], ['game.game_id'])
    )
    game_id = Column('game_id', pg.INTEGER)
    epoch = Column('epoch', pg.INTEGER)
    viewers = Column('viewers', pg.INTEGER, nullable=False)


class TwitchChannel(Base):
    __tablename__ = 'twitch_channel'
    __table_args__ = (
        ForeignKeyConstraint(['affiliation'], ['tournament_organizer.org_name']),
    )
    channel_id = Column('channel_id', pg.INTEGER, primary_key=True)
    name = Column('name', pg.TEXT, unique=True, nullable=True)
    affiliation = Column('affiliation', pg.TEXT, nullable=True)


class TournamentOrganizer(Base):
    __tablename__ = 'tournament_organizer'
    org_name = Column('org_name', pg.TEXT, primary_key=True)


class TwitchStream(Base):
    __tablename__ = 'twitch_stream'
    __table_args__ = (
        PrimaryKeyConstraint('channel_id', 'epoch'),
        ForeignKeyConstraint(['channel_id'], ['twitch_channel.channel_id']),
        ForeignKeyConstraint(['game_id'], ['game.game_id'])
    )
    channel_id = Column('channel_id', pg.INTEGER)
    epoch = Column('epoch', pg.INTEGER, nullable=False)
    game_id = Column('game_id', pg.INTEGER, nullable=False)
    viewers = Column('viewers', pg.INTEGER, nullable=False)
    title = Column('title', pg.TEXT)
    language = Column('language', pg.TEXT)
    stream_id = Column('stream_id', pg.BIGINT)
    stream_type = Column('stream_type', pg.TEXT)


class YouTubeChannel(Base):
    __tablename__ = 'youtube_channel'
    __table_args__ = (
        ForeignKeyConstraint(['affiliation'], ['tournament_organizer.org_name']),
    )
    channel_id = Column('channel_id', pg.TEXT, primary_key=True)
    name = Column('name', pg.TEXT, nullable=False)
    main_language = Column('main_language', pg.TEXT, nullable=True)
    description = Column('description', pg.TEXT, nullable=True)
    affiliation = Column('affiliation', pg.TEXT, nullable=True)


class YouTubeStream(Base):
    __tablename__ = 'youtube_stream'
    __table_args__ = (
        PrimaryKeyConstraint('video_id', 'epoch'),
        ForeignKeyConstraint(['game_id'], ['game.game_id']),
        ForeignKeyConstraint(['channel_id'], ['youtube_channel.channel_id'])
    )
    video_id = Column('video_id', pg.TEXT)
    epoch = Column('epoch', pg.INTEGER)
    channel_id = Column('channel_id', pg.TEXT, nullable=False)
    game_id = Column('game_id', pg.INTEGER, nullable=True)
    viewers = Column('viewers', pg.INTEGER, nullable=False)
    title = Column('title', pg.TEXT, nullable=False)
    language = Column('language', pg.TEXT, nullable=True)
    tags = Column('tags', pg.TEXT, nullable=True)


if __name__ == '__main__':
    with open('../../keys.yml') as f:
        keys = yaml.load(f)['postgres']
    engine = connect('localhost', 5432, 'esports_stats', keys['user'], keys['passwd'])
    print('Done')