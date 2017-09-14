import os
import sys
import time
from ruamel import yaml

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_PATH[0:len(DIR_PATH)-len('esportstracker/')])

from esportstracker.classifiers import YoutubeIdentifier
from esportstracker.dbinterface import PostgresManager
from esportstracker.aggregator import Aggregator


def classifydb():
    """
    Attempts to determine and update the game_id of every youtube stream in
    the database.

    Standalone classifier for testing purposes.
    """
    start = time.time()
    cfgpath = '../esportstracker/config/dev_config.yml'
    keypath = '../keys.yml'
    with open(cfgpath) as f:
        config = yaml.safe_load(f)
    with open(keypath) as f:
        keys = yaml.safe_load(f)
    dbn = config['postgres']['db_name']
    host = config['postgres']['host']
    port = config['postgres']['port']
    user = keys['postgres']['user']
    pwd = keys['postgres']['passwd']
    pgm = PostgresManager(host, port, user, pwd, dbn, {})
    yti = YoutubeIdentifier()
    limit = 200000

    count = 0
    updated = 0
    now = Aggregator.epoch_to_hour(time.time())
    epoch = pgm.earliest_epoch('youtube_stream')
    while epoch < now:
        if epoch % 360000 == 0:
            print(f'Total Scanned: {count} Total Updated: {updated}')
        yts = pgm.get_yts(epoch, limit)
        for stream in yts:
            yti.classify(stream)
            if stream.game_id:
                updated += 1
                pgm.update_ytstream_game(stream)
        epoch += 3600
        count += len(yts)

    pgm.commit()
    end = time.time()
    print('Classification Complete: {:.02f}s'.format(end - start))
    print('Total scanned: ', count)
    print('Total updated: ', updated)


if __name__ == '__main__':
    classifydb()