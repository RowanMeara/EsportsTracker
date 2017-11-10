import os
import sys
import time
from ruamel import yaml

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_PATH[0:len(DIR_PATH)-len('scripts/')])

from esportstracker.classifiers import YouTubeGameClassifier
from esportstracker.dbinterface import PostgresManager
from esportstracker.aggregator import Aggregator


def classifydb():
    """
    Attempts to determine and update the game_id of every youtube stream in
    the database.

    Standalone classifier for testing purposes.
    """
    print('Classifying YouTube Games')
    start = time.time()
    parent = DIR_PATH[0:len(DIR_PATH) - len('scripts/')]
    cfgpath = parent + '/esportstracker/config/config.yml'
    keypath = parent + '/keys.yml'
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
    yti = YouTubeGameClassifier()
    limit = 200000

    count = 0
    updated = 0
    classified = 0
    now = Aggregator.epoch_to_hour(time.time())
    epoch = pgm.earliest_epoch('youtube_stream')
    while epoch < now:
        if epoch % 900000 == 0:
            pgm.commit()
            print(f'Total Scanned: {count}  Total Updated: {updated} ',
                  '{:.1f} entries/s'.format(count/(time.time()-start)))
        yts = pgm.get_yts(epoch, limit)
        for stream in yts:
            old_game_id = stream.game_id
            yti.classify_game(stream)
            if old_game_id != stream.game_id:
                updated += 1
                pgm.update_rows(stream, 'game_id')
            classified += 1 if stream.game_id else 0
        epoch += 3600
        count += len(yts)

    pgm.commit()
    end = time.time()
    print('Classification Complete: {:.02f}s'.format(end - start))
    print('Total scanned: ', count)
    print('Total classified: ', classified)
    print('Updated: {:.1f}%'.format(100 * updated / count))
    print('Classified: {:.1f}%'.format(100 * classified / count))


if __name__ == '__main__':
    classifydb()