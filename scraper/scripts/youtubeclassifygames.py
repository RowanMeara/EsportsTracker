import os
from ruamel import yaml
from esportstracker.game_identifier import YoutubeIdentifier
from esportstracker.dbinterface import PostgresManager
import time
from esportstracker.aggregator import Aggregator


def classifydb():
    """
    Standalone classifier for testing purposes.
    """
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
    epoch = 0
    limit = 200
    now = Aggregator.epoch_to_hour(time.time())
    count = 0
    updated = 0
    while epoch < now:
        if count % 10000 == 0:
            print(f'Total Scanned: {count} Total Updated: {updated}')
        yts = pgm.get_yts(epoch, limit)
        if not yts:
            break
        epoch = yts[-1].epoch
        for stream in yts:
            yti.classify(stream)
            if stream.game_id:
                updated += 1
                pgm.update_ytstream_game(stream)
        count += len(yts)
    pgm.commit()
    print('Classification Complete')
    print('Total scanned: ', count)
    print('Total updated: ', updated)

if __name__ == '__main__':
    classifydb()