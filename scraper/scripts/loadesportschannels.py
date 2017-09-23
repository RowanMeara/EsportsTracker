import os
import sys
from ruamel import yaml

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_PATH[0:len(DIR_PATH)-len('esportstracker/')])

from esportstracker.models import TournamentOrganizer
from esportstracker.models import TwitchChannel, YoutubeChannel
from esportstracker import dbinterface
from esportstracker.youtube_scraper import YoutubeScraper


def getorgs():
    """
    Loads the esports org file.

    Any missing channel ids are retrieved based on their names.

    :param chanpath: str, path to channel yaml file.
    :return: (dict, dict), ,
    """
    parent = DIR_PATH[0:len(DIR_PATH) - len('scripts/')]
    chanpath = parent + '/esportstracker/config/esports_channels.yml'
    configpath = parent + '/esportstracker/config/config.yml'
    secretspath = parent + '/keys.yml'
    with open(chanpath, 'r') as f:
        cfg = yaml.safe_load(f)
        torgs = cfg['twitch']
        ytorgs = cfg['youtube']
    yts = YoutubeScraper(configpath, secretspath)
    for orgn in ytorgs.keys():
        for chan in ytorgs[orgn]:
            if 'id' not in chan:
                chan['id'] = yts.get_channel_ids(chan['name'])[chan['name']]
    for orgn in torgs.keys():
        for chan in torgs[orgn]:
            if type(chan['id']) == str:
                chan['id'] = int(chan['id'])

    with open(chanpath, 'w') as f:
        yaml.dump(cfg, f)
    return torgs, ytorgs


def main():
    parent = DIR_PATH[0:len(DIR_PATH) - len('scripts/')]
    configpath = parent + '/esportstracker/config/config.yml'
    secretspath = parent + '/keys.yml'
    with open(configpath, 'r') as f:
        config = yaml.safe_load(f)
        dbname = config['postgres']['db_name']
        host = config['postgres']['host']
        port = config['postgres']['port']
    with open(secretspath, 'r') as f:
        secrets = yaml.safe_load(f)
        user = secrets['postgres']['user']
        password = secrets['postgres']['passwd']
    torgs, ytorgs = getorgs()

    db = dbinterface.PostgresManager(host, port, user, password, dbname)
    orgrows = []
    tchans = []
    ychans = []
    for orgname in torgs.keys():
        orgrows.append(TournamentOrganizer(orgname))
        for channel in torgs[orgname]:
            tc = TwitchChannel(channel['id'], channel['name'], orgname)
            tchans.append(tc)
    for orgname in ytorgs.keys():
        orgrows.append(TournamentOrganizer(orgname))
        for channel in ytorgs[orgname]:
            yc = YoutubeChannel(channel['id'], channel['name'], 'en', None, orgname)
            ychans.append(yc)

    db.store_rows(orgrows, 'tournament_organizer')
    db.set_twitch_affiliations(tchans)
    db.store_rows(ychans, 'youtube_channel')
    db.commit()
    print('Channels Stored')


if __name__ == '__main__':
    main()