import os
import sys
from ruamel import yaml

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_PATH[0:len(DIR_PATH)-len('scripts/')])

from esportstracker.models.postgresmodels import TournamentOrganizer
from esportstracker.models.postgresmodels import TwitchChannel, YouTubeChannel
from esportstracker import dbinterface
from esportstracker.apiclients import YouTubeAPIClient, TwitchAPIClient

"""
Upserts organizer affiliations for twitch and youtube channels.
"""


def getorgs():
    """
    Loads the esports org file.

    Any missing channel ids are retrieved based on their names.

    :param chanpath: str, path to channel yaml file.
    :return: (dict, dict), ,
    """
    parent = DIR_PATH[0:len(DIR_PATH) - len('scripts/')]
    chanpath = parent + '/esportstracker/config/esports_channels.yml'
    secretspath = parent + '/keys.yml'
    with open(chanpath, 'r') as f:
        cfg = yaml.safe_load(f)
        torgs = cfg['twitch']
        ytorgs = cfg['youtube']
    with open(secretspath) as f:
        s = yaml.safe_load(f)
        id = s['youtubeclientid']
        secret = s['youtubesecret']
        tid = s['twitchclientid']
        tsecret = s['twitchsecret']
    yts = YouTubeAPIClient('https://www.googleapis.com/youtube/v3', id, secret)
    twc = TwitchAPIClient('https://api.twitch.tv', tid, tsecret)
    for orgn in ytorgs.keys():
        for chan in ytorgs[orgn]:
            if 'id' not in chan:
                chan['id'] = yts.getid(chan['name'])
    for orgn in torgs.keys():
        for chan in torgs[orgn]:
            if 'id' not in chan:
                chan['id'] = twc.getuserid(chan['name'].lower())
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
            tc = TwitchChannel(channel_id=channel['id'],
                               display_name=channel['name'],
                               affiliation=orgname)
            tchans.append(tc)
    for orgname in ytorgs.keys():
        orgrows.append(TournamentOrganizer(orgname))
        for channel in ytorgs[orgname]:
            yc = YouTubeChannel(channel_id=channel['id'], affiliation=orgname)
            ychans.append(yc)

    db.store_rows(orgrows, 'tournament_organizer')
    db.update_rows(tchans, 'affiliation')
    db.update_rows(ychans, 'affiliation')
    db.commit()
    print('Channels Stored')


if __name__ == '__main__':
    main()
