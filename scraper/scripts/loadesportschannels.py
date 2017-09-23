import os
import sys
from ruamel import yaml

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_PATH[0:len(DIR_PATH)-len('esportstracker/')])

from esportstracker.models import EsportsOrg, ChannelAffiliation, TwitchChannel
from esportstracker import dbinterface


def main():
    parent = DIR_PATH[0:len(DIR_PATH) - len('scripts/')]
    chanpath = parent + '/esportstracker/config/esports_channels.yml'
    configpath = parent + '/esportstracker/config/config.yml'
    secretspath = parent + '/keys.yml'
    with open(chanpath, 'r') as f:
        orgs = yaml.safe_load(f)['twitch']
    with open(configpath, 'r') as f:
        config = yaml.safe_load(f)
        dbname = config['postgres']['db_name']
        host = config['postgres']['host']
        port = config['postgres']['port']
    with open(secretspath, 'r') as f:
        secrets = yaml.safe_load(f)
        user = secrets['postgres']['user']
        password = secrets['postgres']['passwd']
    db = dbinterface.PostgresManager(host, port, user, password, dbname)
    orgrows = []
    aff = []
    chans = []
    for orgname in orgs.keys():
        orgrows.append(EsportsOrg(orgname))
        for channel in orgs[orgname]:
            chans.append(TwitchChannel(channel['id'], channel['name']))
            aff.append(ChannelAffiliation(orgname, channel['id']))
    db.store_rows(chans, 'twitch_channel', commit=True)
    db.store_rows(orgrows, 'esports_org', commit=True)
    db.store_rows(aff, 'channel_affiliation', commit=True)
    print('Channels Stored')


if __name__ == '__main__':
    main()