import os
import sys
import pymongo
from bson.objectid import ObjectId
import os
import time
import yaml

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_PATH[0:len(DIR_PATH)-len('scripts/')])

from esportstracker.models.mongomodels import TwitchStreamsAPIResponse, TwitchStreamSnapshot
from esportstracker.apiclients import TwitchAPIClient

# V1 entries are organized as follows:
#
#  {'timestamp': epoch int,
#   'game': str, name of the game
#   'streams': broadcasts}
#
#    streams[broadcast['snippet']['channelId']] = {
#               'display_name':     stream['channel']['display_name'],
#               'viewers':          stream['viewers'],
#               'game':             stream['game'],
#               'status':           stream['channel']['status'],
#               'broadcaster_id':   stream['channel']['_id']
#    }
#
# V2 entries are organized as follows:
#
#  {'timestamp': epoch int,
#   'game_id': int
#   'streams': streams}
#
#    streams[stream['user_id'] = {
#               'viewers':          stream['viewer_count'],
#               'game_id':          gameid,
#               'language':         stream['language'],
#               'bctype':           stream['type'],
#               'title':            stream['title'],
#               'stream_id':        stream['id'],
#               'broadcaster_id':   stream['user_id'],
#           }, ...
#  ]
#

def retrieve_v1(coll, num):
    cursor = coll.find({'game_id': {'$exists': False}})
    doc = cursor[0:num]
    return doc


def numv1(coll):
    return coll.count({"game_id": {"$exists": False}})


def numv2(coll):
    return coll.count({"game_id": {"$exists": True}})


def retrieve_all(coll, num):
    cursor = coll.find()
    return cursor[0:num]


def onev1tov2(v1doc, client):
    """ Assume entry in V1 format. """
    streams = {}
    timestamp = v1doc['timestamp']
    gameid = client.getgameid(v1doc['game'])
    for channel_id, stream in v1doc['streams'].items():
        params = {
            'viewers': int(stream['viewers']),
            'game_id': client.getgameid(stream['game']),
            'language': 'en',
            'bctype': 'live',
            'title': stream['status'],
            'stream_id': None,
            'broadcaster_id': int(stream['broadcaster_id']),
        }
        streams[params['broadcaster_id']] = TwitchStreamSnapshot(**params)
    return TwitchStreamsAPIResponse(timestamp, streams, gameid).todoc()


def v1tov2(keypath):
    """ Converts V1 to V2 entries. """
    print("Starting...")
    start = time.time()
    with open(keypath, 'r') as f:
        keys = yaml.load(f)
        m = keys['mongodb']['write']
        user = m['user']
        pwd = m['pwd']
        client = keys['twitchclientid']
        secret = keys['twitchsecret']
    apiclient = TwitchAPIClient('https://api.twitch.tv/', client, secret)
    client = pymongo.MongoClient()
    conn = client.esports_stats
    conn.authenticate(user, pwd, source='admin')
    coll = conn.twitch_streams
    print("Num V1 Documents: {}".format(numv1(coll)))
    print("Num V2 Documents: {}".format(numv2(coll)))
    v1docs = retrieve_v1(coll, 5000)
    count = 0
    while v1docs:
        for v1doc in v1docs:
            count += 1
            if count % 1000 == 0:
                print("Progress: ", count)
            v2doc = onev1tov2(v1doc, apiclient)
            filter = {'_id': ObjectId(v1doc['_id'])}
            res = coll.replace_one(filter, v2doc)
        v1docs = retrieve_v1(coll, 5000)

    end = time.time()
    print("Total Time: ", end-start)
    print("Total V1 entries updated: ", count)
    print("Num V1 Documents: {}".format(numv1(coll)))
    print("Num V2 Documents: {}".format(numv2(coll)))



if __name__ == '__main__':
    keypath = DIR_PATH[0:len(DIR_PATH)-len('scripts/')] + '/keys.yml'
    v1tov2(keypath)