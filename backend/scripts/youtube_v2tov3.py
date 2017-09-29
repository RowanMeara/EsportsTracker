import os
import sys
import pymongo
from bson.objectid import ObjectId
import os
import time
import yaml

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, DIR_PATH[0:len(DIR_PATH)-len('scripts/')])

from esportstracker.models.mongomodels import YTLivestreams, YTLivestream

# V2 entries will have one timestamp for every entry organized as follows:
#
#  {'timestamp': epoch int, 'broadcasts': broadcasts}
#
#    broadcasts[broadcast['snippet']['channelId']] = {
#      'title':                 Title of broadcast,
#      'broadcaster_name':      channel name,
#      'broadcast_id':          Youtube videoId,
#      'concurrent_viewers':    int,
#      'language':              defaultAudioLanguage or 'unknown',
#      'tags':                  Youtube tags or []
#  }
#


def retrieve_v2(coll, num):
    cursor = coll.find({"streams": {"$exists": False}})
    doc = cursor[0:num]
    return doc


def numv2(coll):
    return coll.count({"streams": {"$exists": False}})


def numv3(coll):
    return coll.count({"streams": {"$exists": True}})

def retrieve_all(coll, num):
    cursor = coll.find()
    return cursor[0:num]


def onev2tov3(v2doc):
    """ Assume entry in V2 format. """
    streams = []
    timestamp = v2doc['timestamp']
    for channel_id, broadcast in v2doc['broadcasts'].items():
        title = broadcast['title']
        channame = broadcast['broadcaster_name']
        chanid = broadcast['broadcast_id']
        vidid = ''
        viewers = broadcast['concurrent_viewers'],
        language = broadcast['language']
        tags = broadcast['tags']
        streams.append(YTLivestream(title, channame, chanid, vidid, viewers, language, tags))
    return YTLivestreams(streams, timestamp).to_dict()


def v2tov3(keypath):
    """ Pulls timestamps out of V1 entries. """
    print("Starting...")
    start = time.time()
    with open(keypath, 'r') as f:
        m = yaml.load(f)['mongodb']['write']
        user = m['user']
        pwd = m['pwd']
    client = pymongo.MongoClient()
    conn = client.esports_stats
    conn.authenticate(user, pwd, source='admin')
    coll = conn.youtube_streams
    print("Num V2 Documents: {}".format(numv2(coll)))
    print("Num V3 Documents: {}".format(numv3(coll)))
    v2docs = retrieve_v2(coll, 50000)
    count = 0
    for v2doc in v2docs:
        count += 1
        if count % 100 == 0:
            print("Progress: ", count)
        v3doc = onev2tov3(v2doc)
        filter = {'_id': ObjectId(v2doc['_id'])}
        print(v3doc)
        print(v2doc)
        exit()
        res = coll.replace_one(filter, v3doc)
    end = time.time()
    print("Total Time: ", end-start)
    print("Total V2 entries updated: ", count)
    print("Num V2 Documents: {}".format(numv2(coll)))
    print("Num V3 Documents: {}".format(numv3(coll)))



if __name__ == '__main__':
    keypath = DIR_PATH[0:len(DIR_PATH)-len('scripts/')] + '/keys.yml'
    v2tov3(keypath)