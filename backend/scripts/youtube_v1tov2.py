# Original MongoDB entries for each youtube broadcast consisted of:
#
#    broadcasts[broadcast['snippet']['channelId']] = {
#      'timestamp': time.time(),
#      'title': broadcast['snippet']['title'],
#      'broadcaster_name': broadcast['snippet']['channelTitle'],
#      'broadcast_id': broadcast['id']['videoId'],
#      'concurrent_viewers': view_counts[broadcast['id']['videoId']]
#  }
#
# New entries will have one timestamp for every entry organized as follows:
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
# Note that defaultAudioLanguage and tags are only returned in a small
# fraction of API results but we will store unknown for results without the
# field.
#

import pymongo
from bson.objectid import ObjectId
import os
import time


def retrieve_v1(coll, num):
    cursor = coll.find({"timestamp": {"$exists": False }})
    doc = cursor[0:num]
    return doc


def retrieve_all(coll, num):
    cursor = coll.find()
    return cursor[0:num]


def isv15(entry):
    if 'broadcasts' not in entry:
        return False
    firstbc = next(iter(entry['broadcasts']))
    return 'tags' not in entry['broadcasts'][firstbc]


def onev1tov2(v1doc):
    """ Assume entry in V1 format. """
    new_entry = {'timestamp': -1, 'broadcasts': {}}
    for channel_id, broadcast in v1doc.items():
        if type(broadcast) == ObjectId:
            continue
        new_entry['timestamp'] = int(broadcast['timestamp'])
        new_entry['broadcasts'][channel_id] = {
            'title': broadcast['title'],
            'broadcaster_name': broadcast['broadcaster_name'],
            'broadcast_id': broadcast['broadcast_id'],
            'concurrent_viewers': broadcast['concurrent_viewers'],
            'language': 'unknown',
            'tags': []
        }
    return new_entry


def updatev15(doc):
    v2doc = {'timestamp': int(doc['timestamp']), 'broadcasts': {}}
    for channel_id, bc in doc['broadcasts'].items():
        v2doc['broadcasts'][channel_id] = {
            'tags': [],
            'language': 'unknown',
            'title': bc['title'],
            'broadcaster_name': bc['broadcaster_name'],
            'broadcast_id': bc['broadcast_id'],
            'concurrent_viewers': bc['concurrent_viewers']
        }
    return v2doc


def v1tov2():
    """ Pulls timestamps out of V1 entries. """
    print("Starting...")
    start = time.time()
    client = pymongo.MongoClient()
    db = client.esports_stats
    coll = db.youtube_streams

    v1docs = retrieve_v1(coll, 50000)
    count = 0
    for v1doc in v1docs:
        count += 1
        if count % 100 == 0:
            print("Progress: ", count)
        v2doc = onev1tov2(v1doc)
        filter = {'_id': ObjectId(v1doc['_id'])}
        res = coll.replace_one(filter, v2doc)

    end = time.time()
    print("Total Time: ", end-start)
    print("Total V1 entries updated: ", count)


def v15tov2():
    """ Pulls timestamps out of V1 entries. """
    print("Starting V1.5 to V2")
    start = time.time()
    client = pymongo.MongoClient()
    db = client.esports_stats
    coll = db.youtube_streams

    docs = retrieve_all(coll, 50000)
    total_entries = docs.count()
    countv15 = 0
    countv2 = 0
    for doc in docs:
        if isv15(doc):
            countv15 += 1
            v2doc = updatev15(doc)
            filter = {'_id': ObjectId(doc['_id'])}
            res = coll.replace_one(filter, v2doc)
        else:
            countv2 += 1

    end = time.time()
    print("V15 Time: ", end-start)
    print("Total V1.5 entries updated: ", countv15)
    print("Total V2 entries ignored: ", countv2)
    print("Total Entries:", total_entries)

if __name__ == '__main__':
    v1tov2()
    v15tov2()
