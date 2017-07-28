# Converts the twitch_top_games V1 MongoDB entries to the V2 entries.
# V1 Style:
# {
#    'timestamp': int,
#    'games': {
#       by 'giantbomb_id': {
#           'name': str,
#           'viewers': int,
#           'channels': int
#       }
#       ...
# }
# New V2 Style
# {
#    'timestamp': int,
#    'games': {
#       by 'twitch_id': {
#           'name': str,
#           'viewers': int,
#           'channels': int,
#           'id': int,
#           'giantbomb_id': int
#       }
#       ...
# }
import pymongo
import os
from scraper.twitch_scraper import TwitchScraper
import pickle
import time
import sys

retrieved_ids = {'PokĂŠmon Sun/Moon': 491599}


def retrieve_oldest_mongo_doc(coll, num):
    cursor = coll.find().sort("timestamp", pymongo.ASCENDING).limit(1)
    doc = cursor[0:num]
    return doc


def retrieve_missing_info(game, giantbomb_id):
    """ Returns copy of game with two missing fields added."""
    contents = game.copy()
    contents['giantbomb_id'] = giantbomb_id

    # Need to retrieve the twitch id corresponding to game
    # Cache ids to avoid running into Twitch API limitations.
    gamename = contents['name']
    if gamename not in retrieved_ids:
        print("Retrieving: {}  : ".format(gamename), end='')
        scraper = TwitchScraper()
        retrieved_ids[gamename] = scraper.gamename_to_id(gamename)
        print(str(retrieved_ids[gamename]))
    contents['id'] = retrieved_ids[gamename]
    return contents


def v1tov2(entry):
    """ Assume entry in V1 format. """
    new_entry = {'timestamp': entry['timestamp']}
    games = {}
    for giantbomb_id, game in entry['games'].items():
        contents = retrieve_missing_info(game, giantbomb_id)
        games[str(contents['id'])] = contents
    new_entry['games'] = games
    return new_entry

def update_all():
    os.chdir('..')
    os.chdir('scraper')
    print("Starting...")
    start = time.time()
    client = pymongo.MongoClient()
    db = client.esports_stats
    coll = db.twitch_top_games

    oldest = retrieve_oldest_mongo_doc(coll, 50000)

    for entry in oldest:
        print(entry)
        oldestv2 = v1tov2(entry)
        print(oldestv2)
    end = time.time()
    print("Total Time: ", end-start)

def test_retrieval(gamename):
    os.chdir('..')
    os.chdir('scraper')
    scraper = TwitchScraper()
    id = scraper.gamename_to_id(gamename)
    print(id)


if __name__ == '__main__':
    #update_all()
    test_retrieval('Pokémon Sun/Moon')
    #test_retrieval('RuneScape')

# Step 1: Read each mongo entry.
# Step 2: For each game, retrieve game_id
# Step 3: Reformat entry based on result
