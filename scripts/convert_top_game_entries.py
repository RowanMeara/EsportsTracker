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

from pymongo import MongoClient

# Step 1: Read each mongo entry.
# Step 2: For each game, retrieve game_id
# Step 3: Reformat entry based on result
