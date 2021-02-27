"""
Cron job to wipe the DB at a specified time
"""

from main import prices

import sys

import pymongo
from bson.objectid import ObjectId

# most recent prices are first
all_prices = list(prices.find({}, sort=[('utc_time', pymongo.DESCENDING)]))
to_delete = all_prices[100:]
if len(to_delete) == 0:
    sys.exit('Not enough price data to delete yet.')
else:
    for price_point in to_delete:
        prices.delete_one({'_id': ObjectId(price_point['_id'])})