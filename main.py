import requests
from datetime import datetime
import time
import os, sys
import ssl
import pymongo
from dotenv import load_dotenv
import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
root.addHandler(handler)

load_dotenv()

ASSET = os.getenv('ASSET')
PYMONGO_USERNAME = os.getenv('PYMONGO_USERNAME')
PYMONGO_PASSWORD = os.getenv('PYMONGO_PASSWORD')

PYMONGO_DB_URL = f"mongodb+srv://{PYMONGO_USERNAME}:{PYMONGO_PASSWORD}@trades.cdsd5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
client = pymongo.MongoClient(PYMONGO_DB_URL, ssl_cert_reqs=ssl.CERT_NONE)
db = client.development
prices = db.prices

url = f'https://api.coinbase.com/v2/prices/{ASSET}-USD/buy'

if __name__ == '__main__':
    while True:
        #only run the task at the change of every minute
        t = datetime.utcnow()
        #on average a request to the coinbase API takes about a 1/10th of a second or 100,000 microseconds
        #subtract out an additional 1/10th of a second to the sleep process to account for that
        seconds = 60 - (t.second + (t.microsecond)/1000000.0)
        time.sleep(seconds)

        res = requests.get(url)

        data = {
            'asset': ASSET,
            'price': res.json()['data']['amount'],
            'utc_time': datetime.utcnow()
        }

        logging.info(data)

        prices.insert_one(data)


