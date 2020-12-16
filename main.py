from cryptofeed.feedhandler import FeedHandler
from cryptofeed import exchanges
from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK

import pandas as pd
import redis
import json

import time

import os
import shutil
from glob import glob

import multiprocessing
import threading

from utils import flush_redis
from algos.daddy.bot import daddy_bot, daddy_trade, daddy_book
from algos.vol_trend.bot import vol_bot

from utils import print

EXCHANGES = pd.read_csv('exchanges.csv')
r = redis.Redis(host='localhost', port=6379, db=0)

if not os.path.isdir("logs/"):
    os.makedirs("logs/")

flush_redis(r, EXCHANGES)

f = FeedHandler(retries=100000)

def bot():
    daddy_thread = multiprocessing.Process(target=daddy_bot, args=())
    daddy_thread.start()

    while True:
        if int(r.get('daddy_enabled').decode()) != 1:
            daddy_thread.terminate()

        time.sleep(1)  

bot_thread = threading.Thread(target=bot)
bot_thread.start()

async def trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):    
    if int(r.get('daddy_enabled').decode()) == 1:
        await daddy_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price)

async def book(feed, pair, book, timestamp, receipt_timestamp):    
    if int(r.get('daddy_enabled').decode()) == 1:
        await daddy_book(feed, pair, book, timestamp, receipt_timestamp)

for exchange, details in EXCHANGES.groupby('cryptofeed_name'):
    channels=[TRADES, L2_BOOK]
    callbacks={TRADES: trade, L2_BOOK: book}

    b = getattr(exchanges, exchange)
    print(details)
    f.add_feed(getattr(exchanges, exchange)(pairs=details['cryptofeed_symbol'].values, channels=channels, callbacks=callbacks), timeout=-1)


f.run()