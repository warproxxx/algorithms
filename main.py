from cryptofeed.feedhandler import FeedHandler
from cryptofeed import exchanges
from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK
import requests

import pandas as pd
import redis
import json
import re

import time

import os
import shutil
from glob import glob

import multiprocessing
import threading

from utils import flush_redis
from algos.daddy.bot import daddy_bot, daddy_trade, daddy_book
from algos.vol_trend.bot import vol_bot, vol_trend_trade, vol_trend_book

from utils import print

EXCHANGES = pd.read_csv('exchanges.csv')
r = redis.Redis(host='localhost', port=6379, db=0)

if os.path.isdir("logs/"):
    shutil.rmtree("logs/")

if not os.path.isdir("logs/"):
    os.makedirs("logs/")

flush_redis(r, EXCHANGES)

f = FeedHandler(retries=100000)

def bot():
    daddy_thread = multiprocessing.Process(target=daddy_bot, args=())
    daddy_thread.start()

    vol_thread = multiprocessing.Process(target=vol_bot, args=())
    vol_thread.start()

    while True:
        if float(r.get('daddy_enabled').decode()) != 1:
            daddy_thread.terminate()

        if daddy_thread.is_alive() == False:
            if float(r.get('daddy_enabled').decode()) == 1:
                daddy_thread = multiprocessing.Process(target=daddy_bot, args=())
                daddy_thread.start()

        if float(r.get('vol_trend_enabled').decode()) != 1:
            vol_thread.terminate()

        if vol_thread.is_alive() == False:
            if float(r.get('vol_trend_enabled').decode()) == 1:
                vol_thread = multiprocessing.Process(target=vol_bot, args=())
                vol_thread.start()

        time.sleep(1)  

bot_thread = threading.Thread(target=bot)
bot_thread.start()

async def trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):  
    if float(r.get('daddy_enabled').decode()) == 1:
        await daddy_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price)
    
    if feed == 'FTX':
        await vol_trend_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price)

async def book(feed, pair, book, timestamp, receipt_timestamp):    
    if float(r.get('daddy_enabled').decode()) == 1:
        await daddy_book(feed, pair, book, timestamp, receipt_timestamp)

    if float(r.get('vol_trend_enabled').decode()) == 1:
        if feed == 'FTX':
            await vol_trend_book(feed, pair, book, timestamp, receipt_timestamp)

PAIRS = pd.read_csv('pairs.csv')

for exchange, details in PAIRS.groupby('cryptofeed_name'):
    channels=[TRADES, L2_BOOK]
    callbacks={TRADES: trade, L2_BOOK: book}

    b = getattr(exchanges, exchange)
    
    pairs_list = details['cryptofeed_symbol'].values
    new_pairs_list = []

    for pair in pairs_list:
        if "MOVE" in pair:
            pairs = json.loads(requests.get('https://ftx.com/api/markets').text)['result']
            new_pairs_list = new_pairs_list + [pair['name'] for pair in pairs if re.search("MOVE-20[0-9][0-9]Q", pair['name'])]
        else:
            new_pairs_list.append(pair)

    print("{} {}".format(exchange, new_pairs_list))
    f.add_feed(getattr(exchanges, exchange)(pairs=new_pairs_list, channels=channels, callbacks=callbacks), timeout=-1)


f.run()