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
from algos.altcoin.bot import alt_bot, altcoin_trade, altcoin_book

from utils import print

EXCHANGES = pd.read_csv('exchanges.csv')
r = redis.Redis(host='localhost', port=6379, db=0)

if os.path.isdir("logs/"):
    if os.path.isdir("old_logs/"):
        shutil.rmtree("old_logs/")

    shutil.move("logs/", "old_logs")

if not os.path.isdir("logs/"):
    os.makedirs("logs/")

flush_redis(r, EXCHANGES)

f = FeedHandler(retries=100000)

def bot():
    daddy_thread = multiprocessing.Process(target=daddy_bot, args=())
    daddy_thread.start()

    vol_thread = multiprocessing.Process(target=vol_bot, args=())
    vol_thread.start()

    altcoin_thread = multiprocessing.Process(target=alt_bot, args=())
    altcoin_thread.start()

    while True:
        #daddy
        if float(r.get('daddy_enabled').decode()) != 1:
            daddy_thread.terminate()

        if daddy_thread.is_alive() == False:
            if float(r.get('daddy_enabled').decode()) == 1:
                daddy_thread = multiprocessing.Process(target=daddy_bot, args=())
                daddy_thread.start()
        
        #vol
        if float(r.get('vol_trend_enabled').decode()) != 1:
            vol_thread.terminate()

        if vol_thread.is_alive() == False:
            if float(r.get('vol_trend_enabled').decode()) == 1:
                vol_thread = multiprocessing.Process(target=vol_bot, args=())
                vol_thread.start()

        #altcoin
        if float(r.get('altcoin_enabled').decode()) != 1:
            altcoin_thread.terminate()

        if vol_thread.is_alive() == False:
            if float(r.get('altcoin_enabled').decode()) == 1:
                altcoin_thread = multiprocessing.Process(target=alt_bot, args=())
                altcoin_thread.start()

        time.sleep(1)  

bot_thread = threading.Thread(target=bot)
bot_thread.start()

PAIRS = pd.read_csv('pairs.csv')

for idx, row in PAIRS.iterrows():
    exchange = row['cryptofeed_name']
    b = getattr(exchanges, exchange)

    if "MOVE" in row['cryptofeed_symbol']:
        pairs = json.loads(requests.get('https://ftx.com/api/markets').text)['result']
        pairs = [pair['name'] for pair in pairs if re.search("MOVE-20[0-9][0-9]Q", pair['name'])]
    else:
        pairs = [row['cryptofeed_symbol']]

    trade_callbacks = []
    obook_callbacks = []

    for callback in row['feed'].split(","):
        if callback == 'daddy':
            trade_callbacks.append(daddy_trade)
            obook_callbacks.append(daddy_book)
        elif callback == 'vol_trend':
            trade_callbacks.append(vol_trend_trade)
            obook_callbacks.append(vol_trend_book)
        elif callback == 'altcoin':
            trade_callbacks.append(altcoin_trade)
            obook_callbacks.append(altcoin_book)

    f.add_feed(b(pairs=pairs, channels=[TRADES, L2_BOOK], callbacks={TRADES: trade_callbacks, L2_BOOK: obook_callbacks}), timeout=-1)


f.run()