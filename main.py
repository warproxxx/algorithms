from cryptofeed.feedhandler import FeedHandler
from cryptofeed import exchanges
from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK, TICKER
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
from algos.daddy.bot import daddy_bot, daddy_trade, daddy_book, daddy_ticker
from algos.vol_trend.bot import vol_bot, vol_trend_trade, vol_trend_book, vol_trend_ticker
from algos.altcoin.bot import alt_bot, altcoin_trade, altcoin_book, altcoin_ticker

import ccxt
from algos.daddy.huobi.HuobiDMService import HuobiDM

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

def obook_process():
    while True:
        pairs_df = pd.read_csv('pairs.csv')
        pairs_df = pairs_df[pairs_df['types'].isnull()]
        exchange_list = set(pairs_df['exchange'].values)

        exchanges = {}

        for exchange in exchange_list:
            if exchange == 'ftx':
                exchanges[exchange] = ccxt.ftx({})
            elif exchange == 'okex':
                exchanges[exchange] = ccxt.okex({})
            elif exchange == 'bybit':
                exchanges[exchange] = ccxt.bybit({})
            elif exchange == 'binance_futures':
                exchanges[exchange] = ccxt.binance({})
            elif exchange == 'huobi_swap':
                exchanges[exchange] = HuobiDM("https://api.hbdm.com", "", "")

        for idx, row in pairs_df.iterrows():
            if "MOVE" in row['ccxt_symbol']:
                pairs = json.load(open('algos/vol_trend/pairs.json'))
            else:
                pairs = [row['ccxt_symbol']]
                
            for pair in pairs:
                try:
                    if row['exchange'] in ['ftx', 'okex', 'bybit']:
                        exchange = exchanges[row['exchange']]    
                        book = exchange.fetch_order_book(pair)
                        bid = book['bids'][0][0]
                        ask = book['asks'][0][0]
                    elif row['exchange'] == 'binance_futures':
                        exchange = exchanges[row['exchange']] 
                        book = exchange.fapiPublicGetDepth({'symbol': 'BTCUSDT', 'limit': 5})
                        bid = book['bids'][0][0]
                        ask = book['asks'][0][0]
                    elif row['exchange'] == 'huobi_swap':
                        book = exchanges['huobi_swap'].send_get_request('/swap-ex/market/depth', {'contract_code': row['ccxt_symbol'], 'type': 'step0'})['tick']
                        bid = book['bids'][0][0]
                        ask = book['asks'][0][0]
                        
                    if 'daddy' in row['feed']:
                        r.set('{}_best_bid'.format(row['exchange']), bid)
                        r.set('{}_best_ask'.format(row['exchange']), ask)
                    
                    if 'vol_trend' in row['feed']:
                        r.set('FTX_{}_best_bid'.format(pair), bid)
                        r.set('FTX_{}_best_ask'.format(pair), ask)
                    
                    if 'altcoin' in row['feed']:
                        r.set('{}_best_bid'.format(pair), bid)
                        r.set('{}_best_ask'.format(pair), ask)
                except:
                    pass

        time.sleep(20)

def bot():
    daddy_thread = multiprocessing.Process(target=daddy_bot, args=())
    daddy_thread.start()

    vol_thread = multiprocessing.Process(target=vol_bot, args=())
    vol_thread.start()

    altcoin_thread = multiprocessing.Process(target=alt_bot, args=())
    altcoin_thread.start()

    obook_thread = multiprocessing.Process(target=obook_process, args=())
    obook_thread.start()

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
PAIRS = PAIRS.fillna("")

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
    ticker_callbacks = []

    if row['types'] != "":
        for callback in row['feed'].split(";"):
            types = row['types']

            if callback == 'daddy':
                if 'stream' in types:
                    trade_callbacks.append(daddy_trade)

                if 'book' in types:
                    obook_callbacks.append(daddy_book)

                if 'ticker' in types:
                    ticker_callbacks.append(daddy_ticker)

            elif callback == 'vol_trend':
                if 'stream' in types:
                    trade_callbacks.append(vol_trend_trade)

                if 'book' in types:
                    obook_callbacks.append(vol_trend_book)

                if 'ticker' in types:
                    ticker_callbacks.append(vol_trend_ticker)
            elif callback == 'altcoin':
                if 'stream' in types:
                    trade_callbacks.append(altcoin_trade)

                if 'book' in types:
                    obook_callbacks.append(altcoin_book)

                if 'ticker' in types:
                    ticker_callbacks.append(altcoin_ticker)

        channels = []
        callbacks = {}

        if len(trade_callbacks) > 0:
            channels.append(TRADES)
            callbacks[TRADES] = trade_callbacks

        if len(obook_callbacks) > 0:
            channels.append(L2_BOOK)
            callbacks[L2_BOOK] = obook_callbacks

        if len(ticker_callbacks) > 0:
            channels.append(TICKER)
            callbacks[TICKER] = ticker_callbacks

        print("{} {} {}".format(pairs, callbacks, channels))
        f.add_feed(b(pairs=pairs, channels=channels, callbacks=callbacks), timeout=-1)

f.run()