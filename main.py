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
from algos.vol_trend.bot import vol_bot
from algos.altcoin.bot import alt_bot
from algos.ratio.bot import ratio_bot

import ccxt
from algos.daddy.huobi.HuobiDMService import HuobiDM


r = redis.Redis(host='localhost', port=6379, db=0)

def initial_tasks():
    if os.path.isdir("logs/"):
        if os.path.isdir("old_logs/"):
            shutil.rmtree("old_logs/")

        shutil.move("logs/", "old_logs")

    if not os.path.isdir("logs/"):
        os.makedirs("logs/")
        
    flush_redis()

initial_tasks()

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
            elif exchange == 'binance_futures' or exchange == 'binance':
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
                    if row['exchange'] in ['ftx', 'okex', 'bybit', 'binance']:
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

                    if 'ratio' in row['feed']:
                        pair = pair.replace("/", "")
                        r.set('{}_best_bid'.format(pair), bid)
                        r.set('{}_best_ask'.format(pair), ask)
                except Exception as e:
                    print(str(e))

        time.sleep(20)

def bot():
    vol_thread = multiprocessing.Process(target=vol_bot, args=())
    vol_thread.start()

    altcoin_thread = multiprocessing.Process(target=alt_bot, args=())
    altcoin_thread.start()

    ratio_thread = multiprocessing.Process(target=ratio_bot, args=())
    ratio_thread.start()

    obook_thread = multiprocessing.Process(target=obook_process, args=())
    obook_thread.start()

    while True:
        try:            
            #vol
            if float(r.get('vol_trend_enabled').decode()) != 1:
                if vol_thread.is_alive():
                    print("Volatility Bot terminated")
                    vol_thread.terminate()

            if vol_thread.is_alive() == False:
                if float(r.get('vol_trend_enabled').decode()) == 1:
                    print("Vol Bot started")
                    vol_thread = multiprocessing.Process(target=vol_bot, args=())
                    vol_thread.start()

            #altcoin
            if float(r.get('altcoin_enabled').decode()) != 1:
                if altcoin_thread.is_alive():
                    print("Altcoin bot terminated")
                    altcoin_thread.terminate()

            if altcoin_thread.is_alive() == False:
                if float(r.get('altcoin_enabled').decode()) == 1:
                    print("Alt Bot started")
                    altcoin_thread = multiprocessing.Process(target=alt_bot, args=())
                    altcoin_thread.start()

            #ratio
            if float(r.get('ratio_enabled').decode()) != 1:
                if ratio_thread.is_alive():
                    print("Ratio bot terminated")
                    ratio_thread.terminate()

            if ratio_thread.is_alive() == False:
                if float(r.get('ratio_enabled').decode()) == 1:
                    print("Ratio Bot started")
                    ratio_thread = multiprocessing.Process(target=ratio_bot, args=())
                    ratio_thread.start()

            time.sleep(1)  
        except:
            print("error in main")

bot_thread = threading.Thread(target=bot)
bot_thread.start()