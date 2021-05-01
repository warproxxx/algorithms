import os
import sys
from glob import glob
import requests

import time
import datetime

import pandas as pd
import numpy as np
import json

import threading

import ta
import redis
import schedule
import shutil

from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK

from algos.daddy.live_trader import liveTrading
from algos.daddy.trade_analysis import get_trades, process_data, get_details
from algos.daddy.historic import single_price_from_rest
from algos.daddy.plot import create_chart
from algos.daddy.trades import update_trades, run_backtest

from utils import print

TESTNET = False
EXCHANGES = pd.read_csv('algos/daddy/exchanges.csv')
r = redis.Redis(host='localhost', port=6379, db=0)
lts = {}

for idx, details in EXCHANGES.iterrows():
    exchange_name = details['exchange']
    name = details['name']

    if details['trade'] == 1 or exchange_name == 'bitmex':       
        lts[name] = liveTrading(exchange_name, name, symbol=details['ccxt_symbol'],testnet=TESTNET) 
        lts[name].set_position()


def after_stuffs(exchange_name):
    global lts
    lt = lts[exchange_name]
    try:
        lt.set_position()
    except Exception as e:
        print("Error in setting pos in {}".format(exchange_name))
        
    lt.update_parameters()

    current_pos = r.get('{}_current_pos'.format(exchange_name)).decode()

    if current_pos == 'NONE':
        lt.close_stop_order()
    else:
        lt.update_stop()

def perform_backtrade_verification(details, analysis):
    global lts

    try:
        buy_method = r.get('backtest_buy_method').decode()
    except:
        buy_method = '8sec_average'

    try:
        sell_method = r.get('backtest_sell_method').decode()
    except:
        sell_method = '8sec_average'

    try:
        chadlor_position = int(r.get('chadlor_position').decode())
    except:
        chadlor_position = 0

    chadlor_position = 0

    if details['trade'] == 1:
        lt = lts[details['name']]
        current_pos, _, _ = lt.get_position()
        
        if 'open' in analysis['total']:
            if analysis['total']['open'] == 1 and current_pos == "NONE":
                print("Opened position from backtest_verification for {}".format(details['name']))
                r.set('daddy_position', 1)
                lt.fill_order('buy', method=buy_method)
            elif analysis['total']['open'] == 0 and current_pos != "NONE":
                if chadlor_position == 0:
                    print("Closed long position from backtest_verification for {}".format(details['name']))
                    r.set('daddy_position', 0)
                    lt.fill_order('sell', method=sell_method)
                else:
                    print("Daddy isn't in position but chadlor is long so not closing")
            else:
                print("As required for {}".format(details['name']))
        else:
            print("As required for {}".format(details['name']))

        after_stuffs(details['name'])   


def process_trades():
    global lts
    global EXCHANGES

    EXCHANGES = pd.read_csv('algos/daddy/exchanges.csv') #update exchanges
    
    try:
        stop_trading = float(r.get('stop_trading').decode())
    except:
        stop_trading = 0

    if stop_trading == 0:
        analysis, backtest_date = run_backtest()
        save_file_name = r.get('save_file_name').decode()
        backtest_date = pd.to_datetime(backtest_date) - pd.Timedelta(minutes=10)
        save_file_name = pd.to_datetime(save_file_name)

        backtest_thread = {}

        print("Backtest Date: {} Save file name: {}".format(backtest_date, save_file_name))
        if backtest_date == save_file_name:
            for idx, details in EXCHANGES.iterrows():
                backtest_thread[details['name']] = threading.Thread(target=perform_backtrade_verification, args=(details, analysis, ))
                backtest_thread[details['name']].start()

async def daddy_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):
    pass           

async def daddy_book(feed, pair, book, timestamp, receipt_timestamp):
    if float(r.get('daddy_enabled').decode()) == 1:
        bid = float(list(book[BID].keys())[-1])
        ask = float(list(book[ASK].keys())[0])

        r.set('{}_best_bid'.format(feed.lower()), bid)
        r.set('{}_best_ask'.format(feed.lower()), ask)

async def daddy_ticker(feed, pair, bid, ask, timestamp, receipt_timestamp):  
    pass

def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)

def call_every(symbol='XBT'):
    while True:
        time.sleep(1)

        curr_time = datetime.datetime.utcnow()

        df = pd.DataFrame(pd.Series({'Time': curr_time})).T
        save_file = str(df.groupby(pd.Grouper(key='Time', freq="10Min", label='left')).sum().index[0])

        timestamp = pd.to_datetime(curr_time)
        current_full_time = str(timestamp.minute)
        current_time_check = current_full_time[1:]

        if current_full_time == '8' or current_time_check == '8':
            r.set('save_file_name', save_file)
            if (r.get(save_file) == None):
                r.set(save_file, 1)
                print("\n\033[1m" + str(datetime.datetime.utcnow()) + "\033[0;0m:")
                process_trades() 


def daddy_bot():
    symbol = 'XBT'
    schedule.every().day.at("00:30").do(create_chart)

    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()

    trades_update_thread = threading.Thread(target=update_trades, args=(symbol))
    trades_update_thread.start()

    call_every_thread = threading.Thread(target=call_every, args=(symbol))
    call_every_thread.start()