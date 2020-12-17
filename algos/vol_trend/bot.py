from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK

import pandas as pd
import json

import time

import threading
import schedule
import redis

from algos.vol_trend.backtest import perform_backtests

r = redis.Redis(host='localhost', port=6379, db=0)

def perform():
    perform_backtests()
    #then perform others

async def vol_trend_book(feed, pair, book, timestamp, receipt_timestamp):
    bid = float(list(book[BID].keys())[-1])
    ask = float(list(book[ASK].keys())[0])

    r.set('FTX_{}_best_bid'.format(pair), bid)
    r.set('FTX_{}_best_ask'.format(pair), ask)


def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)

def vol_bot():
    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()