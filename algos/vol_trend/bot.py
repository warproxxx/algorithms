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

def get_position_balance():
    r = redis.Redis(host='localhost', port=6379, db=0)
    move_backtest = pd.read_csv('data/trades_move.csv')
    perp_backtest = pd.read_csv('data/trades_perp.csv')

    pairs = json.load(open('algos/vol_trend/pairs.json'))
    pairs.append('BTC-PERP')

    details_df = pd.DataFrame()

    for asset in pairs:
        curr_detail = pd.Series()
        curr_detail['name'] = asset
        
        try:
            curr_detail['position'] = r.get('FTX_{}_current_pos'.format(asset)).decode()
        except:
            curr_detail['position'] = "NONE"

        try:
            curr_detail['entry'] = float(r.get('FTX_{}_avgEntryPrice'.format(asset)).decode())
        except:
            curr_detail['entry'] = 0
        
        try:
            curr_detail['pos_size'] = float(r.get('FTX_{}_pos_size'.format(asset)).decode())
        except:
            curr_detail['pos_size'] = 0

        if asset == "BTC-PERP":
            curr_detail['backtest_position'] = 'SHORT' if perp_backtest.iloc[-1]['Type'] == 'SELL' else 'LONG'
            curr_detail['backtest_date'] = perp_backtest.iloc[-1]['Date']
            curr_detail['entry_price'] = perp_backtest.iloc[-1]['Price']
        else:
            curr_df = move_backtest[move_backtest['Data'] == asset]

            curr_detail['backtest_position'] = 'NONE'
            curr_detail['backtest_date'] = ""

            if len(curr_df) > 0:
                if abs(curr_df['Size'].sum()) > 0.01:
                    curr_detail['backtest_position'] = 'SHORT' if curr_df.iloc[-1]['Type'] == 'SELL' else 'LONG'
                    curr_detail['backtest_date'] = curr_df.iloc[-1]['Date']
                    curr_detail['entry_price'] = round(curr_df.iloc[-1]['Price'], 2)

        details_df = details_df.append(curr_detail, ignore_index=True)


    balances = {}
    try:
        balances['MOVE_BALANCE'] = float(r.get('FTX_MOVE_balance').decode())
    except:
        balances['MOVE_BALANCE'] = 0

    try:
        balances['PERP_BALANCE'] = float(r.get('FTX_PERP_balance').decode())
    except:
        balances['PERP_BALANCE'] = 0

    return details_df, balances

def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)

def vol_bot():
    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()