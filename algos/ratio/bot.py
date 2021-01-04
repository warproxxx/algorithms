from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK

import pandas as pd
import json
import math

import time
import datetime

import threading
import schedule
import redis

from algos.ratio.backtest import perform_backtests
from algos.ratio.live_trader import liveTrading, round_down
from utils import print


r = redis.Redis(host='localhost', port=6379, db=0)

async def ratio_book(feed, pair, book, timestamp, receipt_timestamp):
    if float(r.get('ratio_enabled').decode()) == 1:
        bid = float(list(book[BID].keys())[-1])
        ask = float(list(book[ASK].keys())[0])

        r.set('{}_best_bid'.format(pair), bid)
        r.set('{}_best_ask'.format(pair), ask)

async def ratio_ticker(feed, pair, bid, ask, timestamp, receipt_timestamp):  
    pass

def perform_move_free():
    config = pd.read_csv('algos/ratio/config.csv')

    lt = liveTrading('ETHBTC')
    initial_balance = lt.get_main_balance()

    for idx, row in config.iterrows():
        lt = liveTrading(row['name'])
        amount = round_down(initial_balance * row['allocation'], 6)
        print("Moving {} to {}".format(amount, row['name']))
        lt.transfer_to_subaccount(amount, row['name'])

async def ratio_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):
    pass

def my_round(num, upto=2):
    if not num:
        return num
    current_num = abs(num) * 10
    round_value = 1

    while not (current_num//1):
        current_num *= 10
        round_value +=1

    round_value = round_value + upto
    return round(num, round_value)

def get_positions():
    r = redis.Redis(host='localhost', port=6379, db=0)
    config = pd.read_csv('algos/ratio/config.csv')
    details_df = pd.DataFrame()

    for idx, row in config.iterrows():
        asset = row['name']

        curr_detail = pd.Series()
        curr_detail['name'] = asset
        backtest = pd.read_csv("data/binance/trades_{}.csv".format(asset))

        try:
            curr_detail['position'] = r.get('{}_current_pos'.format(asset)).decode()
        except:
            curr_detail['position'] = "NONE"

        try:
            curr_detail['entry'] = my_round(float(r.get('{}_avgEntryPrice'.format(asset)).decode()))
        except:
            curr_detail['entry'] = 0

        try:
            curr_detail['pos_size'] = float(r.get('{}_pos_size'.format(asset)).decode())
        except:
            curr_detail['pos_size'] = 0

        try:
            curr_detail['live_price'] = my_round(float(r.get('{}_best_ask'.format(asset)).decode()))
        except:
            curr_detail['live_price'] = 0


        curr_detail['backtest_position'] = 'SHORT' if backtest.iloc[-1]['Type'] == 'SELL' else 'LONG'
        curr_detail['backtest_date'] = backtest.iloc[-1]['Date']
        curr_detail['entry_price'] = my_round(backtest.iloc[-1]['Price'])
        curr_detail['to_trade'] = row['to_trade']
        curr_detail['live_lev'] = int(row['mult'])

        try:
            curr_detail['live_pnl'] = round(((curr_detail['live_price'] - curr_detail['entry'])/curr_detail['entry']) * 100 * curr_detail['live_lev'], 2)

            if curr_detail['backtest_position'] == 'SHORT':
                curr_detail['live_pnl'] = curr_detail['live_pnl'] * -1
        except:
            curr_detail['live_pnl'] = 0
            
        try:
            curr_detail['backtest_pnl'] = round(((curr_detail['live_price'] - curr_detail['entry_price'])/curr_detail['entry_price']) * 100 * curr_detail['live_lev'], 2)

            if curr_detail['backtest_position'] == 'SHORT':
                curr_detail['backtest_pnl'] = curr_detail['backtest_pnl'] * -1
        except:
            curr_detail['backtest_pnl'] = 0
            
        curr_detail['allocation'] = row['allocation']

        details_df = details_df.append(curr_detail, ignore_index=True)
    
    return details_df


def daily_tasks():
    print("Time: {}".format(datetime.datetime.utcnow()))
    perform_backtests()
    print("\n")

    config = pd.read_csv('algos/ratio/config.csv')

    for pair in config['name']:
        lt = liveTrading(pair)
        lt.set_position()


    enabled = 1

    try:
        enabled = float(r.get('ratio_enabled').decode())
    except:
        pass

    
    if enabled == 1:
        details_df = get_positions()
        
        details_df['target_pos'] = details_df['backtest_position'].replace("LONG", 1).replace("SHORT", -1).replace("NONE", 0)
        details_df['curr_pos'] = details_df['position'].replace("LONG", 1).replace("SHORT", -1).replace("NONE", 0)

        to_close = details_df[details_df['target_pos'] == 0]

        for idx, row in to_close.iterrows():
            if row['to_trade'] == 1:
                if row['curr_pos'] != 0:
                    lt = liveTrading(symbol=row['name'])
                    lt.fill_order('close', row['position'].lower())
        
        to_open = details_df[details_df['target_pos'] != 0]

        for idx, row in to_open.iterrows():
            if row['to_trade'] == 1:
                try:
                    if row['target_pos'] == row['curr_pos']:
                        print("As required for {}".format(row['name']))
                        pass
                    elif row['target_pos'] * row['curr_pos'] == -1:
                        print("Closing and opening for {}".format(row['name']))
                        lt = liveTrading(symbol=row['name'])
                        lt.fill_order('close', row['position'].lower())
                        lt.fill_order('open', row['backtest_position'].lower())
                    else:
                        print("Opening for {}".format(row['name']))
                        lt = liveTrading(symbol=row['name'])
                        lt.fill_order('open', row['backtest_position'].lower())
                except Exception as e:
                    print(str(e))


        print("\n")

def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)

def hourly_tasks():
    details_df = get_positions()
    for idx, row in details_df.iterrows():
        lt = liveTrading(row['name'])
        lt.set_position()

def perform_close_and_main():
    config = pd.read_csv('algos/ratio/config.csv')

    for idx, row in config.iterrows():
        lt = liveTrading(row['name'])
        pos, _, _ = lt.get_position()

        if pos != "NONE":
            lt.fill_order('close', pos.lower())

        amount = lt.get_balance()

        if amount > 0:
            lt.transfer_to_subaccount(amount, row['name'], source='ISOLATED_MARGIN', destination='SPOT')

def ratio_bot():
    perform_backtests()
    pairs = pd.read_csv('algos/ratio/config.csv')['name']

    for pair in pairs:
        lt = liveTrading(pair)
        lt.set_position()

    schedule.every().day.at("00:01").do(daily_tasks)
    schedule.every().hour.do(hourly_tasks)

    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()

    while True:
        time.sleep(1)
        
        if float(r.get('ratio_enabled').decode()) == 1:
            move_free = 0
            close_and_rebalance = 0
            enter_now = 0
            close_and_main = 0

            try:
                move_free = float(r.get('move_free_ratio').decode())
            except:
                pass

            try:
                close_and_rebalance = float(r.get('close_and_rebalance_ratio').decode())
            except:
                pass

            try:
                close_and_main = float(r.get('close_and_main_ratio').decode())
            except:
                pass

            try:
                enter_now = float(r.get('enter_now_ratio').decode())
            except:
                pass

            if move_free == 1:
                r.set('move_free', 0)
                perform_move_free()

            if close_and_main == 1:
                r.set('close_and_main', 0)
                perform_close_and_main()

            if close_and_rebalance == 1:
                r.set('close_and_rebalance', 0)
                perform_close_and_main()
                perform_move_free()
                daily_tasks()

            if enter_now == 1:
                r.set('enter_now', 0)
                daily_tasks()