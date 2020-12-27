from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK

import pandas as pd
import json

import time
import datetime

import threading
import schedule
import redis

from algos.vol_trend.backtest import perform_backtests
from algos.vol_trend.live_trader import liveTrading
from utils import print


r = redis.Redis(host='localhost', port=6379, db=0)

async def vol_trend_book(feed, pair, book, timestamp, receipt_timestamp):
    bid = float(list(book[BID].keys())[-1])
    ask = float(list(book[ASK].keys())[0])

    r.set('FTX_{}_best_bid'.format(pair), bid)
    r.set('FTX_{}_best_ask'.format(pair), ask)

async def vol_trend_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):
    buy_missed_perp = 0
    buy_missed_move = 0
    enable_per_close_and_stop = 0
    enable_move_close_and_stop = 0

    try:
        buy_missed_perp = float(r.get('buy_missed_perp').decode())
        perp_long_or_short = float(r.get('perp_long_or_short').decode())
        price_perp = float(r.get('price_perp').decode())
    except:
        pass

    try:
        buy_missed_move = float(r.get('buy_missed_move').decode())
        move_long_or_short = float(r.get('move_long_or_short').decode())
        price_move = float(r.get('price_move').decode())
    except:
        pass

    try:
        enable_per_close_and_stop = float(r.get('enable_per_close_and_stop').decode())
    except:
        pass

    try:
        enable_move_close_and_stop = float(r.get('enable_move_close_and_stop').decode())
    except:
        pass
    

    if pair == 'BTC-PERP':
        if buy_missed_perp == 1:
            lt = liveTrading(symbol='BTC-PERP')
            pos, _, _ = lt.get_position()

            type = "open" if pos == "NONE" else "close"
            if perp_long_or_short == 1:
                if price < price_perp:
                    r.set('buy_missed_perp', 0)
                    lt.fill_order(type, 'long')
            else:
                if price > price_perp:
                    r.set('buy_missed_perp', 0)
                    lt.fill_order(type, 'short')
        
        if enable_per_close_and_stop == 1:
            r.set('enable_per_close_and_stop', 0)
            r.set('stop_perp', 1)
            lt = liveTrading(symbol='BTC-PERP')
            pos, _, _ = lt.get_position()
            lt.fill_order('close', pos.lower())

            
    elif 'MOVE' in pair:
        if buy_missed_move == 1:
            
            pair = get_curr_move_pair()
            lt = liveTrading(symbol=pair)
            pos, _, _ = lt.get_position()

            type = "open" if pos == "NONE" else "close"

            if move_long_or_short == 1:
                if price < price_move:
                    r.set('buy_missed_move', 0)
                    lt.fill_order(type, 'long')
            else:
                if price > price_move:
                    r.set('buy_missed_move', 0)
                    lt.fill_order(type, 'short')

        if enable_move_close_and_stop == 1:
            r.set('enable_move_close_and_stop', 0)
            r.set('stop_move', 1)
            pair = get_curr_move_pair()
            lt = liveTrading(symbol=pair)
            pos, _, _ = lt.get_position()
            lt.fill_order('close', pos.lower())

def get_curr_move_pair():
    details_df, balances = get_position_balance()
    moves = details_df[details_df['name'].str.contains('MOVE')]
    return moves[moves['backtest_position'] != "NONE"].iloc[0]['name']

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
            curr_detail['entry'] = round(float(r.get('FTX_{}_avgEntryPrice'.format(asset)).decode()),2)
        except:
            curr_detail['entry'] = 0
        
        try:
            curr_detail['pos_size'] = float(r.get('FTX_{}_pos_size'.format(asset)).decode())
        except:
            curr_detail['pos_size'] = 0

        try:
            curr_detail['live_price'] = float(r.get('FTX_{}_best_ask'.format(asset)).decode())
        except:
            curr_detail['live_price'] = 0

        



        if asset == "BTC-PERP":
            try:
                curr_detail['live_lev']= float(r.get('PERP_mult').decode())
            except:
                curr_detail['live_lev'] = 4

            curr_detail['backtest_position'] = 'SHORT' if perp_backtest.iloc[-1]['Type'] == 'SELL' else 'LONG'
            curr_detail['backtest_date'] = perp_backtest.iloc[-1]['Date']
            curr_detail['entry_price'] = round(perp_backtest.iloc[-1]['Price'], 2)

            
        else:
            try:
                curr_detail['live_lev'] = float(r.get('MOVE_mult').decode())
            except:
                curr_detail['live_lev']= 2

            curr_df = move_backtest[move_backtest['Data'] == asset]

            curr_detail['backtest_position'] = 'NONE'
            curr_detail['backtest_date'] = ""

            if len(curr_df) > 0:
                if abs(curr_df['Size'].sum()) > 0.01:
                    curr_detail['backtest_position'] = 'SHORT' if curr_df.iloc[-1]['Type'] == 'SELL' else 'LONG'
                    curr_detail['backtest_date'] = curr_df.iloc[-1]['Date']
                    curr_detail['entry_price'] = round(curr_df.iloc[-1]['Price'], 2)

        try:
            curr_detail['live_pnl'] = round(((curr_detail['live_price'] - curr_detail['entry'])/curr_detail['entry']) * 100 * curr_detail['live_lev'], 2)
        except:
            curr_detail['live_pnl'] = 0

        details_df = details_df.append(curr_detail, ignore_index=True)


    balances = {}
    try:
        balances['MOVE_BALANCE'] = round(float(r.get('FTX_MOVE_balance').decode()), 2)
    except:
        balances['MOVE_BALANCE'] = 0

    try:
        balances['PERP_BALANCE'] = round(float(r.get('FTX_PERP_balance').decode()), 2)
    except:
        balances['PERP_BALANCE'] = 0

    return details_df, balances

def get_overriden(details_df):
    replace_perp = ""
    replace_move = ""

    try:
        if float(r.get('override_perp').decode()) == 1:
            perp = float(r.get('perp_override_direction').decode())

        if perp == 1:
            replace_perp = 'LONG'
        elif perp == -1:
            replace_perp = 'SHORT'
        else:
            replace_perp = 'NONE'
    except:
        pass

    try:
        if float(r.get('override_move').decode()) == 1:
            move = float(r.get('move_override_direction').decode())

        if move == 1:
            replace_move = 'LONG'
        elif move == -1:
            replace_move = 'SHORT'
        else:
            replace_move = 'NONE'
    except:
        pass

    new_details = pd.DataFrame()
    for idx, row in details_df.iterrows():
        if row['name'] == 'BTC-PERP':
            if replace_perp != "":
                row['backtest_position'] = replace_perp
        else:
            if replace_move != "":
                if row['backtest_position'] != "NONE":
                    row['backtest_position'] = replace_move
                    
        new_details = new_details.append(row, ignore_index=True)

    return new_details

def daily_tasks():
    print("Time: {}".format(datetime.datetime.utcnow()))
    perform_backtests()
    print("\n")

    pairs = json.load(open('algos/vol_trend/pairs.json'))
    pairs.append('BTC-PERP')

    for pair in pairs:
        lt = liveTrading(pair)
        lt.set_position()


    enabled = 1
    stop_perp = 0
    stop_move = 0

    try:
        enabled = float(r.get('vol_trend_enabled').decode())
    except:
        pass

    try:
        stop_perp = float(r.get('stop_perp').decode())
    except:
        pass

    try:
        stop_move = float(r.get('stop_move').decode())
    except:
        pass
    
    if enabled == 1:
        details_df, balances = get_position_balance()

        details_df = get_overriden(details_df)
        
        details_df['target_pos'] = details_df['backtest_position'].replace("LONG", 1).replace("SHORT", -1).replace("NONE", 0)
        details_df['curr_pos'] = details_df['position'].replace("LONG", 1).replace("SHORT", -1).replace("NONE", 0)

        to_close = details_df[details_df['target_pos'] == 0]

        for idx, row in to_close.iterrows():
            if row['name'] == 'BTC-PERP':
                curr_stop = stop_perp
            else:
                curr_stop = stop_move

            if curr_stop == 0:
                if row['curr_pos'] != 0:
                    lt = liveTrading(symbol=row['name'])
                    lt.fill_order('close', row['position'].lower())
        
        to_open = details_df[details_df['target_pos'] != 0]

        for idx, row in to_open.iterrows():
            if row['name'] == 'BTC-PERP':
                curr_stop = stop_perp
            else:
                curr_stop = stop_move

            if curr_stop == 0:
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

        print("\n")

def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)

def hourly_tasks():
    details_df, balances = get_position_balance()
    for idx, row in details_df.iterrows():
        lt = liveTrading(row['name'])
        lt.set_position()

def vol_bot():
    pairs = json.load(open('algos/vol_trend/pairs.json'))
    pairs.append('BTC-PERP')

    for pair in pairs:
        lt = liveTrading(pair)
        lt.set_position()

    schedule.every().day.at("00:01").do(daily_tasks)
    schedule.every().hour.do(hourly_tasks)

    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()