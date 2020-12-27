import os
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
from algos.daddy.historic import single_price_from_rest
from algos.daddy.plot import create_chart
from utils import print

TESTNET = False
EXCHANGES = pd.read_csv('exchanges.csv')
r = redis.Redis(host='localhost', port=6379, db=0)
lts = {}

for idx, details in EXCHANGES.iterrows():
    exchange_name = details['exchange']

    if details['trade'] == 1 or exchange_name == 'bitmex':       
        lts[exchange_name] = liveTrading(exchange_name, symbol=details['ccxt_symbol'],testnet=TESTNET) 
        lts[exchange_name].set_position()

def get_interferance_vars():
    try:
        buy_missed = float(r.get('buy_missed').decode())
    except:
        buy_missed = 0

    try:
        buy_at = float(r.get('buy_at').decode())
    except:
        buy_at = 0
    
    try:
        close_and_stop = float(r.get('close_and_stop').decode())
    except:
        close_and_stop = 0

    try:
        stop_trading = float(r.get('stop_trading').decode())
    except:
        stop_trading = 0

    return buy_missed, buy_at, close_and_stop, stop_trading

def ohlcv_from_trade(curr_df):
    ser = {}
    ser['Open'] = curr_df.iloc[0]['price']
    ser['High'] = curr_df['price'].max()
    ser['Low'] = curr_df['price'].min()
    ser['Close'] = curr_df.iloc[-1]['price']
    ser['Volume'] = curr_df['foreignNotional'].sum()
    return pd.Series(ser)

def merge_prices(curr_df):
    ser = {}
    try:
        ser['Open'] = curr_df.iloc[0]['Open']
        ser['High'] = curr_df['High'].max()
        ser['Low'] = curr_df['Low'].min()
        ser['Close'] = curr_df.iloc[-1]['Close']
        ser['Volume'] = curr_df['Volume'].sum()
    except:
        ser['Open'] = np.nan
        ser['High'] = np.nan
        ser['Low'] = np.nan
        ser['Close'] = np.nan
        ser['Volume'] = np.nan
    
    return pd.Series(ser)

def custom_buy():
    print("Making a custom buy")
    EXCHANGES = pd.read_csv('exchanges.csv')
    
    for idx, details in EXCHANGES.iterrows():
        if details['trade'] == 1:
            current_pos = r.get('{}_current_pos'.format(exchange_name)).decode()

            if current_pos == "NONE":
                curr_exchange = EXCHANGES[EXCHANGES['exchange'] == exchange_name].iloc[0]
                lt = lts[details['exchange']]
                lt.fill_order('buy', method=curr_exchange['buy_method'])
                r.set('{}_position_since'.format(exchange_name), 1)
                lt.add_stop_loss()

def custom_sell():
    print("Making a custom sell")
    EXCHANGES = pd.read_csv('exchanges.csv')
    
    for idx, details in EXCHANGES.iterrows():
        if details['trade'] == 1:
            current_pos = r.get('{}_current_pos'.format(exchange_name)).decode()

            if current_pos == "LONG":
                curr_exchange = EXCHANGES[EXCHANGES['exchange'] == exchange_name].iloc[0]
                lt = lts[details['exchange']]
                lt.close_stop_order()
                lt.fill_order('sell', method=curr_exchange['sell_method'])
                r.set('{}_position_since'.format(exchange_name), 0)

def perform_trade(exchange_name, lt, parameters, macd, rsi, changes, percentage_large, buy_percentage_large, manual_call):
    position_since = 0
    current_pos = r.get('{}_current_pos'.format(exchange_name)).decode()
    avgEntryPrice = float(r.get('bitmex_avgEntryPrice').decode())

    pnl_percentage = 0
    curr_exchange = EXCHANGES[EXCHANGES['exchange'] == exchange_name].iloc[0]

    buy_missed, buy_at, close_and_stop, stop_trading = get_interferance_vars()

    if stop_trading == 0:
        if current_pos == "LONG":
            position_since = float(r.get('{}_position_since'.format(exchange_name)).decode())
            position_since = position_since + 1
            r.set('{}_position_since'.format(exchange_name), position_since)


            if manual_call == False:
                btc_price = float(r.get('bitmex_best_ask').decode())
            else:
                if float(r.get('got_this_turn').decode()) == 0:

                    prices = {}
                    try:
                        prices['coinbase'] = float(json.loads(requests.get('https://api.coinbase.com/v2/prices/spot?currency=USD').text)['data']['amount'])
                    except:
                        pass

                    try:
                        prices['bitstamp'] = float(json.loads(requests.get('https://www.bitstamp.net/api/transactions').text)[0]['price'])
                    except:
                        pass

                    try:                
                        prices['kraken'] = float(json.loads(requests.get('https://api.kraken.com/0/public/Trades?pair=XBTUSD').text)['result']['XXBTZUSD'][-1][0])
                    except:
                        pass
                    
                    try:
                        btc_price = sum(prices.values())/len(prices)
                        r.set('exchanges_price', btc_price)
                        r.set('got_this_turn', 1)
                        print("Manually got BTC prices using {} at {}".format(prices, btc_price))
                    except:
                        btc_price = float(r.get('bitmex_best_ask').decode())
                else:
                    btc_price = float(r.get('exchanges_price').decode())
                    print("Getting {} from redis".format(btc_price))

            pnl_percentage = ((btc_price - avgEntryPrice)/avgEntryPrice) * 100 * parameters['mult']

            if position_since > parameters['position_since']:
                if pnl_percentage > parameters['pnl_percentage']:
                    if (macd < parameters['profit_macd'])  or (rsi > parameters['rsi']):
                        lt.close_stop_order()
                        lt.fill_order('sell', method=curr_exchange['sell_method'])
                        r.set('{}_position_since'.format(exchange_name), 0)
                else:
                    if (pnl_percentage < parameters['close_percentage']) or (macd < parameters['macd'])  or (rsi > parameters['rsi']):
                        lt.close_stop_order()
                        lt.fill_order('sell', method=curr_exchange['sell_method'])
                        r.set('{}_position_since'.format(exchange_name), 0)

        elif current_pos == "NONE":
            if (sum(changes < parameters['change']) >= (parameters['previous_days'] - parameters['position_since_diff'])) and (macd > parameters['macd']) and (rsi < parameters['rsi']):
                if ((percentage_large > parameters['percentage_large']) and (buy_percentage_large > parameters['buy_percentage_large'])):
                    lt.fill_order('buy', method=curr_exchange['buy_method'])
                    r.set('{}_position_since'.format(exchange_name), 1)
                    lt.add_stop_loss()

    position_since = float(r.get('{}_position_since'.format(exchange_name)).decode())
    avgEntryPrice = float(r.get('{}_avgEntryPrice'.format(exchange_name)).decode())
    print("\nExchange      : {}\nAvg Entry     : {}\nPnL Percentage: {}%\nPosition Since: {}".format(exchange_name, avgEntryPrice, round(pnl_percentage,2), position_since))

def after_stuffs(exchange_name):
    global lts
    lt = lts[exchange_name]
    lt.set_position()
    lt.update_parameters()

    current_pos = r.get('{}_current_pos'.format(exchange_name)).decode()

    if current_pos == 'NONE':
        lt.close_stop_order()
    else:
        lt.update_stop()

def trade_caller(parameters, macd, rsi, changes, percentage_large, buy_percentage_large, manual_call=False):
    global lts
    global EXCHANGES
    EXCHANGES = pd.read_csv('exchanges.csv') #update exchanges

    print("Time: {} percentage_large: {} buy_percentage_large: {} rsi: {} macd: {} changes: {} manual_call: {}".format(datetime.datetime.utcnow(), round(percentage_large,3), round(buy_percentage_large,3), round(rsi,2), round(macd,2), changes, manual_call))

    threads = {}
    for idx, details in EXCHANGES.iterrows():
        if details['trade'] == 1:
            threads[details['exchange']] = threading.Thread(target=perform_trade, args=(details['exchange'], lts[details['exchange']], parameters, macd, rsi, changes, percentage_large, buy_percentage_large, manual_call, ))
            threads[details['exchange']].start()

    #wait till completion
    for idx, details in EXCHANGES.iterrows():
        if details['trade'] == 1:
            threads[details['exchange']].join()
            after_stuffs(details['exchange'])

    #add if new exchange added
    for idx, details in EXCHANGES.iterrows():
        exchange_name = details['exchange']

        if details['trade'] == 1 or exchange_name == 'bitmex':       

            if not exchange_name in lts:
                lts[exchange_name] = liveTrading(exchange_name, symbol=details['ccxt_symbol'],testnet=TESTNET) 
                lts[exchange_name].set_position()
    

def spaced_print(str, target_length=15):
    str_len =len(str)
    spaces = target_length - str_len

    if spaces > 0:
        str = str + " " * spaces

    return str
    

def single_process(manual_call=False):
    files = glob("data/stream/*")

    if len(files) > 1:
        print("There are multilple files")
        print(files)
        files.sort()

        for file in files[:-1]:
            print("Removing {}".format(file))
            os.remove(file)

        files = glob("data/stream/*")
    
    price_file = "data/ohlcv/bitmex/BTCUSD.csv"

    if len(files) == 1:
        r.set('got_this_turn', 0)
        file = files[0]
        df = pd.read_csv(file)
        df['Time'] = pd.to_datetime(df['Time'])

        df = df.groupby(['Time', 'side']).sum()
        df['price'] = df['foreignNotional']/df['homeNotional'] #as sum would mess price
        df = df.reset_index()

        df = df[df['foreignNotional'] > 500]

        df['homeNotional'] = pd.to_numeric(df['homeNotional'], errors='coerce').fillna(0)
        df['foreignNotional'] = pd.to_numeric(df['foreignNotional'], errors='coerce').fillna(0)

        price_df = df.groupby(pd.Grouper(key='Time', freq="5Min", label='left')).apply(ohlcv_from_trade)
        price_df = price_df.reset_index()
        price_df['Time'] = pd.to_datetime(price_df['Time'])

        price_df.to_csv(price_file, index=None, mode='a', header=None)

        price_df = pd.read_csv(price_file)
        price_df['Time'] = pd.to_datetime(price_df['Time'])
        price_df = price_df[-1000:]
        price_df.to_csv(price_file, index=None)

        price_df = price_df.groupby(pd.Grouper(key='Time', freq="10Min", label='left')).apply(merge_prices)
        price_df = price_df.fillna(method='ffill').fillna(method='bfill')

        price_df['macd'] = ta.trend.macd_signal(price_df['Close'])
        price_df['rsi'] = ta.momentum.rsi(price_df['Close'])

        price_df['change'] = ((price_df['Close'] - price_df['Open'])/price_df['Open']) * 100

        buy_orders = df[df['side'] == 'buy']
        sell_orders = df[df['side'] == 'sell']

        total_buy = buy_orders['homeNotional'].sum()
        total_sell = sell_orders['homeNotional'].sum()
        total = total_buy + total_sell

        readable_bins = [0, 2, 10, np.inf]
        readable_labels = ['small', 'medium', 'large']
        df['new_range'] = pd.cut(df['homeNotional'], readable_bins, include_lowest=True, labels=readable_labels).astype(str)
        total = total_buy + total_sell

        parameters = json.load(open('algos/daddy/parameters.json'))

        changes =  []
        
        for i in range(0, -1 * int(parameters['previous_days']), -1):
            prev_change = round(price_df.iloc[i-1]['change'], 3)
            changes.append(prev_change)
            
        changes = np.array(changes)

        group = df[df['new_range'] == 'large']
        buy_orders = group[group['side'] == 'buy']
        percentage_large = group['homeNotional'].sum()/total
        buy_percentage_large = (buy_orders['homeNotional'].sum())/group['homeNotional'].sum()
        rsi = price_df.iloc[-1]['rsi']
        macd = price_df.iloc[-1]['macd']

        os.remove(file)

        trade_caller(parameters, macd, rsi, changes, percentage_large, buy_percentage_large, manual_call=manual_call)


    else:
        print("There is no trade file on {}. Still performing".format(datetime.datetime.utcnow()))

        if exchange_name == 'bitmex':
            parameters = json.load(open('algos/daddy/parameters.json'))
            trade_caller(parameters, 0, 0, np.array([0] * parameters['previous_days']), 0, 0, manual_call=manual_call)

    if os.path.isfile(price_file):
        price_df = pd.read_csv(price_file)
        price_df['Time'] = pd.to_datetime(price_df['Time'])
        price_df = price_df.drop_duplicates(subset=['Time'])
        price_df = price_df.sort_values('Time')
        price_df.to_csv(price_file, index=None)
    else:
        print("There is no price file. Something is very wrong. This should never happen.")

def process_thread(save_file):
    r.set(save_file, 1)
    print("\n\033[1m" + str(datetime.datetime.utcnow()) + "\033[0;0m:")

    single_process()  

def check_calling():
    while True:
        time.sleep(1)

        if float(r.get('first_execution').decode()) == 0:
            curr_time = datetime.datetime.utcnow()

            df = pd.DataFrame(pd.Series({'Time': curr_time})).T
            save_file = str(df.groupby(pd.Grouper(key='Time', freq="10Min", label='left')).sum().index[0])

            timestamp = pd.to_datetime(curr_time)
            current_full_time = str(timestamp.minute)
            current_time_check = current_full_time[1:]

            if current_full_time == '8' or current_time_check == '8':
                if (timestamp.second > 5) and (r.get(save_file) == None):
                    r.set(save_file, 1)
                    print("\n\033[1m" + str(datetime.datetime.utcnow()) + "(Manual Call) \033[0;0m:")
                    single_process(manual_call=True) 

async def daddy_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):

    if float(r.get('daddy_enabled').decode()) == 1:
        if feed == 'BITMEX':
            timestamp = pd.to_datetime(timestamp, unit='s')
            current_full_time = str(timestamp.minute)
            current_time_check = current_full_time[1:]

            if float(r.get('first_execution').decode()) == 0:
                if feed == 'BITMEX':
                    foreignNotional = amount
                    homeNotional = round(amount/price, 5)
                elif feed == "BINANCE_FUTURES":
                    homeNotional = amount
                    foreignNotional = round(amount * price, 5)

                df = pd.DataFrame(pd.Series({'Time': timestamp, 'side': side, 'homeNotional': homeNotional, 'foreignNotional': foreignNotional, 'price': price})).T
                save_file = str(df.groupby(pd.Grouper(key='Time', freq="10Min", label='left')).sum().index[0])

                buy_missed, buy_at, close_and_stop, stop_trading = get_interferance_vars()

                if buy_missed == 1:
                    if price < buy_at:
                        r.set('buy_missed', 0)
                        custom_buy_thread = threading.Thread(target=custom_buy)
                        custom_buy_thread.start()

                if close_and_stop == 1:
                    r.set('close_and_stop', 0)
                    r.set('stop_trading', 1)
                    custom_sell_thread = threading.Thread(target=custom_sell)
                    custom_sell_thread.start()   

                if current_full_time == '8' or current_time_check == '8' or current_full_time == '9' or current_time_check == '9':
                    if float(r.get('first_nine').decode()) == 1:

                        if r.get(save_file) == None:
                            r.set('first_nine', 0)
                            t = threading.Thread(target=process_thread, args=(save_file,))
                            t.start()          
                        else: #because sometimes binance API has delays in receiving data
                            var_name = "{}_printed".format(save_file)
                            
                            if r.get(var_name) == None:
                                r.set(var_name, 1)
                                print("Prevented a multiple run. Potential delays taking place")                 
                else:
                    r.set('first_nine', 1)
                    folder = "data/stream"
                    if not os.path.isdir(folder):
                        os.makedirs(folder)

                    trades_file = "{}/significant_trades_{}.csv".format(folder, save_file)

                    if os.path.isfile(trades_file):
                        df.to_csv(trades_file, mode='a', header=None, index=None)
                    else:
                        df.to_csv(trades_file, index=None)

            else:
                print("Current Time check is {}/{}".format(current_time_check, current_full_time))

                if current_full_time == '0' or current_time_check == '0':             
                    r.set('first_execution', 0)    
                    delayed_thread = threading.Thread(target=delayed_price_from_rest)
                    delayed_thread.start()

async def daddy_book(feed, pair, book, timestamp, receipt_timestamp):
    if float(r.get('daddy_enabled').decode()) == 1:
        bid = float(list(book[BID].keys())[-1])
        ask = float(list(book[ASK].keys())[0])

        r.set('{}_best_bid'.format(feed.lower()), bid)
        r.set('{}_best_ask'.format(feed.lower()), ask)

def delayed_price_from_rest():
    time.sleep(60) #because API gives data ~15 seconds late.
    update_price_from_rest() 

def update_price_from_rest():
    single_price_from_rest('bitmex', 'BTC/USD')

def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)

def daddy_bot():
    if os.path.isdir("data/stream"):
        shutil.rmtree('data/stream')

    update_price_from_rest()
    schedule.every().day.at("00:03").do(create_chart)

    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()

    calling_check_thread = threading.Thread(target=check_calling)
    calling_check_thread.start()