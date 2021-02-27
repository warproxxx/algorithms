import pandas as pd
import json

import requests

import redis
from arctic import Arctic, TICK_STORE

import time
import datetime
from arctic.date import DateRange

import threading

from algos.chadlor.live_trader import liveTrading

store = Arctic('localhost')

if store.library_exists('chadlor') == False:
    store.initialize_library('chadlor', lib_type=TICK_STORE)

library = store['chadlor']
library._chunk_size = 500

r = redis.Redis(host='localhost', port=6379, db=0)

try:
    r.get('chadlor_position_since').decode()
except:
    r.set('chadlor_position_since', 0)

def get_coinbase_api():
    cbase = pd.DataFrame(json.loads(requests.get("https://api.pro.coinbase.com/products/BTC-USD/candles?granularity=60").text))
    cbase.columns = ['timestamp', 'low', 'high', 'open', 'close', 'volume']
    cbase['timestamp'] = pd.to_datetime(cbase['timestamp'], unit='s')
    return cbase

def get_bitmex_api():
    bmex = pd.DataFrame(json.loads(requests.get("https://www.bitmex.com/api/v1/trade/bucketed?binSize=1m&symbol=XBTUSD&count=500&reverse=true").text))
    bmex['timestamp'] = pd.to_datetime(bmex['timestamp'])
    bmex['timestamp'] = bmex['timestamp'].dt.tz_localize(None)
    bmex = bmex[['timestamp', 'low', 'high', 'open', 'close', 'volume']]
    return bmex

def get_prices(endTime):
    endTime = pd.to_datetime(endTime)
    endTime = endTime.replace(second=0, microsecond=0)
    of_timestamp = endTime - pd.Timedelta(minutes=1)

    endTime = endTime.tz_localize(tz='UTC')
    bitmex_close = library.read('BITMEX', date_range = DateRange(end=endTime)).reset_index().iloc[-1]
    coinbase_close = library.read('COINBASE', date_range = DateRange(end=endTime)).reset_index().iloc[-1]


    bitmex_price = bitmex_close['price']
    coinbase_price = coinbase_close['price']

    bitmex_at = pd.to_datetime(bitmex_close['index']).tz_localize(None)
    coinbase_at = pd.to_datetime(coinbase_close['index']).tz_localize(None)

    try:
        coinbase = get_coinbase_api()
        coinbase_api = coinbase.iloc[0]
        print("Current Time: {}\nExchange: {}\nREST Time: {}\nRest Price: {}\nWSS Time: {}\nWSS Price: {}".format(datetime.datetime.utcnow(), "COINBASE", coinbase_api['timestamp'], coinbase_api['close'], coinbase_at, coinbase_price))

        if coinbase_api['timestamp'] == of_timestamp:
            print("Using REST price for coinbase")
            coinbase_price = coinbase_api['close']
            coinbase_at = of_timestamp

    except Exception as e:
        print("Error in coinbase API: {}".format(str(e)))
    
    try:
        bitmex = get_bitmex_api()
        bitmex_api = bitmex.iloc[0]
        print("Current Time: {}\nExchange: {}\nREST Time: {}\nRest Price: {}\nWSS Time: {}\nWSS Price: {}".format(datetime.datetime.utcnow(), "BITMEX", bitmex_api['timestamp'], bitmex_api['close'], bitmex_at, bitmex_price))

        if bitmex_api['timestamp'] == of_timestamp:
            print("Using REST price for bitmex")
            coinbase_price = bitmex_api['close']
            bitmex_at = of_timestamp

    except Exception as e:
        print("Error in bitmex API: {}".format(str(e)))

    #if either is too old, use merged API.
    if (bitmex_at < of_timestamp) or (coinbase_at < of_timestamp):
        try:
            df = coinbase.merge(bitmex, on='timestamp', suffixes=('_coinbase', '_bitmex'))
            df = df[['timestamp', 'close_coinbase', 'close_bitmex']]
            data = df.iloc[0]
            coinbase_price = data['close_coinbase']
            bitmex_price = data['close_bitmex']
        except Exception as e: #if there is no data too, return same for both
            print("Couldn't get merged either. Using same for both Error: {}".format(str(e)))
            coinbase_price = bitmex_price

    return coinbase_price, bitmex_price

def process_thread(endTime):
    time.sleep(1)
    coinbase_price, bitmex_price = get_prices(endTime)
    parameters = json.load(open('algos/chadlor/parameters.json'))
    lt = liveTrading()
    current_pos, avgEntryPrice, pos_size = lt.get_position()
    position_since = int(r.get('chadlor_position_since').decode())

    if current_pos == "NONE":
        if coinbase_price > (parameters['bigger_than'] * bitmex_price):
            lt.fill_order('buy')
    else:
        if position_since > parameters['position_since']:
            position_since = int(r.get('chadlor_position_since').decode())
            pnl_percentage = ((bitmex_price-avgEntryPrice)/avgEntryPrice) * 100 * parameters['mult']

            if pnl_percentage < parameters['loss_cap']:
                lt.fill_order('sell')

        if coinbase_price > (parameters['bigger_than'] * bitmex_price):
            r.set('chadlor_position_since', 0)

        if position_since >= parameters['position_since']:
            lt.fill_order('sell')
    
    if current_pos == "NONE":
        r.set('chadlor_position_since', 0)
    else:
        position_since = position_since + 1
        r.set('chadlor_position_since', position_since)

async def chadlor_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price, order_type=None):
    start_time = time.time()
    ser = {}
    
    ser['price'] = float(price)

    if feed == "BITMEX":
        splitted = str(timestamp).split(".")
        timestamp = float(splitted[0]) + float(splitted[1])/1000

    ser['timestamp'] = pd.to_datetime(timestamp, unit='s')

    df = pd.DataFrame()
    df = df.append(ser, ignore_index=True)
    df = df.set_index('timestamp')
    df = df.tz_localize(tz='UTC')    

    library.write(feed, df)  

def delete_thread():
    startTime = pd.to_datetime(datetime.datetime.utcnow()) - pd.Timedelta(days=10)
    startTime = startTime.tz_localize(tz='UTC')

    endTime = pd.to_datetime(datetime.datetime.utcnow()) - pd.Timedelta(minutes=60)
    endTime = endTime.tz_localize(tz='UTC')

    library.delete('BITMEX', date_range = DateRange(start=startTime, end=endTime))
    library.delete('COINBASE', date_range = DateRange(start=startTime, end=endTime))
    
def chadlor_bot():
    old_min = datetime.datetime.utcnow().minute

    while datetime.datetime.utcnow().minute == old_min:
        time.sleep(.1)

    starttime = time.time()

    while True:
        t = threading.Thread(target=process_thread, args=(datetime.datetime.utcnow(), ))
        t.start()

        time.sleep(60.0 - ((time.time() - starttime) % 60.0))

        del_t = threading.Thread(target=delete_thread, )
        del_t.start()
        