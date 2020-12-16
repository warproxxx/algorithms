import ccxt
from cryptofeed.rest import Rest

import pandas as pd

import datetime
import os
from utils import print


def fetch(exchange_name, exchange, pairname, timeframe, since):
    if exchange_name == 'bitmex':
        fetch = exchange.fetch_ohlcv(pairname, timeframe=timeframe, limit=1000, since=since)
    elif exchange_name == 'binance_futures':
        fetch = exchange.fapiPublicGetKlines({'symbol': pairname, 'interval': timeframe, 'limit': 1500, 'startTime': since})
    
    return fetch

def get_ohlcv(exchange_name, pairname, since=0, timeframe='5m'):
    exchange_name_here = exchange_name
    
    if exchange_name == 'binance_futures':
        exchange_name_here = 'binance'
    
    
    exchange = getattr(ccxt, exchange_name_here)({
           'enableRateLimit': True,
    })
    
    all_df = pd.DataFrame()
    old_since = 999999999999
    
    fetch_val = fetch(exchange_name, exchange, pairname, timeframe, since)
    current = pd.DataFrame(fetch_val)
    
    while len(current) > 0:
        fetch_val = fetch(exchange_name, exchange, pairname, timeframe, since)
    
        current = pd.DataFrame(fetch_val)
        current = current[[0, 1, 2, 3, 4, 5]]
        current.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        all_df = pd.concat([all_df, current])
        since = int(all_df.iloc[-1]['Time'])
        
        if old_since == since:
            break
        
        old_since = since
        print(since)
    
    all_df = all_df.drop_duplicates(subset=['Time'])
    all_df = all_df.sort_values('Time').reset_index(drop=True)
    all_df['Time'] = pd.to_datetime(all_df['Time'], unit='ms')
    
    
    return all_df

def single_price_from_rest(exchange_name, ticker_name):

    try:
        folder = "data/ohlcv/{}".format(exchange_name)

        if not os.path.isdir(folder):
            os.makedirs(folder)

        price_file = folder + "/{}.csv".format(ticker_name.replace("/", ""))

        if os.path.isfile(price_file):
            try:
                price_df = pd.read_csv(price_file)
                price_df = price_df[-1000:]
                price_df.to_csv(price_file, index=None)
                curr_time = pd.to_datetime(pd.read_csv(price_file).iloc[-1]['Time'])
                curr_time = curr_time + pd.Timedelta(minutes=5)
                start_time = int(curr_time.timestamp()) * 1000
            except:
                try:
                    os.remove(price_file)
                except:
                    pass

                curr_time = datetime.datetime.utcnow() - datetime.timedelta(days=10)
                start_time = int(pd.to_datetime(curr_time).timestamp()) * 1000
        else:
            curr_time = datetime.datetime.utcnow() - datetime.timedelta(days=10)
            start_time = int(pd.to_datetime(curr_time).timestamp()) * 1000
        
        
        curr_df = get_ohlcv(exchange_name.lower(), ticker_name, since=start_time)
        curr_df['Time'] = curr_df['Time'] - pd.Timedelta(minutes=5)

        if os.path.isfile(price_file):
            curr_df[1:].to_csv(price_file, index=None, mode='a', header=None)
        else:
            curr_df.to_csv(price_file, index=None)
    except Exception as e:
        print("Error at getting price from rest. This is expected if the exchange is down or all data have been written already. Error is: {}".format(str(e)))

#too slow need to use my old one with proxies otherwise i get ban.
def old_historic_binance_trade():
    r = Rest(config={'key_id': os.getenv('BINANCE_ID'), 'key_secret': os.getenv('BINANCE_SECRET')})
    trades = []
    file = "data/binance_trades.csv"

    for t in r.binance_futures.trades('BTC-USDT', '2020-07-01', '2020-10-06'):
        df = pd.DataFrame(t)[['timestamp', 'side', 'amount', 'price']]
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        
        if os.path.isfile(file):
            df.to_csv(file, header=None, index=None, mode='a')
        else:
            df.to_csv(file, index=None)

def historic_mex_trade():
    r = Rest(config={'key_id': os.getenv('BITMEX_ID'), 'key_secret': os.getenv('BITMEX_SECRET')})
    trades = []

    for t in r.bitmex.trades('XBTUSD', '2020-01-01', '2020-01-03'):
        trades.extend(t)   

