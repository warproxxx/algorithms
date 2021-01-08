import pandas as pd
import requests
import os
import datetime
import json
import time
import ccxt
import numpy as np
import threading
from algos.daddy.huobi.HuobiDMService import HuobiDM

def get_bitmex_data():
    apiKey = os.getenv('BITMEX_ID')
    apiSecret = os.getenv('BITMEX_SECRET')

    exchange = ccxt.bitmex({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                    })
    limit = 500
    since = exchange.parse8601("2020-12-15 00:00:00")

    all_trades = []

    while True:
        curr_trades = exchange.fetchMyTrades(symbol='BTC/USD', limit=limit, since=since)
        since = exchange.parse8601(curr_trades[-1]['datetime'])
        all_trades = all_trades + curr_trades

        if len(curr_trades) < limit:
            break


    trades = pd.DataFrame([trade['info'] for trade in all_trades])
    trades['transactTime'] = pd.to_datetime(trades['transactTime'])
    trades['transactTime'] = trades['transactTime'].dt.tz_localize(None)
    trades = trades[['transactTime', 'symbol', 'side', 'price','lastQty', 'execComm', 'execType', 'text']]
    funding = trades[trades['text'].str.contains('Funding')]
    trades = trades[trades['execType'] == 'Trade']
    # trades = trades[trades['text'].str.contains('API')]
    trades['exchange'] = 'BITMEX'
    trades['execComm'] = trades['execComm'] * 0.00000001
    trades['commissionAsset'] = 'XBT'
    trades = trades.rename(columns={'execComm': 'commission', 'lastQty': 'qty'})
    trades['side'] = trades['side'].str.upper()
    trades = trades[['transactTime', 'exchange', 'symbol', 'side', 'price', 'qty', 'commission', 'commissionAsset']]
    
    funding['exchange'] = 'BITMEX'
    funding['execComm'] = funding['execComm'] * 0.00000001
    funding['asset'] = 'XBT'
    funding = funding.rename(columns={'lastQty': 'qty'})
    funding = funding[['transactTime', 'exchange', 'symbol', 'qty', 'price', 'asset']]
    return trades, funding

def get_trades(exchange_name):
    if exchange_name == 'bitmex':
        return get_bitmex_data()

def get_ccxt_data(exchange_name, pairname, since, timeframe):
    exchange = getattr(ccxt, exchange_name)({
           'enableRateLimit': True,
    })
    
    all_df = pd.DataFrame()
    old_since = 999999999999
    

    current = pd.DataFrame(exchange.fetch_ohlcv(pairname, timeframe=timeframe, limit=1000, since=since))
    
    while len(current) > 0:
        current = pd.DataFrame(exchange.fetch_ohlcv(pairname, timeframe=timeframe, limit=1000, since=since))
            
        current.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        all_df = pd.concat([all_df, current])
        since = int(all_df.iloc[-1]['Time'])
        
        if old_since == since:
            break
        
        old_since = since
        print(since)
    
    price_file = "data/{}_{}.csv".format(exchange_name, pairname.replace("/", ""))
    
    if os.path.isfile(price_file):
        all_df.to_csv(price_file, index=None, header=None, mode='a')
        all_df = pd.read_csv(price_file)
        all_df = all_df.drop_duplicates(subset=['Time'])
        all_df.to_csv(price_file, index=None)
    else:
        all_df.to_csv(price_file, index=None)

def get_all_price():
    symbols = {
               'bitmex': 'BTC/USD'               }
    threads = {}
    
    for exchange, symbol in symbols.items():        
        if not os.path.isdir("data/"):
            os.makedirs("data")
            
        price_file = "data/{}_{}.csv".format(exchange, symbol.replace("/", ""))
        
        if os.path.isfile(price_file):
            start_time = int(pd.read_csv(price_file).iloc[-1]['Time'])
        else:
            start_time = 1607990400000
            
        timeframe = '5m'
        
        if 'BTC' in symbol:
            timeframe = '1m'
            
                
        threads[exchange] = threading.Thread(target=get_ccxt_data, args=(exchange, symbol, start_time, timeframe))
        threads[exchange].start()
    
    print("Waiting for the threads to complete")

    for exchange, symbol in symbols.items():     
        threads[exchange].join()
    
    
def merge_trades(df):

    exchange_name = df.iloc[0]['exchange']

    ser = {}
    
    filled = df['qty']
    
    if exchange_name == 'BITMEX':
        ser['price'] = sum(filled)/(sum(filled/df['price']))
    elif exchange_name == 'BINANCE':
        ser['price'] = (filled * df['price']).sum()/filled.sum()
    elif exchange_name == 'HUOBI':
        ser['price'] = sum(filled)/(sum(filled/df['price']))

    ser['amount'] = sum(filled)
    ser['fee'] = sum(df['commission_usd'])
    ser['actualExecuted'] = df.iloc[-1]['actualExecuted']
    
    ser['first_time'] = df.iloc[0]['actualExecuted']
    try:
        ser['last_time'] = df.iloc[-2]['actualExecuted']
    except:
        ser['last_time'] = df.iloc[-1]['actualExecuted']
        
    ser['total_trades'] = len(df)

    df = df.drop_duplicates(subset=['actualExecuted'])

    ser['total_non_dupes'] = len(df)

    times = (df['actualExecuted'].shift(-1) - df['actualExecuted']).dropna().astype(int) / 10 ** 9

    if len(times) > 5:
        ser['mean_time_taken_except_last'] = times[:-1].mean()
    else:
        ser['mean_time_taken_except_last'] = np.nan
    
    return pd.Series(ser)

def get_one_after(price_df, trades, exchange_name):
    if exchange_name == 'HUOBI':
        price_df['Time'] = pd.to_datetime(price_df['Time'], unit='s')
    else:
        price_df['Time'] = pd.to_datetime(price_df['Time'], unit='ms')

    trades = trades.merge(price_df[['Time', 'Open']], left_on='transactTime', right_on='Time', how='left')
    trades = trades.rename(columns={'price': 'actualPrice', 'Open': 'expectedPrice'})
    
    price_df['transactTime'] = price_df['Time'] - pd.Timedelta(minutes=1)
    trades = trades.merge(price_df[['transactTime', 'Open']], on='transactTime', how='left')
    trades = trades.rename(columns={'Open': 'one_min_ago'})
    
    price_df['transactTime'] = price_df['Time'] - pd.Timedelta(minutes=2)
    trades = trades.merge(price_df[['transactTime', 'Open']], on='transactTime', how='left')
    trades = trades.rename(columns={'Open': 'two_min_ago'})
    
    price_df['transactTime'] = price_df['Time'] + pd.Timedelta(minutes=1)
    trades = trades.merge(price_df[['transactTime', 'Open']], on='transactTime', how='left')
    trades = trades.rename(columns={'Open': 'one_min_after'})
    return trades
    

def process_data(trades):
    get_all_price()
    trades['price'] = trades['price'].astype(float)
    trades['qty'] = trades['qty'].astype(float)
    trades['commission'] = trades['commission'].astype(float)
    trades = trades.sort_values('transactTime').reset_index(drop=True)
    trades['actualExecuted'] = trades['transactTime']
    trades['transactTime'] = trades['transactTime'].agg(lambda x : x.round('10min'))
    
    exchange_name = trades.iloc[0]['exchange']
    
    if exchange_name == 'BITMEX':
        trades['commission_usd'] = trades['commission'] * trades['price']
    elif exchange_name == 'BINANCE':
        BNB = pd.read_csv("data/binance_BNBUSDT.csv")
        BNB['Time'] = pd.to_datetime(BNB['Time'], unit='ms')
        BNB = BNB.rename(columns={'Close': 'BNB'})
        trades = trades.merge(BNB[['Time', 'BNB']], left_on='transactTime', right_on='Time', how='left').drop('Time', axis=1)
        trades['commission_usd'] = trades['commission']
        bnbs = trades[trades['commissionAsset'] == 'BNB']
        trades.loc[bnbs.index, 'commission_usd'] = bnbs['commission'] * bnbs['BNB']
    elif exchange_name == 'HUOBI':
        trades['commission_usd'] = trades['commission'] * trades['price']
        
    trades = trades.groupby(['transactTime', 'side']).apply(merge_trades)
    trades = trades.reset_index()
    
    if exchange_name == 'BITMEX':
        price_df = pd.read_csv('data/bitmex_BTCUSD.csv')
    elif exchange_name == 'BINANCE':
        price_df = pd.read_csv('data/binance_BTCUSDT.csv')
    elif exchange_name == 'HUOBI':
        price_df = pd.read_csv('data/huobi_BTCUSD.csv')
        
    return get_one_after(price_df, trades, exchange_name)

def rounded_get(var, string):
    return str(round(var, 3)) + " " + string

def calculate_slippage(buys, sells, comp1, comp2):
    buys['slippage_percentage'] = (((buys[comp2] - buys[comp1])/buys[comp1]) * 100)
    sells['slippage_percentage'] = (((sells[comp1] - sells[comp2])/sells[comp2]) * 100)
    return rounded_get((buys['slippage_percentage'].sum() + sells['slippage_percentage'].sum())/(len(buys) + len(sells)), "%")

    
def get_details(trades, funding):
    exchange_name = funding.iloc[0]['exchange']
    buys = trades[trades['side'] == 'BUY']
    sells = trades[trades['side'] == 'SELL']
    
    summary = {}
    summary['Total Trades'] = len(trades)
    summary['Total Fees Paid'] = rounded_get(trades['fee'].sum(), "$")
    
    if exchange_name == 'BINANCE':
        summary['Avg Fee'] = rounded_get((trades['fee']/(trades['amount'] * trades['actualPrice']) * 100).mean(), '%')
        funding['qty'] = funding['qty'].astype(float) * -1
        funding = funding['qty'].sum()
        amt = (trades['amount'] * trades['actualPrice']).sum()
        summary['Total Funding Paid'] = rounded_get(funding, '$')
    elif exchange_name == 'BITMEX':
        summary['Avg Fee'] = rounded_get(((trades['fee']/trades['amount']) * 100).mean(), '%')
        funding = (funding['qty'] * 0.00000001 * funding['price']).sum()
        amt = trades['amount'].sum()
        summary['Total Funding Paid'] = rounded_get(funding, '$')
    elif exchange_name == 'HUOBI':
        trades['fee'] =  trades['fee'] * -1
        funding['qty'] = funding['qty'] * -1
        trades['amount'] = trades['amount'] * 100
        summary['Avg Fee'] = rounded_get(((trades['fee']/trades['amount']) * 100).mean(), '%')
        price_df = pd.read_csv('data/huobi_BTCUSD.csv')
        price_df['Time'] = pd.to_datetime(price_df['Time'], unit='s')
        funding['transactTime'] = funding['transactTime'].agg(lambda x : x.round('1min'))
        funding = funding.merge(price_df[['Time', 'Close']], left_on='transactTime', right_on='Time', how='left')
        funding = (funding['qty'] * funding['Close']).sum()
        amt = trades['amount'].sum()
        summary['Total Funding Paid'] = rounded_get(funding, '$')

    days = (trades['transactTime'].iloc[-1] - trades['transactTime'].iloc[0]).days

    if days == 0:
        days = 1
        
    summary['Avg Funding Interest'] = rounded_get((funding/days)/(amt/days) * 100 * 365, '%')

    summary['Avg Slippage to 00 (Neg Bad)'] = calculate_slippage(buys, sells, 'actualPrice', 'expectedPrice')
    summary['Avg Slippage to 08 (Neg Bad)'] = calculate_slippage(buys, sells, 'actualPrice', 'two_min_ago')
    summary['Avg Slippage to 09 (Neg Bad)'] = calculate_slippage(buys, sells, 'actualPrice', 'one_min_ago')
    summary['Avg Slippage to 11 (Neg Bad)'] = calculate_slippage(buys, sells, 'actualPrice', 'one_min_after')
    
    
    
    cols = trades.columns.tolist()
    cols.insert(5, cols.pop(cols.index('fee')))
    print_trades = trades[['transactTime', 'side', 'actualPrice', 'transactTime', 'fee']]
    
    
    return summary, print_trades, buys, sells
