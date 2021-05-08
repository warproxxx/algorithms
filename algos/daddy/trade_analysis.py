import pandas as pd
import requests
import os
import datetime
import json
import time
import ccxt
import numpy as np
import threading

def get_ftx_data(row):
    apiKey=os.getenv('FTX_ID')
    apiSecret=os.getenv('FTX_SECRET')

    exchange = ccxt.ftx({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                    })

    exchange.headers = {
                        'FTX-SUBACCOUNT': row['subaccount'],
                    }

    limit = 500
    since = None

    all_trades = []

    while True:
        curr_trades = exchange.fetch_my_trades(symbol=row['ccxt_symbol'], limit=limit, since=since)
        since = exchange.parse8601(curr_trades[-1]['datetime'])
        all_trades = all_trades + curr_trades

        if len(curr_trades) < limit:
            break

    trades = pd.DataFrame([trade['info'] for trade in all_trades])
    trades['time'] = pd.to_datetime(trades['time'])
    trades['time'] = trades['time'].dt.tz_localize(None)
    trades['exchange'] = 'FTX'
    trades['side'] = trades['side'].str.upper()
    trades['commissionAsset'] = 'USD'

    trades = trades.rename(columns={'time': 'transactTime', 'market': 'symbol', 'size': 'qty', 'fee': 'commission'})
    trades = trades[['transactTime', 'exchange', 'symbol', 'side', 'price', 'qty', 'commission', 'commissionAsset']]
    trades['commission_usd'] = trades['commission']

    all_fundings = []
    since = 0

    while True:
        curr_fundings = exchange.private_get_funding_payments(params={'start_time' :since})['result']
        if len(curr_fundings) > 0:
            since = exchange.parse8601(curr_fundings[-1]['time'])/1000
            all_fundings = all_fundings + curr_fundings

            if len(curr_trades) < limit:
                break
        else:
            break

    fundings = pd.DataFrame([funding for funding in all_fundings])
    fundings['time'] = pd.to_datetime(fundings['time'])
    fundings['time'] = fundings['time'].dt.tz_localize(None)
    fundings['exchange'] = "FTX"
    fundings['commissionAsset'] = 'USD'
    fundings = fundings.rename(columns={'time': 'transactTime', 'payment': 'qty', 'fee': 'commission', 'future': 'symbol'})
    fundings = fundings[['transactTime', 'exchange', 'symbol', 'qty']]
    return trades, fundings

def get_bitmex_data(row):
    apiKey=os.getenv('BITMEX_ID')
    apiSecret=os.getenv('BITMEX_SECRET')

    exchange = ccxt.bitmex({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                    })
    limit = 500
    since = None

    all_trades = []

    while True:
        curr_trades = exchange.fetchMyTrades(symbol=row['ccxt_symbol'], limit=limit, since=since)
        since = exchange.parse8601(curr_trades[-1]['datetime'])
        all_trades = all_trades + curr_trades

        if len(curr_trades) < limit:
            break


    trades = pd.DataFrame([trade['info'] for trade in all_trades])
    trades['transactTime'] = pd.to_datetime(trades['transactTime'])
    trades['transactTime'] = trades['transactTime'].dt.tz_localize(None)
    funding = trades[trades['text'].str.contains('Funding')]

    trades = trades[['transactTime', 'symbol', 'side', 'price','lastQty', 'execComm', 'execType', 'text']]
    trades = trades[trades['execType'] == 'Trade']
#     trades = trades[trades['text'].str.contains('API')]
    trades['exchange'] = 'BITMEX'
    trades['execComm'] = trades['execComm'].astype(float)
    trades['execComm'] = trades['execComm'] * 0.00000001
    trades['commissionAsset'] = 'XBT'
    trades = trades.rename(columns={'execComm': 'commission', 'lastQty': 'qty'})
    trades['side'] = trades['side'].str.upper()
    trades = trades[['transactTime', 'exchange', 'symbol', 'side', 'price', 'qty', 'commission', 'commissionAsset']]
    
    funding['exchange'] = 'BITMEX'
    funding['asset'] = 'XBT'
    funding = funding.rename(columns={'lastQty': 'qty'})
    funding['qty'] = funding['commission'].astype(float) * funding['qty'].astype(float)
    funding = funding[['transactTime', 'exchange', 'symbol', 'qty', 'price', 'asset']]
    return trades, funding

def get_trade_funding_data(row):
    exchange_name = row['exchange']

    if exchange_name == 'bitmex':
        return get_bitmex_data(row)
    elif exchange_name == 'ftx':
        return get_ftx_data(row)

def merge_trades(df):

    exchange_name = df.iloc[0]['exchange']

    ser = {}
    
    filled = df['qty']
    
    if exchange_name == 'BITMEX':
        ser['price'] = sum(filled)/(sum(filled/df['price']))
    elif exchange_name == 'BINANCE' or exchange_name == 'FTX':
        ser['price'] = (filled * df['price']).sum()/filled.sum()
    elif exchange_name == 'HUOBI':
        ser['price'] = sum(filled)/(sum(filled/df['price']))

    ser['amount'] = sum(filled)

    try:
        ser['fee'] = sum(df['commission_usd'])
    except:
        ser['fee'] = sum(df['commission'])

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

def calculate_slippage(buys, sells, comp1, comp2):
    buys['slippage_percentage'] = (((buys[comp2] - buys[comp1])/buys[comp1]) * 100)
    sells['slippage_percentage'] = (((sells[comp1] - sells[comp2])/sells[comp2]) * 100)
    return rounded_get((buys['slippage_percentage'].sum() + sells['slippage_percentage'].sum())/(len(buys) + len(sells)), "%")

def process_data(trades, row):
    trades['price'] = trades['price'].astype(float)
    trades['qty'] = trades['qty'].astype(float)
    trades['commission'] = trades['commission'].astype(float)

    try:
        trades['commission_usd'] = trades['commission_usd'].astype(float)
    except:
        pass

    trades = trades.sort_values('transactTime').reset_index(drop=True)
    trades['actualExecuted'] = trades['transactTime']
    trades['transactTime'] = trades['transactTime'].agg(lambda x : x.round('10min'))

    exchange_name = trades.iloc[0]['exchange']
    trades = trades.groupby(['transactTime', 'side']).apply(merge_trades)
    trades = trades.reset_index()

    symbol = row['symbol'].replace("USDT", "").replace("USDC", "").replace("USD", "")
    price_df = pd.read_csv('data/{}_features.csv'.format(symbol))
    price_df['timestamp'] = pd.to_datetime(price_df['timestamp'])
    price_df['expectedPrice'] = price_df['close'].shift(-1)
    price_df = price_df[['timestamp', 'close', 'expectedPrice']]

    trades = trades.merge(price_df, left_on='transactTime', right_on='timestamp', how='left')
    return trades

def rounded_get(var, string):
    return str(round(var, 3)) + " " + string

def get_details(trades, funding):
    exchange_name = funding.iloc[0]['exchange']

    if exchange_name == 'BITMEX':
        trades['fee'] = trades['fee'] * trades['close']

    buys = trades[trades['side'] == 'BUY']
    sells = trades[trades['side'] == 'SELL']
    
    summary = {}
    summary['Total Trades'] = len(trades)
    summary['Total Fees Paid'] = rounded_get(trades['fee'].sum(), "$")

    if exchange_name == 'BINANCE':
        summary['Avg Fee'] = rounded_get((trades['fee']/(trades['amount'] * trades['price']) * 100).mean(), '%')
        funding['qty'] = funding['qty'].astype(float) * -1
        funding = funding['qty'].sum()
        amt = (trades['amount'] * trades['price']).sum()
        summary['Total Funding Paid'] = rounded_get(funding, '$')
    elif exchange_name == 'BITMEX':
        summary['Avg Fee'] = rounded_get(((trades['fee']/trades['amount']) * 100).mean(), '%')
        funding = funding['qty'].sum()
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
    elif exchange_name == 'FTX':
        summary['Avg Fee'] = rounded_get((trades['fee']/(trades['amount'] * trades['price']) * 100).mean(), '%')
        funding['qty'] = funding['qty'].astype(float)
        funding = funding['qty'].sum()
        amt = (trades['amount'] * trades['price']).sum()
        summary['Total Funding Paid'] = rounded_get(funding, '$')
    
    days = (trades['transactTime'].iloc[-1] - trades['transactTime'].iloc[0]).days

    if days == 0:
        days = 1
        
    summary['Avg Funding Interest'] = rounded_get((funding/days)/(amt/days) * 100 * 365, '%')
    summary['Avg Slippage to 00 (Neg Bad)'] = calculate_slippage(buys, sells, 'price', 'expectedPrice')
    summary['Avg Slippage to 08 (Neg Bad)'] = calculate_slippage(buys, sells, 'price', 'close')

    print_trades = trades[['transactTime', 'side', 'price', 'transactTime', 'fee']]
    
    return summary, print_trades, buys, sells