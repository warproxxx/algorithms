import pandas as pd
import requests
import os
import datetime
import json
import time
import ccxt
import numpy as np
import threading

def get_ftx_data():
    apiKey = os.getenv('BITMEX_ID')
    apiSecret = os.getenv('BITMEX_SECRET')

    exchange = ccxt.bitmex({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                    })
    limit = 500
    since = exchange.parse8601("2021-02-5 00:00:00")

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
    # funding = funding[['transactTime', 'exchange', 'symbol', 'qty', 'price', 'asset']]
    return trades, funding

def merge_trades(df):
    ser = {}
    filled = df['size']
    ser['avgPrice'] = sum(filled)/(sum(filled/df['price']))
    ser['amount'] = sum(filled)
    ser['market'] = df.iloc[0]['market']
    ser['fee'] = sum(df['fee'])
    
    return pd.Series(ser)

def get_trades(subaccount, limit = 500):
    apiKey = os.getenv('FTX_ID')
    apiSecret = os.getenv('FTX_SECRET')

    exchange = ccxt.ftx({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                    })
    
    exchange.headers = {
                        'FTX-SUBACCOUNT': subaccount,
                    }
    since = exchange.parse8601("2021-02-01 00:00:00")
    all_trades = []

    while True:
        curr_trades = exchange.fetchMyTrades(limit=limit, since=since)
        
        if len(curr_trades) > 0:
            since = exchange.parse8601(curr_trades[-1]['datetime'])
            all_trades = all_trades + curr_trades

            if len(curr_trades) < limit:
                break
        else:
            break
            
            
    try:
        trades = pd.DataFrame([trade['info'] for trade in all_trades])
        trades = trades[['time', 'market', 'side', 'size', 'price', 'fee']]

        trades['time'] = pd.to_datetime(trades['time']).dt.tz_localize(None)
        trades['time'] = trades['time'].dt.round('120min')
        trades = trades.groupby(['time', 'side']).apply(merge_trades)

        return trades.reset_index()
    except:
        return pd.DataFrame()

def save_ftx_trades():
    apiKey = os.getenv('FTX_ID')
    apiSecret = os.getenv('FTX_SECRET')

    exchange = ccxt.ftx({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                    })

    accounts = pd.DataFrame(exchange.private_get_subaccounts()['result'])

    altcoin_accs = accounts[accounts['nickname'].str.contains("-PERP")]

    altcoin_trades = pd.DataFrame()

    for symbol in altcoin_accs['nickname']:
        trades = get_trades(symbol)
        altcoin_trades = altcoin_trades.append(trades, ignore_index=True)

    perp_trades = get_trades('PERP')
    move_trades = get_trades('MOVE')

    vol_trades = perp_trades.append(move_trades, ignore_index=True)

    altcoin_trades.to_csv("data/altcoin_trades.csv", index=None)
    vol_trades.to_csv("data/vol_trades.csv", index=None)