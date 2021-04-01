import pandas as pd
import requests
import os
import datetime
import json
import time
import ccxt
import numpy as np
import threading

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
    
    since = int(exchange.parse8601("2021-02-01 00:00:00")/1000)
    all_fundings = []
    while True:
        curr_fundings = exchange.private_get_funding_payments(params={'start_time' :since})['result']
        if len(curr_fundings) > 0:
            since = exchange.parse8601(curr_fundings[-1]['time'])/1000
            all_fundings = all_fundings + curr_fundings

            if len(curr_trades) < limit:
                break
        else:
            break
    
    try:
        fundings = pd.DataFrame([funding for funding in all_fundings])
        fundings['time'] = pd.to_datetime(fundings['time'])
        fundings['time'] = fundings['time'].dt.tz_localize(None)
        fundings['time'] = fundings['time'].dt.round('30D')
        fundings = fundings[['time', 'future', 'payment']].groupby(['future', 'time']).sum().reset_index()
    except:
        fundings = pd.DataFrame()
        
    try:
        trades = pd.DataFrame([trade['info'] for trade in all_trades])
        trades = trades[['time', 'market', 'side', 'size', 'price', 'fee']]

        trades['time'] = pd.to_datetime(trades['time']).dt.tz_localize(None)
        trades['time'] = trades['time'].dt.round('120min')
        trades = trades.groupby(['time', 'side']).apply(merge_trades)

        trades = trades.reset_index()
    except:
        trades = pd.DataFrame()
        
    return trades, fundings

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
    altcoin_fundings = pd.DataFrame()

    for symbol in altcoin_accs['nickname']:
        trades, fundings = get_trades(symbol)
        altcoin_trades = altcoin_trades.append(trades, ignore_index=True)
        altcoin_fundings = altcoin_fundings.append(fundings, ignore_index=True)

    # perp_trades, perp_funding = get_trades('PERP')
    # move_trades, move_funding = get_trades('MOVE')

    # vol_trades = perp_trades.append(move_trades, ignore_index=True)
    # vol_fundings = perp_funding.append(move_funding, ignore_index=True)

    altcoin_trades.to_csv("data/altcoin_trades.csv", index=None)
    altcoin_fundings.to_csv("data/altcoin_funding.csv", index=None)

    # vol_trades.to_csv("data/vol_trades.csv", index=None)
    # vol_fundings.to_csv("data/vol_funding.csv", index=None)