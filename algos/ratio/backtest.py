import pandas as pd
import json

import os
import requests

import backtrader as bt

from algos.altcoin.backtest import add_volatility, CommInfoFractional, Custom_Data, get_sharpe, priceStrategy

def get_binance_df(symbol, cache=False):

    if cache == False:
        if not os.path.isdir("data/binance/"):
            os.makedirs("data/binance/")

        all_df = pd.DataFrame()
        res = requests.get('https://api.binance.com/api/v3/klines?symbol={}&interval=1d&limit=1000'.format(symbol))
        df = pd.DataFrame(json.loads(res.text))
        all_df = all_df.append(df, ignore_index=True)
        
        while len(df) >= 1000:
            res = requests.get('https://api.binance.com/api/v3/klines?symbol={}&interval=1d&limit=1000&startTime={}'.format(symbol, df.iloc[-1][0]))
            df = pd.DataFrame(json.loads(res.text))
            all_df = all_df.append(df, ignore_index=True)
            
        all_df = all_df[[0,1,2,3,4,5]]
        all_df.columns = ['startTime', 'open', 'high', 'low', 'close', 'volume']
        all_df['startTime'] = pd.to_datetime(all_df['startTime'], unit='ms')
        all_df = all_df.drop_duplicates(subset=['startTime'])
        all_df.to_csv("data/binance/{}.csv".format(symbol), index=None)
    
    df = pd.read_csv("data/binance/{}.csv".format(symbol))
    df['startTime'] = pd.to_datetime(df['startTime'])

    return df


def perform_backtests():
    if not os.path.isdir("data/binance/"):
        os.makedirs("data/binance/")
    
    config = pd.read_csv('algos/ratio/config.csv')
    config['vol_day'] = config['vol_day'].astype(int)
    config['prev_day'] = config['prev_day'].astype(int)

    for idx, row in config.iterrows():
        print(row['name'])
        mult = 1
        initial_cash = 1000

        pair = row['name']
        gaussian = row['gaussian']
        days = row['vol_day']
        number_days = row['prev_day']
        allocation = row['allocation']

        price_df = get_binance_df(pair)
        price_df = add_volatility(price_df, days=days, gaussian=gaussian)
        price_df['curr_group'] = pd.to_datetime(price_df['curr_group']).astype(int)
        price_df['startTime'] = pd.to_datetime(price_df['startTime'])

        price_data = Custom_Data(dataname=price_df)

        cerebro = bt.Cerebro()

        cerebro.adddata(price_data, name='data')
        cerebro.addstrategy(priceStrategy, number_days=number_days)
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.0, annualize=True, timeframe=bt.TimeFrame.Days)
        cerebro.addanalyzer(bt.analyzers.Calmar)
        cerebro.addanalyzer(bt.analyzers.DrawDown)
        cerebro.addanalyzer(bt.analyzers.Returns)
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer)
        cerebro.addanalyzer(bt.analyzers.TimeReturn)
        cerebro.addanalyzer(bt.analyzers.PyFolio)
        cerebro.addanalyzer(bt.analyzers.PositionsValue)
        
        cerebro.broker = bt.brokers.BackBroker(cash=initial_cash, slip_perc=0.01/100, commission = CommInfoFractional(commission=(0.075*mult)/100, mult=mult), slip_open=True, slip_out=True)  # 0.5%
        run = cerebro.run()
        portfolio, trades, operations = run[0].get_logs()
        trades.to_csv("data/binance/trades_{}.csv".format(pair), index=None)

if __name__ == "__main__":
    perform_backtests()