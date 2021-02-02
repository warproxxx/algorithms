import pandas as pd
import json

import os
import requests

import backtrader as bt

from algos.altcoin.backtest import add_volatility, CommInfoFractional, Custom_Data, get_sharpe, priceStrategy, unbiasedTest, plot
from utils import print

import redis

#this includes one extra day in the chart. But the logic is that backtrader needs 1 day to open position. So although it looks wrong in chart, overall this is right
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
    porfolios = pd.DataFrame()

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

        plot(price_df, portfolio, pair)

        now = pd.Timestamp.utcnow().date()
        now = pd.to_datetime(now.replace(day=1))

        price_df = price_df[(price_df['startTime'] >= now - pd.Timedelta(days=20))].reset_index(drop=True)
        price_data = Custom_Data(dataname=price_df)
        initial_cash = 1000

        cerebro = bt.Cerebro()

        cerebro.adddata(price_data, name='data')
        cerebro.addstrategy(unbiasedTest, number_days={'number_days': int(row['prev_day']), 'lag': 0})
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.0, annualize=True, timeframe=bt.TimeFrame.Days)
        cerebro.addanalyzer(bt.analyzers.Calmar)
        cerebro.addanalyzer(bt.analyzers.DrawDown)
        cerebro.addanalyzer(bt.analyzers.Returns)
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer)
        cerebro.addanalyzer(bt.analyzers.TimeReturn)
        cerebro.addanalyzer(bt.analyzers.PyFolio)
        cerebro.addanalyzer(bt.analyzers.PositionsValue)

        cerebro.broker = bt.brokers.BackBroker(cash=initial_cash, slip_perc=0.01/100, commission = CommInfoFractional(commission=(0.075*row['mult'])/100, mult=row['mult']), slip_open=True, slip_out=True)  # 0.5%
        run = cerebro.run()

        portfolio, trades, operations = run[0].get_logs()
        pct_change = portfolio['Value'].pct_change().fillna(0)

        start = 1000
        vals = []

        for val in pct_change * row['mult']:
            start = start * (1+val)
            if start < 0:
                start = 0

            vals.append(start)

        portfolio[row['name']] = vals
        portfolio = portfolio[['Date', row['name']]]

        if len(porfolios) == 0:
            porfolios = portfolio
        else:
            porfolios = porfolios.merge(portfolio, on='Date', how='left')

        porfolios = porfolios[porfolios['Date'] >= now]
        porfolios.to_csv("data/ratio_port.csv", index=None)

        check_days=[3,5,10,15,20,25]

        porfolios = porfolios.set_index('Date')
        ret = porfolios.sum(axis=1)

        for d in check_days:
            if len(ret) > d:
                curr_ret = round(((ret.iloc[d] - ret.iloc[0])/ret.iloc[0]) * 100, 2)

                if curr_ret < -10:
                    r = redis.Redis(host='localhost', port=6379, db=0)
                    r.set('close_and_main_ratio', 1)
                    r.set('ratio_enabled', 0)

if __name__ == "__main__":
    perform_backtests()