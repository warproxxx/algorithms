import pandas as pd
import json

import os
import requests

import backtrader as bt

from algos.altcoin.backtest import add_volatility, CommInfoFractional, Custom_Data, get_sharpe, priceStrategy, unbiasedTest, plot, get_df
from utils import print

import redis

import time

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
        try:
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

            start = pd.to_datetime(price_df['curr_group'].iloc[-1])
            start = start.replace(day=1)
            first_group = price_df[price_df['startTime'] == start].iloc[0]['curr_group']

            start_from = pd.to_datetime(first_group) - pd.Timedelta(days=int(row['prev_day']) + 4)
            # start_from = now - pd.Timedelta(days=20)
            start_month = price_df['startTime'].iloc[-1].month

            price_df = price_df[(price_df['startTime'] >= start_from)].reset_index(drop=True)
            price_data = Custom_Data(dataname=price_df)
            initial_cash = 1000

            cerebro = bt.Cerebro()
            
            cerebro.adddata(price_data, name='data')

            details = {'number_days': int(row['prev_day']), 'start_month': start_month, 'lag': 0}
            cerebro.addstrategy(unbiasedTest, number_days=details)
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
        except Exception as e:
            print(str(e))


    porfolios = porfolios[porfolios['Date'] >= now]
    porfolios = porfolios[:-1]
    
    btc = get_df('BTC-PERP', cache=False)
    btc_price = porfolios.merge(btc[['startTime', 'close']], left_on='Date', right_on='startTime', how='left')['close'].values
    
    for col in porfolios.columns:
        porfolios[col] = porfolios[col] * btc_price

    porfolios = porfolios.set_index('Date')
    porfolios = porfolios/porfolios.iloc[0] * 1000
    porfolios.to_csv("data/ratio_port.csv")

    check_days=[1,2,3,5,6,7,8,9,10,15,20,25]

    
    ret = porfolios.sum(axis=1)

    for d in check_days:
        if len(ret) > d:
            curr_ret = round(((ret.iloc[d] - ret.iloc[0])/ret.iloc[0]) * 100, 2)

            if curr_ret < -20:
                r = redis.Redis(host='localhost', port=6379, db=0)
                r.set('close_and_main_ratio', 1)
                time.sleep(3600)
                r.set('ratio_enabled', 0)

if __name__ == "__main__":
    perform_backtests()