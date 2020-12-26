import pandas as pd
import numpy as np
import csv
import re
import json


import os
import io
from glob import glob


import backtrader as bt

from scipy.ndimage import gaussian_filter

import requests

from datetime import datetime
from utils import print

def get_df(symbol, cache=False):

    if cache == False:
        if not os.path.isdir("data/"):
            os.makedirs("data/")

        res = requests.get('https://ftx.com/api/markets/{}/candles?resolution=86400&limit=5000'.format(symbol))
        df = pd.DataFrame(json.loads(res.text)['result'])
        df['startTime'] = pd.to_datetime(df['startTime']).dt.tz_localize(None)
        df = df[['startTime', 'open', 'high', 'low', 'close', 'volume']]
        df.to_csv("data/{}.csv".format(symbol), index=None)
    
    df = pd.read_csv("data/{}.csv".format(symbol))
    df['startTime'] = pd.to_datetime(df['startTime'])

    return df

def add_volatility(price_df, days=30, gaussian=3.):
    price_df["30D_volatility"] = price_df['close'].rolling(days).std()/10
    gaussian_vols = []

    for idx, row in price_df.iterrows():
        gaussian_vols.append(gaussian_filter(price_df[:idx+1]['30D_volatility'], gaussian)[-1])

    price_df['30D_volatility'] = gaussian_vols
    
    curr_group = ""
    new_price_df = pd.DataFrame()

    for i in range(1, len(price_df)):
        row = price_df.iloc[i]
        curr_vol = price_df.iloc[i]['30D_volatility']
        prev_vol = price_df.iloc[i-1]['30D_volatility']
        three_vol = price_df.iloc[i-2]['30D_volatility']

        if pd.isnull(prev_vol) == False:
            if curr_group == "":
                curr_group = price_df.iloc[i]['startTime']


            if (three_vol - prev_vol) * (prev_vol - curr_vol) < 0:
                curr_group = price_df.iloc[i]['startTime']



            row['curr_group'] = curr_group
            new_price_df = new_price_df.append(row, ignore_index=True)


    new_price_df = new_price_df.sort_values('startTime')
    new_price_df = new_price_df[['startTime', 'open', 'high', 'low', 'close', 'volume', '30D_volatility', 'curr_group']]
    return new_price_df

class CommInfoFractional(bt.CommissionInfo):
    
    def getsize(self, price, cash):
        '''Returns fractional size for cash operation @price'''
        return self.p.leverage * (cash / price)
    
class Custom_Data(bt.feeds.PandasData):
    lines = ('30D_volatility', 'curr_group', )
    params = (
        ('datetime', 0),
        ('open', 1),
        ('high', 2),
        ('low', 3),
        ('close', 4),
        ('volume', 5),
        ('30D_volatility', 6),
        ('curr_group', 7)
    )
    
def get_sharpe(col):
    change = col.pct_change(1)

    try:
        sharpe = round(change.mean()/change.std() * (365**0.5), 2)
    except:
        sharpe = 0

    return sharpe

class priceStrategy(bt.Strategy):
    params = dict(number_days={})
    
    def __init__(self):        
        self.trades = io.StringIO()
        self.trades_writer = csv.writer(self.trades)

        self.operations = io.StringIO()
        self.operations_writer = csv.writer(self.operations)

        self.portfolioValue = io.StringIO()
        self.portfolioValue_writer = csv.writer(self.portfolioValue)
        
        self.first_time = True
        
        self.number_days = self.params.number_days

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.datetime(0)
#         print("Datetime: {} Message: {}".format(dt, txt))
    
    def notify_order(self, order):
            
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                ordertype = "BUY"
                self.log("BUY EXECUTED, Type: {}, Price: {}, Cost: {}, Comm: {}".format(order.info['name'], order.executed.price, order.executed.value, order.executed.comm))
            else:
                ordertype = "SELL"
                self.log("SELL EXECUTED, Type: {}, Price: {}, Cost: {}, Comm: {}".format(order.info['name'], order.executed.price, order.executed.value, order.executed.comm))
            
#             print(order)
            self.trades_writer.writerow([self.datas[0].datetime.datetime(0), ordertype, order.info['name'], order.executed.price, order.executed.size, order.executed.comm])
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("Order Canceled/Margin/Rejected")
            self.log(order.Rejected)

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS: {}, NET: {}'.format(trade.pnl, trade.pnlcomm))
        self.operations_writer.writerow([self.datas[0].datetime.datetime(0), trade.pnlcomm])

    def start(self):
        self.order = None 

    def get_logs(self):
        self.portfolioValue.seek(0)
        portfolioValueDf = pd.read_csv(self.portfolioValue, names=['Date', 'Value'])
        portfolioValueDf['Date'] = pd.to_datetime(portfolioValueDf['Date'])

        self.trades.seek(0)
        tradesDf = pd.read_csv(self.trades, names=['Date', 'Type', 'Data', 'Price', 'Size', 'Comission'])
        tradesDf['Date'] = pd.to_datetime(tradesDf['Date'])
        tradesDf['Total Spent'] = tradesDf['Price'] * tradesDf['Size']

        self.operations.seek(0)
        operationsDf = pd.read_csv(self.operations, names=['Date', 'Profit'])
        operationsDf['Date'] = pd.to_datetime(operationsDf['Date'])
        operationsDf = operationsDf.merge(portfolioValueDf, on='Date',how='left')
        operationsDf['original_value'] = operationsDf['Value'] - operationsDf['Profit']
        operationsDf['pct_change'] = (operationsDf.Value - operationsDf.original_value)/operationsDf.original_value * 100

        return portfolioValueDf, tradesDf, operationsDf
    
    def next(self):       
        self.portfolioValue_writer.writerow([self.datas[0].datetime.datetime(0), self.broker.getvalue()])
        price_data = self.datas[0]
        price_pos = self.getposition(price_data).size
        
        curr_group = pd.to_datetime(price_data.curr_group[0])
        curr_datetime = pd.to_datetime(price_data.datetime.datetime(0))
        
        if curr_group == curr_datetime:
            four_days_ago_price = price_data.open[-1 * self.number_days]
            today_price = price_data.close[0]
            
            if today_price >= four_days_ago_price:
                price_direction = 1
            else:
                price_direction = -1
                
            pos_direction = 1 if price_pos > 0 else -1
            
            if pos_direction != price_direction:
                order=self.order_target_percent(target=0.99*price_direction)
                order.addinfo(name=price_data._name)


def perform_backtests():
    if not os.path.isdir("data/"):
        os.makedirs("data/")
    
    config = pd.read_csv('algos/altcoin/config.csv')
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

        price_df = get_df(pair)
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
        trades.to_csv("data/trades_{}.csv".format(pair), index=None)

if __name__ == "__main__":
    perform_backtests()