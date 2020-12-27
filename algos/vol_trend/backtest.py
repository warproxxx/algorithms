import pandas as pd
import numpy as np
import csv
import json
import re

from scipy.ndimage import gaussian_filter

import requests

import os
import io
import traceback
from glob import glob

from datetime import datetime
import time

import backtrader as bt

from utils import print

def get_df(symbol):
    res = requests.get('https://ftx.com/api/markets/{}/candles?resolution=86400&limit=5000'.format(symbol))
    df = pd.DataFrame(json.loads(res.text)['result'])
    df['startTime'] = pd.to_datetime(df['startTime']).dt.tz_localize(None)
    df = df[['startTime', 'open', 'high', 'low', 'close', 'volume']]
    return df

def add_volatility(price_df):
    price_df["30D_volatility"] = price_df['close'].rolling(30).std()/10
    gaussian_vols = []

    for idx, row in price_df.iterrows():
        gaussian_vols.append(gaussian_filter(price_df[:idx+1]['30D_volatility'], 3.)[-1])

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

def save_move_data():
    pairs = json.load(open('algos/vol_trend/pairs.json'))

    for pair in pairs:
        curr_move_df = get_df(pair)
        curr_move_df.to_csv('data/vol_data/{}.csv'.format(pair), index=None)

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

class volStrategy(bt.Strategy):
    def __init__(self):        
        self.trades = io.StringIO()
        self.trades_writer = csv.writer(self.trades)

        self.operations = io.StringIO()
        self.operations_writer = csv.writer(self.operations)

        self.portfolioValue = io.StringIO()
        self.portfolioValue_writer = csv.writer(self.portfolioValue)
        
        self.curr_pos_in = None

    def log(self, txt, dt=None):
        dt = dt or self.datetime.date()
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
            self.trades_writer.writerow([self.datetime.date(), ordertype, order.info['name'], order.executed.price, order.executed.size, order.executed.comm])
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("Order Canceled/Margin/Rejected")
            self.log(order.Rejected)

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS: {}, NET: {}'.format(trade.pnl, trade.pnlcomm))
        self.operations_writer.writerow([self.datetime.date(), trade.pnlcomm])

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
    
    def perform_trade(self,target, data):
#         print(self.broker.getvalue())
        target *= self.broker.getvalue()
#         print(target)
        possize = self.getposition(data, self.broker).size
        
        if target == 0:
            price = data.close[0]
            print("Price is {},Value is {} ".format(price, self.broker.getvalue()))
            return self.close(data=data, size=possize, price=price)
        else:
            value = self.broker.getvalue(datas=[data])
            comminfo = self.broker.getcommissioninfo(data)

            price = data.close[0]
            
            if target > 0:
                size = comminfo.getsize(price, target - value)                    
                return self.buy(data=data, size=size, price=price)

            elif target < value:
                size = comminfo.getsize(price, value - target)
                return self.sell(data=data, size=size, price=price)
    
    def prenext(self):
        self.next()
    
    def get_data_to_trade(self):
        feeds_with_data = [d for i, d in enumerate(self.datas) if len(d)]
        curr_datetime = pd.to_datetime(self.datetime.date())
        highest_volume = 0
        highest_volume_data = None
        
        for data in feeds_with_data:
            end_time = pd.to_datetime(data._name.split('-')[-1]).to_period("Q").end_time
            if (end_time - curr_datetime).days > 45:
                curr_vol = 0
    
                for i in range(-3, 0, 1):
                    curr_vol = curr_vol + data.volume[i]
                        
                if curr_vol > highest_volume:
                    highest_volume = curr_vol
                    highest_volume_data = data._name
                        
        return highest_volume_data
    
    def get_price_direction(self):
        #also look at using one with the highest volume only
        feeds_with_data = [d for i, d in enumerate(self.datas) if len(d)]
        total_longs = 0
        total_shorts = 0
        
        for curr_data in feeds_with_data:
            four_days_ago_price = curr_data.open[-4]
            today_price = curr_data.close[0]

            if today_price >= four_days_ago_price:
                total_longs = total_longs + 1
            else:
                total_shorts = total_shorts + 1
        
        if total_longs >= total_shorts:
            return 1
        else:
            return -1
                
        
    def next(self):
        self.portfolioValue_writer.writerow([self.datetime.date(), self.broker.getvalue()])
        data_name = self.get_data_to_trade()
        curr_data = self.getdatabyname(data_name)
        pos = self.getposition(curr_data).size
        
        curr_group = pd.to_datetime(curr_data.curr_group[0])
        curr_datetime = self.datetime.date()
        
        if curr_group == curr_datetime:
            
            price_direction=self.get_price_direction()
                
            
            if self.curr_pos_in != None:
                if self.curr_pos_in != curr_data._name:
                    old_data = self.getdatabyname(self.curr_pos_in)
                    order=self.close(data=old_data)
                    order.addinfo(name=self.curr_pos_in)
                    
                    order=self.perform_trade(target=0.99*price_direction,data=curr_data)
                    order.addinfo(name=curr_data._name)
                    self.curr_pos_in = curr_data._name
                    return
            
            
            
            price_pos = self.getposition(curr_data).size
            pos_direction = 1 if price_pos > 0 else -1
            
            
            if pos_direction != price_direction:
                order=self.perform_trade(target=0.99*price_direction,data=curr_data)
                order.addinfo(name=curr_data._name)
                self.curr_pos_in = curr_data._name

class priceStrategy(bt.Strategy):
    def __init__(self):        
        self.trades = io.StringIO()
        self.trades_writer = csv.writer(self.trades)

        self.operations = io.StringIO()
        self.operations_writer = csv.writer(self.operations)

        self.portfolioValue = io.StringIO()
        self.portfolioValue_writer = csv.writer(self.portfolioValue)
        
        self.first_time = True

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
            four_days_ago_price = price_data.open[-4]
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

    pairs = json.loads(requests.get('https://ftx.com/api/markets').text)['result']
    pairs_list =  [pair['name'] for pair in pairs if re.search("MOVE-20[0-9][0-9]Q", pair['name'])]

    with open('algos/vol_trend/pairs.json', 'w') as f:
        json.dump(pairs_list, f)
        
    price_df = get_df('BTC/USD')
    new_price_df = add_volatility(price_df)    
    new_price_df.to_csv('data/price_df.csv', index=None)

    save_move_data()

    price_df = pd.read_csv('data/price_df.csv')
    price_df['curr_group'] = pd.to_datetime(price_df['curr_group']).astype(int)
    price_df['startTime'] = pd.to_datetime(price_df['startTime'])
    price_data = Custom_Data(dataname=price_df)
    initial_cash = 1000
    mult = 1

    cerebro = bt.Cerebro()

    cerebro.adddata(price_data, name='data')
    cerebro.addstrategy(priceStrategy)
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
    trades.to_csv("data/trades_perp.csv", index=None)


    initial_cash = 1000
    mult=1

    price_df = pd.read_csv('data/price_df.csv')
    price_df['curr_group'] = pd.to_datetime(price_df['curr_group']).astype(int)
    price_df['startTime'] = pd.to_datetime(price_df['startTime'])

    cerebro = bt.Cerebro()
    files = glob('data/vol_data/*')
    files.sort()
    first_date = pd.to_datetime(pd.read_csv(files[0]).iloc[0]['startTime'])
    last_date = pd.to_datetime(pd.read_csv(files[-1]).iloc[-1]['startTime'])

    for file in files:
        name = file.split("/")[-1].replace(".csv", "")
        move_df = pd.read_csv(file)
        move_df['startTime'] = pd.to_datetime(move_df['startTime'])
        move_df = move_df.merge(price_df[['startTime', '30D_volatility', 'curr_group']], on='startTime')
        data = Custom_Data(dataname=move_df)
        cerebro.adddata(data, name=name)

    cerebro.addstrategy(volStrategy)
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
    trades.to_csv("data/trades_move.csv", index=None)

if __name__ == "__main__":
    perform_backtests()