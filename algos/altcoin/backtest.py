import pandas as pd
import numpy as np
import csv
import re
import json

import os
import io
from glob import glob

import backtrader as bt

import plotly.graph_objects as go
from scipy.ndimage import gaussian_filter

import requests

from datetime import datetime
from utils import print

import time

import redis

def create_multiple_plot(df, variable_names, time='Time', verbose=False):        
    fig = go.Figure(layout=go.Layout(xaxis={'spikemode': 'across'}))
    colors = ['#727272', '#56b4e9', "#009E73", "#000000"]
    last = len(variable_names) - 1
    
    var_one = variable_names[0]
    var_two = variable_names[1]
    
    
    for i in range(0, len(variable_names)):
        var = variable_names[i]
        
        if i <= (len(colors)):
            color = colors[i]
        else:
            color = ''
        
        if verbose == True:
            print("i: {} var: {} color: {}".format(i, var, color))
        
        if i != last:
            fig.add_trace(go.Scatter(x=df[time], y=df[var], name=var, marker={'color': color}, yaxis="y1"))
        else:
            fig.add_trace(go.Scatter(x=df[time], y=df[var], name=var, marker={'color': color}, yaxis="y2"))
    


    fig.update_layout(
            yaxis=dict(
                titlefont=dict(
                    color="#000000"
                ),
                tickfont=dict(
                    color="#000000"
                )
            ),
            yaxis2=dict(
                tickfont=dict(
                    color=color
                ),
                anchor="free",
                overlaying="y",
                side="left",
                position=1
            ))
            
    fig.update_layout(
        xaxis=go.layout.XAxis(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label="1m",
                         step="month",
                         stepmode="backward"),
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="YTD",
                         step="year",
                         stepmode="todate"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
                visible=True,
            ),
            type="date",
        )
    )
    
    fig = fig.update_xaxes(spikemode='across+marker')
    fig = fig.update_layout(hovermode="x")

    return fig

#this includes one extra day in the chart. But the logic is that backtrader needs 1 day to open position. So although it looks wrong in chart, overall this is right
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

def get_figure(df):
    decrease_to_increase = pd.to_datetime(df[(df['30D_volatility'] < df['30D_volatility'].shift(1)) & (df['30D_volatility'].shift(-1) > df['30D_volatility'])]['startTime'])
    increase_to_decrease = pd.to_datetime(df[(df['30D_volatility'] > df['30D_volatility'].shift(1)) & (df['30D_volatility'].shift(-1) < df['30D_volatility'])]['startTime'])

    hovertexts = list(("30D volatility : " + df['30D_volatility'].replace(np.nan, 0).round(2).astype(str)).values)
    fig = go.Figure(layout=go.Layout(xaxis={'spikemode': 'across'}))

    fig.add_trace(go.Scatter(x=df['startTime'], y=df['close'], name='Close Price', yaxis="y1", hovertext = hovertexts, line={"color": "#636EFA"}, fillcolor="black"))
    fig.add_trace(go.Scatter(x=df['startTime'], y=df['30D_volatility'], name='30D volatility', yaxis="y2", line={"color": "#EF553B"}))


    fig.update_layout(
                yaxis1=dict(
                    titlefont=dict(
                        color="#000000"
                    ),
                    tickfont=dict(
                        color="#000000"
                    ),
                    anchor="free",
                    domain=[0.25, 1], 
                    position=0.0    
                ),
                yaxis2=dict(
                    tickfont=dict(
                        color="#727272"
                    ),
                    anchor="free",
                    domain=[0, 0.18]
                )
    )
                
    fig.update_layout(hovermode="x unified")

    max_y = df['close'].max() + 0.1 * df['close'].max() 
    min_y = df['close'].min() - 0.3 * df['close'].min() 

    min_y = max(0, min_y)

    for increase_point in decrease_to_increase:
        fig.add_shape(dict(type="line", x0=increase_point, y0=min_y, x1=increase_point, y1=max_y, line=dict(color="green", width=1)))

    for decrease_point in increase_to_decrease:
        fig.add_shape(dict(type="line", x0=decrease_point, y0=min_y, x1=decrease_point, y1=max_y, line=dict(color="red", width=1)))

    fig.update_layout(
            xaxis=go.layout.XAxis(
                rangeslider=dict(
                    visible=True,
                    thickness=0.05
                ),
                type="date",
            )
        )

    html = fig.to_html()
    return html

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


class unbiasedTest(bt.Strategy):
    params = dict(number_days={})
    
    def __init__(self):        
        self.trades = io.StringIO()
        self.trades_writer = csv.writer(self.trades)

        self.operations = io.StringIO()
        self.operations_writer = csv.writer(self.operations)

        self.portfolioValue = io.StringIO()
        self.portfolioValue_writer = csv.writer(self.portfolioValue)
        
        self.first_time = True
        
        self.number_days = self.params.number_days['number_days']
        self.lag = self.params.number_days['lag']
        self.start_month = self.params.number_days['start_month']

        self.entered = False

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.datetime(0)
        print("Datetime: {} Message: {}".format(dt, txt))
    
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
        
        if curr_datetime.day == 1 + self.lag and curr_datetime.month == self.start_month:
            n_days = (curr_group-curr_datetime).days
            
            four_days_ago_price = price_data.open[n_days - self.number_days]
            today_price = price_data.close[n_days]
                        
            if today_price >= four_days_ago_price:
                price_direction = 1
            else:
                price_direction = -1
            
            pos_direction = 1 if price_pos > 0 else -1

            order=self.order_target_percent(target=0.99*price_direction)
            order.addinfo(name=price_data._name)
            
            self.entered = True

        
        if self.entered == True:
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
        # print("Datetime: {} Message: {}".format(dt, txt))
    
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

def plot(df, portfolio, name):
    fig_html = get_figure(df)

    portfolio = portfolio.merge(df[['startTime', 'close']], left_on='Date', right_on='startTime')[['Date', 'Value', 'close']]
    fig = create_multiple_plot(portfolio, ['Value', 'close'], 'Date')
    price_html = fig.to_html()

    with open('frontend_interface/static/{}_vol.html'.format(name), 'w') as file:
        file.write(fig_html)

    with open('frontend_interface/static/{}_price.html'.format(name), 'w') as file:
        file.write(price_html)

def perform_backtests(skip_setting=False):
    if not os.path.isdir("data/"):
        os.makedirs("data/")

    config = pd.read_csv('algos/altcoin/config.csv')
    config['vol_day'] = config['vol_day'].astype(int)
    config['prev_day'] = config['prev_day'].astype(int)
    porfolios = pd.DataFrame()

    for idx, row in config.iterrows():
        try:
            if row['name'] not in porfolios.columns:
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

                cerebro.broker = bt.brokers.BackBroker(cash=initial_cash, slip_perc=0.01/100, commission = CommInfoFractional(commission=(0.075)/100, leverage=row['mult']), slip_open=True, slip_out=True)  # 0.5%
                run = cerebro.run()

                portfolio, trades, operations = run[0].get_logs()

                curr = portfolio[portfolio['Value'] < 0]

                if len(curr) > 0:
                    old = portfolio[curr.index[0]:]
                    portfolio = portfolio[:curr.index[0]]
                    old['Value'] = 0
                    portfolio = portfolio.append(old)
                    portfolio = portfolio.fillna(0)

                portfolio[row['name']] = portfolio['Value']

                portfolio = portfolio[['Date', row['name']]]

                if len(porfolios) == 0:
                    porfolios = portfolio
                else:
                    porfolios = porfolios.merge(portfolio, on='Date', how='left')
        except Exception as e:
            print(str(e))

    porfolios = porfolios[porfolios['Date'] >= now]
    porfolios = porfolios[:-1]
    porfolios = porfolios.set_index('Date')

    for subalgo, rows in config.groupby('subalgo'):
        stop_threshold = rows.iloc[0]['stop_threshold']
        names = [name for name in list(rows['name'].values) if name in porfolios.columns]

        porfolios = porfolios[names]

        porfolios.to_csv("data/altcoin_port_{}.csv".format(subalgo))

        if skip_setting == False:
            check_days=[3,5,10,15,20,25]        
            ret = porfolios.sum(axis=1)

            for d in check_days:
                if len(ret) > d:
                    curr_ret = round(((ret.iloc[d] - ret.iloc[0])/ret.iloc[0]) * 100, 2)

                    if curr_ret < stop_threshold:
                        r = redis.Redis(host='localhost', port=6379, db=0)
                        
                        try:
                            altcoin_close_str = r.get('altcoin_close').decode()
                            altcoin_close = altcoin_close_str.split(",")

                            if subalgo not in altcoin_close:
                                if altcoin_close_str == "":
                                    new = subalgo
                                else:
                                    new = altcoin_close_str + "," + subalgo
                                
                                r.set('altcoin_close', new)
                        except:
                            r.set('altcoin_close', subalgo)

                        r.set('perform_close_and_main_set', subalgo)


if __name__ == "__main__":
    perform_backtests(skip_setting=True)