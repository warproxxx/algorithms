import os
import io
import csv

import numpy as np
import pandas as pd

import backtrader as bt

class PandasData_Custom(bt.feeds.PandasData):
    lines = ('change', 'percentage_large', 'buy_percentage_large', 'macd', 'rsi', )
    params = (
        ('datetime', 0),
        ('open', 1),
        ('high', 2),
        ('low', 3),
        ('close', 4),
        ('volume', 5),
        ('change', 6),
        ('percentage_large', 7),
        ('buy_percentage_large', 8),
        ('macd', 9),
        ('rsi', 10)
    )

class CommInfoFractional(bt.CommissionInfo):
    
    def getsize(self, price, cash):
        '''Returns fractional size for cash operation @price'''
        return self.p.leverage * (cash / price)

class tradingStrategy(bt.Strategy):
    params = dict(parameters={})
        
    def __init__(self):
        self.close_price = self.datas[0].close
        self.macd = self.datas[0].macd
        self.rsi = self.datas[0].rsi
        self.percentage_large = self.datas[0].percentage_large
        self.buy_percentage_large = self.datas[0].buy_percentage_large
        
        self.position_since = 0
        
        self.trades = io.StringIO()
        self.trades_writer = csv.writer(self.trades)

        self.operations = io.StringIO()
        self.operations_writer = csv.writer(self.operations)

        self.portfolioValue = io.StringIO()
        self.portfolioValue_writer = csv.writer(self.portfolioValue)
        
        self.profit_percentages = []
        self.stops_triggered = 0
        
        params = self.params.parameters
        self.percentage_large_par = params['percentage_large']
        self.buy_percentage_large_par = params['buy_percentage_large']
        self.macd_par = params['macd']
        self.rsi_par = params['rsi']
        self.previous_days_par = int(params['previous_days'])
        self.position_since_par = params['position_since']
        self.position_since_diff_par = params['position_since_diff']
        self.change_par = params['change']
        self.pnl_percentage_par = params['pnl_percentage']
        self.close_percentage_par = params['close_percentage']
        self.profit_macd_par = params['profit_macd']
        self.mult = params['mult']
        self.stop_percentage_par = params['stop_percentage']
        self.print = params['print']
        
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.datetime(0)
        
        if self.print == True:
            print("Datetime: {} Message: {}".format(dt, txt))

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                ordertype = "BUY"
                self.log("BUY EXECUTED, Type: {}, Price: {}, Cost: {}, Comm: {}".format(order.info['name'], order.executed.price, order.executed.value, order.executed.comm))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                ordertype = "SELL"
                self.log("SELL EXECUTED, Type: {}, Price: {}, Cost: {}, Comm: {}".format(order.info['name'], order.executed.price, order.executed.value, order.executed.comm))
                
                if order.info['name'] == 'STOP LOSS':
                    self.stops_triggered = self.stops_triggered + 1
    #                 print(self.profit_percentages)
                self.profit_percentages = []
                
            self.trades_writer.writerow([self.datas[0].datetime.datetime(0), ordertype, order.executed.price, order.executed.value, order.executed.comm])
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
        tradesDf = pd.read_csv(self.trades, names=['Date', 'Type', 'Price', 'Total Spent', 'Comission'])
        tradesDf['Date'] = pd.to_datetime(tradesDf['Date'])

        self.operations.seek(0)
        operationsDf = pd.read_csv(self.operations, names=['Date', 'Profit'])
        operationsDf['Date'] = pd.to_datetime(operationsDf['Date'])
        operationsDf = operationsDf.merge(portfolioValueDf, on='Date',how='left')
        operationsDf['original_value'] = operationsDf['Value'] - operationsDf['Profit']
        operationsDf['pct_change'] = (operationsDf.Value - operationsDf.original_value)/operationsDf.original_value * 100

        return portfolioValueDf, tradesDf, operationsDf, self.stops_triggered
                
    def next(self):        
        self.portfolioValue_writer.writerow([self.datas[0].datetime.datetime(0), self.broker.getvalue()])
        
        if not self.position:
            self.position_since = 0
            
        changes =  []
        changes_log = {}
        
        for i in range(0, -1 * self.previous_days_par, -1):
            prev_change = self.data.change[i]
            changes.append(prev_change)
            changes_log[i] = prev_change
            
        changes = np.array(changes)        
        
        if self.position:                       
            pos = self.getposition(self.datas[0])
            comminfo = self.broker.getcommissioninfo(self.data)
            pnl = comminfo.profitandloss(pos.size, pos.price, self.data.close[0])  
            self.position_since = self.position_since + 1
            
            pnl_percentage = ((self.data.close[0]-pos.price)/pos.price) * 100 * self.mult
            
            if self.print == True:
                print("PNL is: {} and Balance is {}. Percentage is {}".format(pnl, self.broker.getvalue(), pnl_percentage))

            if (self.position_since > self.position_since_par):
                if pnl_percentage > self.pnl_percentage_par:
                    self.profit_percentages.append(pnl_percentage)
                    
                    if (self.macd < self.profit_macd_par) or (self.rsi > self.rsi_par): #close at loss or macd
                        self.log("LONG CLOSE {}".format(self.close_price[0]))
    #                         cls_ord = self.close()
                        cls_ord = self.close(oco=self.sl_ord)
                        cls_ord.addinfo(name="MANUAL CLOSE")
                        self.position_since = 0
                else:               
                    if (pnl_percentage < self.close_percentage_par) or (self.macd < self.macd_par) or (self.rsi > self.rsi_par): #close at loss or macd
                        self.log("LONG CLOSE {}".format(self.close_price[0]))
    #                         cls_ord = self.close()
                        cls_ord = self.close(oco=self.sl_ord)
                        cls_ord.addinfo(name="MANUAL CLOSE")
                        self.position_since = 0
            
        if (not self.position) and (sum(changes < self.change_par) >= (self.previous_days_par - self.position_since_diff_par)) and (self.macd > self.macd_par) and (self.rsi < self.rsi_par):
            if ((self.percentage_large > self.percentage_large_par) and (self.buy_percentage_large > self.buy_percentage_large_par)):
                self.log("LONG CREATE {}".format(self.close_price[0]))
                buy_ord = self.order_target_percent(target=.95)
                buy_ord.addinfo(name="LONG ENTRY")
                stop_size = buy_ord.size - abs(self.position.size)
                stop_target = self.close_price * self.stop_percentage_par
                self.sl_ord = self.sell(size=stop_size, exectype=bt.Order.Stop, price=stop_target)
                self.sl_ord.addinfo(name='STOP LOSS')
                if self.print == True:
                    print("STOP set at {}".format(stop_target))

                
                self.position_since = self.position_since + 1
        
def perform_backtest(df, parameters, print=False):
    parameters['print'] = print
    mult = parameters['mult']
    data = PandasData_Custom(dataname=df)
    cerebro = bt.Cerebro()

    cerebro.adddata(data)
    cerebro.addstrategy(tradingStrategy, parameters=parameters)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer)
    initial_cash = 500
    cerebro.broker = bt.brokers.BackBroker(cash=initial_cash, slip_perc=0.015/100, commission = CommInfoFractional(commission=(0.075*mult)/100, mult=mult, interest=(20/100)*mult, interest_long=True), slip_open=True, slip_out=True)  # 0.5%

    run = cerebro.run()
    return run