import ccxt

import os
import time
import numpy as np
import json
import pandas as pd
import redis
import datetime
import decimal
import inspect
from algos.vol_trend.defines import trade_methods
import sys
from utils import print

class liveTrading():
    def __init__(self, symbol='BTC-PERP', testnet=True):
        self.symbol = symbol
        self.parameters = json.load(open('algos/vol_trend/parameters.json'))
        #set leverage acc to pair name

        self.lev = self.parameters['{}_mult'.format(symbol)]
        self.threshold_tiggered = False
        self.attempts = 5

        apiKey = os.getenv('FTX_{}_ID'.format(symbol))
        apiSecret = os.getenv('FTX_{}_SECRET'.format(symbol))

    
        self.exchange = ccxt.ftx({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                        'options': {'defaultMarket': 'futures'}
                    })
            
        self.increment = 0.5
        self.r = redis.Redis(host='localhost', port=6379, db=0)            
        self.update_parameters()

    def update_parameters(self):
        self.parameters = json.load(open('algos/vol_trend/parameters.json'))
        self.lev = self.parameters['{}_mult'.format(self.symbol)]
        count = 0
        
        while count < 5:
            try:
                stats = self.exchange.private_post_account_leverage({"leverage": 5})
                break
            except Exception as e:
                break

    def close_open_orders(self, close_stop=False):
        self.update_parameters()
        
        for lp in range(self.attempts):
            try:
                if close_stop == True:
                    self.exchange.cancel_all_orders()

                orders = self.exchange.fetch_open_orders()

                if len(orders) > 0:
                    for order in orders:
                        self.exchange.cancel_order(order['info']['id'])
            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break
    
    def close_stop_order(self):
        self.close_open_orders(close_stop=True)
    
    def get_orderbook(self):
        orderbook = {}
        orderbook['best_ask'] = float(self.r.get('FTX_best_ask').decode())
        orderbook['best_bid'] = float(self.r.get('FTX_best_bid').decode())

        return orderbook

    def get_position(self):
        '''
        Returns position (LONG, SHORT, NONE), average entry price and current quantity
        '''

        for lp in range(self.attempts):
            try:
                pos = pd.DataFrame(self.exchange.private_get_positions(params={'showAvgPrice': True})['result'])
                pos = pos[pos['future'] == self.symbol].iloc[0]

                if float(pos['openSize']) == 0:
                    return 'NONE', 0, 0

                if float(pos['openSize']) > 0:
                    current_pos = "LONG"
                elif float(pos['openSize']) < 0:
                    current_pos = "SHORT" 
                
                return current_pos, float(pos['recentAverageOpenPrice']), float(pos['openSize'])
            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break

                print(e)
                time.sleep(1)
                pass

    def set_position(self):
        for lp in range(self.attempts):
            try:
                current_pos, avgEntryPrice, amount = self.get_position()
        
                self.r.set('FTX_{}_avgEntryPrice'.format(self.symbol), avgEntryPrice)
                self.r.set('FTX_{}_current_pos'.format(self.symbol), current_pos)
                self.r.set('FTX_{}_pos_size'.format(self.symbol), amount)

                balance = self.get_balance()
                self.r.set('FTX_{}_balance'.format(self.symbol), balance)
                break

            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break

                print(e)
                time.sleep(1)
                pass
    
    def get_balance(self):
        return float(self.exchange.fetch_balance()['USD']['free'])

    def limit_trade(self, order_type, amount, price)
        if amount > 0:
            print("Sending limit {} order for {} of size {} @ {} on {} in {}".format(order_type, self.symbol, amount, price, self.exchange_name, datetime.datetime.now()))

        params = {
            'postOnly': True
            }
        order = self.exchange.create_order(self.symbol, type="limit", side=order_type.lower(), amount=amount, price=price, params=params)
        order = self.exchange.fetch_order(order['info']['id'])

        if order['status'] == 'canceled':
            return []

        return order

    def send_limit_order(self, order_type):
        '''
        Detects amount and sends limit order for that amount
        '''
        for lp in range(self.attempts):
            try:
                amount, price = self.get_max_amount(order_type)

                if amount == 0:
                    return [], 0

                order = self.limit_trade(order_type, amount, price)

                return order, price
            except ccxt.BaseError as e:
                print(e)
                pass

    def market_trade(self, order_type, amount):
        '''
        Performs market trade detecting exchange for the given amount
        '''

        if amount > 0:
            print("Sending market {} order for {} of size {} on {} in {}".format(order_type, self.symbol, amount, self.exchange_name, datetime.datetime.now()))
            order = self.exchange.create_order(self.symbol, 'market', order_type.lower(), amount, None)


    def send_market_order(self, order_type):
        '''
        Detects amount and market buys/sells the amount
        '''
        for lp in range(self.attempts):
            try:
                self.close_open_orders()
                amount, price = self.get_max_amount(order_type)
                order = self.market_trade(order_type, amount)     
                return order, price    
            except ccxt.BaseError as e:
                print(e)
                pass

    
    def second_average(self, intervals, sleep_time, order_type):
        self.close_open_orders()
        self.threshold_tiggered = False

        amount, price = self.get_max_amount(order_type)

        trading_array = []


        if amount != 0:
            amount = abs(amount)
            single_size = round_down(amount / intervals, 3)
            final_amount = round_down(amount - (single_size * (intervals - 1)), 3)

        for amount in trading_array:
            order = self.market_trade(order_type, amount) 
            time.sleep(sleep_time)

        current_pos, avgEntryPrice, amount = self.get_position()

        if current_pos == 'LONG':
            if self.threshold_tiggered == False:
                amount, price = self.get_max_amount(order_type)
                order = self.market_trade(order_type, amount)

     def fill_order(self, order_type, method='attempt_limit'):
        '''
        Parameters:
        ___________

        order_type (string):
        buy or sell

        method (string):
        What to of strategy to use for selling. Strategies:

        attempt_limit: Tries selling limit with best price for 2 mins. Sells at market price if not sold
        5sec_average: Divides into 24 parts and makes market order of that every 5 second
        now: Market buy instantly
        take_biggest: Takes the biggest. If not filled, waits 30 second and takes it again. If not filled by end, takes at market.

        '''

        if method not in trade_methods:
            print("Method not implemented yet")
            return

        print("Time at filling order is: {}".format(datetime.datetime.now()))

        for lp in range(self.attempts):         
            
            curr_pos = self.r.get('{}_current_pos'.format(self.exchange_name)).decode()

            if curr_pos == "NONE" and order_type=='sell': #to fix issue caused by backtrader verification idk why tho.
                print("Had to manually prevent sell order")
                break
                
                
            if method == "attempt_limit":
                try:
                    order, limit_price = self.send_limit_order(order_type)

                    if len(order) == 0:
                        print("Wants to close a zero position lol")
                        self.set_position()
                        return

                    for lp in range(self.attempts):
                        order = self.exchange.fetch_order(order['info']['id'])
                        order_status = order['info']['size']
                        filled_string = order['info']['filledSize']

                    if order_status != filled_string:
                        time.sleep(.5) 
                        orderbook = self.get_orderbook()
                        print("Best Bid is {} and Best Ask is {}".format(orderbook['best_ask'], orderbook['best_bid']))

                        if order_type == 'buy':
                            current_full_time = str(datetime.datetime.now().minute)
                            current_time_check = current_full_time[1:]

                            if ((current_full_time == '9' or current_time_check == '9') and (datetime.datetime.now().second > 50)) or ((current_full_time == '0' or current_time_check == '0')):
                                print("Time at sending market order is: {}".format(datetime.datetime.now()))
                                order = self.send_market_order(order_type)
                                break

                            current_match = orderbook['best_bid']

                            if current_match >= (limit_price + self.increment):
                                print("Current price is much better, closing to open new one")
                                self.close_open_orders()
                                order, limit_price = self.send_limit_order(order_type)

                        elif order_type == 'sell':
                            current_full_time = str(datetime.datetime.now().minute)
                            current_time_check = current_full_time[1:]

                            if ((current_full_time == '9' or current_time_check == '9') and (datetime.datetime.now().second > 50)) or ((current_full_time == '0' or current_time_check == '0')):
                                print("Time at sending market order is: {}".format(datetime.datetime.now()))
                                order = self.send_market_order(order_type)
                                break

                            current_match = orderbook['best_ask']

                            if current_match <= (limit_price - self.increment):
                                print("Current price is much better, closing to open new one")
                                self.close_open_orders()
                                order, limit_price = self.send_limit_order(order_type)


                    else:
                        print("Order has been filled. Exiting out of loop")
                        self.close_open_orders()
                        break
                return
            except ccxt.BaseError as e:
                print(e)
                pass
            elif method == "5sec_average":
                self.second_average(12, 4.8, order_type)
                break
            elif method == "10sec_average":
                self.second_average(12, 9.8, order_type)
                break
            elif method == "now":
                amount, price = self.get_max_amount(order_type)
                order = self.market_trade(order_type, amount)
                break
