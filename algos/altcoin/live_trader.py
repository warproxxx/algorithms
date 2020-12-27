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
import sys
from utils import print

def round_down(value, decimals):
    with decimal.localcontext() as ctx:
        d = decimal.Decimal(value)
        ctx.rounding = decimal.ROUND_DOWN
        return float(round(d, decimals))

class liveTrading():
    def __init__(self, symbol, testnet=True):
        self.symbol = symbol
        
        self.threshold_tiggered = False
        self.attempts = 5

        apiKey = os.getenv('FTX_alt_ID')
        apiSecret = os.getenv('FTX_alt_SECRET')
        self.r = redis.Redis(host='localhost', port=6379, db=0)   
    
        self.exchange = ccxt.ftx({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                        'options': {'defaultMarket': 'futures'}
                    })


        self.neutral_exchange = ccxt.ftx({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                        'options': {'defaultMarket': 'futures'}
                    })

        self.method = "now"

        self.exchange.headers = {
                                        'FTX-SUBACCOUNT': symbol,
                                }

        config = pd.read_csv('algos/altcoin/config.csv')
        curr_config = config[config['name'] == self.symbol]
        self.method = curr_config['method']

        self.increment = 0.5
        self.update_parameters()
        
    def create_subaccount(self, name):
        try:
            self.neutral_exchange.private_post_subaccounts({'nickname': name})
            return 1
        except Exception as e:
            if 'already exists' in str(e):
                print("Subaccount {} exist".format(name))
                return 1
        
        return 0

    def transfer_to_subaccount(self, amount, destination, source='main', coin='USD'):
        self.neutral_exchange.private_post_subaccounts_transfer({'coin': coin, 'size': amount, 'source': source, 'destination': destination})

    def update_parameters(self):
        config = pd.read_csv('algos/altcoin/config.csv')
        curr_config = config[config['name'] == self.symbol].iloc[0]
        self.lev = int(curr_config['mult'])

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
    
    def get_orderbook(self):
        orderbook = {}
        orderbook['best_ask'] = float(self.r.get('{}_best_ask'.format(self.symbol)).decode())
        orderbook['best_bid'] = float(self.r.get('{}_best_bid'.format(self.symbol)).decode())

        return orderbook

    def get_position(self):
        '''
        Returns position (LONG, SHORT, NONE), average entry price and current quantity
        '''
        for lp in range(self.attempts):
            try:
                pos = pd.DataFrame(self.exchange.private_get_positions(params={'showAvgPrice': True})['result'])
                if len(pos) == 0:
                    return 'NONE', 0, 0
                try:
                    pos = pos[pos['future'] == self.symbol].iloc[0]
                except:
                    return 'NONE', 0, 0

                if float(pos['openSize']) == 0:
                    return 'NONE', 0, 0

                if pos['side'] == 'buy':
                    current_pos = "LONG"
                elif pos['side'] == 'sell':
                    current_pos = "SHORT" 
                
                return current_pos, float(pos['recentAverageOpenPrice']), float(pos['openSize'])
            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break

                print(e)
                time.sleep(1)

        return 'NONE', 0, 0

    def set_position(self):
        for lp in range(self.attempts):
            try:
                current_pos, avgEntryPrice, amount = self.get_position()
        
                self.r.set('{}_avgEntryPrice'.format(self.symbol), avgEntryPrice)
                self.r.set('{}_current_pos'.format(self.symbol), current_pos)
                self.r.set('{}_pos_size'.format(self.symbol), amount)

                balance = self.get_balance()

                self.r.set('{}_balance'.format(self.symbol), balance)

                break

            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break

                print(e)
                time.sleep(1)
                pass
    
    def get_balance(self):
        try:
            return float(self.exchange.fetch_balance()['USD']['free'])
        except:
            return 0

    def get_subaccount_balance(self, name):
        balance = pd.DataFrame(self.neutral_exchange.private_get_subaccounts_nickname_balances({'nickname': name})['result'])

        try:
            return balance[balance['coin'] == 'USD'].iloc[0]['free']
        except:
            return 0

    def limit_trade(self, order_type, amount, price):        
        print("Sending limit {} order for {} of size {} @ {} in {}".format(order_type, self.symbol, amount, price, datetime.datetime.now()))

        params = {
            'postOnly': True
            }
        order = self.exchange.create_order(self.symbol, type="limit", side=order_type.lower(), amount=amount, price=price, params=params)
        order = self.exchange.fetch_order(order['info']['id'])

        if order['status'] == 'canceled':
            return []

        return order

    def get_max_amount(self, order_type):
        '''
        Get the max buyable/sellable amount
        '''
        orderbook = self.get_orderbook()

        if order_type == 'open':
            price = orderbook['best_ask'] - self.increment
            balance = self.get_balance()
            amount = round_down(((balance * self.lev)/price) * 0.99, 4)
            return amount, price

        elif order_type == 'close':
            price = orderbook['best_bid'] + self.increment
            current_pos, avgEntryPrice, amount = self.get_position()
            return float(abs(amount)), float(price)

    def send_limit_order(self, type, direction):
        '''
        Detects amount and sends limit order for that amount
        '''
        for lp in range(self.attempts):
            try:
                amount, price = self.get_max_amount(type)
                order = self.limit_trade(direction, amount, price)

                return order, price
            except ccxt.BaseError as e:
                print(e)
                pass

        
        return [], 0

    def market_trade(self, order_type, amount):
        '''
        Performs market trade detecting exchange for the given amount
        '''

        print("Sending market {} order for {} of size {} in {}".format(order_type, self.symbol, amount, datetime.datetime.now()))
        order = self.exchange.create_order(self.symbol, 'market', order_type.lower(), amount, None)
        return order



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

    
    def second_average(self, intervals, sleep_time, type, direction, trade_direction):
        self.close_open_orders()
        self.threshold_tiggered = False

        amount, price = self.get_max_amount(type)

        trading_array = []

        if amount != 0:
            amount = abs(amount)
            single_size = round_down(amount / intervals, 3)
            final_amount = round_down(amount - (single_size * (intervals - 1)), 3)

            trading_array = [single_size] * (intervals - 1)
            trading_array.append(final_amount)

        for amount in trading_array:
            order = self.market_trade(trade_direction, amount) 
            time.sleep(sleep_time)

        if self.threshold_tiggered == False:
            amount, price = self.get_max_amount(type)
            order = self.market_trade(trade_direction, amount)


    def fill_order(self, type, direction):
        '''
        Parameters:
        ___________

        type (string):
        open or close

        direction (string):
        long or short

        attempt_limit: Tries selling limit with best price for 2 mins. Sells at market price if not sold
        5sec_average: Divides into 24 parts and makes market order of that every 5 second
        now: Market buy instantly
        take_biggest: Takes the biggest. If not filled, waits 30 second and takes it again. If not filled by end, takes at market.

        '''
        method = self.method

        self.set_position()
        print("Time at filling order is: {}".format(datetime.datetime.now()))

        if direction == 'long' and type == 'close':
            trade_direction = 'sell'
        elif direction == 'long' and type == 'open':
            trade_direction = 'buy'
        elif direction == 'short' and type == 'open':
            trade_direction = 'sell'
        elif direction == 'short' and type == 'close':
            trade_direction = 'buy'


        for lp in range(self.attempts):         
            curr_pos = self.r.get('{}_current_pos'.format(self.symbol)).decode()

            if curr_pos == "NONE" and type=='close': #to fix issue caused by backtrader verification idk why tho.
                print("Had to manually prevent close order")
                break
                
            
            if method == "attempt_limit":
                try:
                    order, limit_price = self.send_limit_order(type, trade_direction)

                    while len(order) == 0:
                        order, limit_price = self.send_limit_order(type, trade_direction)

                    while True:
                        order = self.exchange.fetch_order(order['info']['id'])
                        order_status = order['info']['size']
                        filled_string = order['info']['filledSize']

                        if order_status != filled_string:
                            time.sleep(.5) 
                            orderbook = self.get_orderbook()
                            print("Best Bid is {} and Best Ask is {}".format(orderbook['best_ask'], orderbook['best_bid']))

                            if trade_direction == 'buy':
                                current_match = orderbook['best_bid']

                                if current_match >= (limit_price + self.increment):
                                    print("Current price is much better, closing to open new one")
                                    self.close_open_orders()

                                    order, limit_price = self.send_limit_order(type, trade_direction)

                                    while len(order) == 0:
                                        order, limit_price = self.send_limit_order(type, trade_direction)

                            elif trade_direction == 'sell':
                                current_match = orderbook['best_ask']

                                if current_match <= (limit_price - self.increment):
                                    print("Current price is much better, closing to open new one")
                                    self.close_open_orders()
                                    order, limit_price = self.send_limit_order(type, trade_direction)
                                    
                                    while len(order) == 0:
                                        order, limit_price = self.send_limit_order(type, trade_direction)
                        else:
                            print("Order has been filled. Exiting out of loop")
                            self.close_open_orders()
                            self.set_position()
                            return

                    

                except ccxt.BaseError as e:
                    print(e)
                    pass
            elif method == "5sec_average":
                self.second_average(12, 4.8, type, direction, trade_direction)
                self.set_position()
                return
            elif method == "10sec_average":
                self.second_average(12, 9.8, type, direction, trade_direction)
                self.set_position()
                return
            elif method == "1min_average":
                self.second_average(12, 60, type, direction, trade_direction)
                self.set_position()
                return
            elif method == "10min_average":
                self.second_average(12, 600, type, direction, trade_direction)
                self.set_position()
                return
            elif method == "now":
                amount, price = self.get_max_amount(type)
                order = self.market_trade(trade_direction, amount)
                self.set_position()
                return