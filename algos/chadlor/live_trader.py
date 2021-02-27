import ccxt
from algos.daddy.huobi.HuobiDMService import HuobiDM

import os
import time
import numpy as np
import json
import pandas as pd
import redis
import datetime
import decimal
import inspect
from algos.daddy.defines import trade_methods
import sys
from utils import print


def round_down(value, decimals):
    with decimal.localcontext() as ctx:
        d = decimal.Decimal(value)
        ctx.rounding = decimal.ROUND_DOWN
        return float(round(d, decimals))

        
class liveTrading():
    def __init__(self, symbol='BTC/USD', testnet=True):
        self.parameters = json.load(open('algos/chadlor/parameters.json'))
        self.lev = self.parameters['mult']
        self.symbol_here = ""
        self.symbol = symbol
        self.attempts = 5
        self.increment = 0.5
        self.name = "chadlor"

        apiKey = os.getenv('BITMEX_CHADLOR_ID')
        apiSecret = os.getenv('BITMEX_CHADLOR_SECRET')


        self.exchange = ccxt.bitmex({
                            'apiKey': apiKey,
                            'secret': apiSecret,
                            'enableRateLimit': True,
                        })
        
        if symbol == "BTC/USD":
            self.symbol_here = "XBTUSD"

        self.r = redis.Redis(host='localhost', port=6379, db=0)            
        self.update_parameters()
    
    def update_parameters(self):
        self.parameters = json.load(open('algos/chadlor/parameters.json'))
        self.lev = self.parameters['mult']
        count = 0
        
        while count < 5:
            try:
                stats = self.exchange.private_post_position_leverage({"symbol": self.symbol_here, "leverage": str(self.lev)})
                break

            except ccxt.BaseError as e:

                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break
                
                if ("insufficient Available Balance" in str(e)):
                    break
                
                count = count + 1
            
    def close_open_orders(self, close_stop=False):
        self.update_parameters()
        
        for lp in range(self.attempts):
            try:

                orders = self.exchange.fetch_open_orders()

        
                if len(orders) > 0:
                    for order in orders:
                        if close_stop == True:
                            self.exchange.cancel_order(order['info']['orderID'])
                            print("Closing Order: {}".format(order['info']['orderID']))
                        else:
                            if order['info']['ordType'] != 'Stop':
                                self.exchange.cancel_order(order['info']['orderID'])
                                print("Closing Order: {}".format(order['info']['orderID']))
                            
                break
            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break

                print(e)
                pass

    def close_stop_order(self):
        self.close_open_orders(close_stop=True)

    def get_orderbook(self):
        book = self.exchange.fetch_order_book(self.symbol)
        orderbook = {}
        orderbook['best_ask'] = book['asks'][0][0]
        orderbook['best_bid'] = book['bids'][0][0]

        return orderbook

    def get_position(self):
        '''
        Returns position (LONG, SHORT, NONE), average entry price and current quantity
        '''

        for lp in range(self.attempts):
            try:
                pos = self.exchange.private_get_position()
                if len(pos) == 0:
                    return 'NONE', 0, 0
                else:
                    pos = pos[0]

                    #try catch because bitmex return old position
                    try:
                        if pos['currentQty'] < 0:
                            current_pos = "SHORT"
                        else:
                            current_pos = "LONG"

                        return current_pos, float(pos['avgEntryPrice']), float(pos['currentQty'])
                    except:
                        return 'NONE', 0, 0

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

                if current_pos == 'NONE':
                    self.r.set('{}_position_since'.format(self.name), 0)

                try:
                    self.r.get('{}_position_since'.format(self.name)).decode()
                except:
                    print("Error getting position since. Setting to ten")
                    self.r.set('{}_position_since'.format(self.name), 10)
        
                self.r.set('{}_avgEntryPrice'.format(self.name), avgEntryPrice)
                self.r.set('{}_current_pos'.format(self.name), current_pos)
                self.r.set('{}_pos_size'.format(self.name), amount)

                balance = self.actually_get_balance()
                self.r.set('{}_balance'.format(self.name), balance)
                break

            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break

                print(e)
                time.sleep(1)
                pass
            except Exception as e:
                print(e)


    def get_stop(self):
        start_time = time.time()

        for lp in range(self.attempts):
            try:
                orders = self.exchange.fetch_open_orders()

                if len(orders) > 0:
                    for order in orders:
                        if order['info']['ordType'] == 'Stop':
                            return [order['info']['stopPx']]
                    
                    return []
                else:
                    return []
            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break

                print(e)
                time.sleep(1)
                pass

    def add_stop_loss(self):
        for lp in range(self.attempts):
            try:
                current_pos, avgEntryPrice, amount = self.get_position()
                close_at = int(avgEntryPrice * self.parameters['stop_percentage'])


                params = {
                    'stopPx': close_at,
                    'execInst': 'LastPrice'
                    }
                
                order = self.exchange.create_order(self.symbol, "Stop", "Sell", amount, None, params)
                return order
                break
            except Exception as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break
                
                print(str(e))
                pass

    def update_stop(self):
        current_pos = self.r.get('{}_current_pos'.format(self.name)).decode()

        if current_pos == "LONG":
            stop = self.get_stop()
            if len(stop) == 0:
                self.add_stop_loss()
            else:
                pos, entryPrice, amount = self.get_position()
                close_at = int(entryPrice * self.parameters['stop_percentage'])

                ratio = float(stop[0]) / close_at
    
                if (ratio <= 1.01 and ratio >= 0.99):
                    pass
                else:
                    print("Removing stop at {} to add stop at {}".format(stop[0], close_at))
                    self.close_stop_order()
                    self.add_stop_loss()

    def actually_get_balance(self):
        symbol_only = self.symbol.split("/")[0]
        return float(self.exchange.fetch_balance()['free'][symbol_only])


    def get_balance(self):
        actual_balance = self.actually_get_balance()
        return actual_balance
        
    def get_max_amount(self, order_type):
        '''
        Get the max buyable/sellable amount
        '''
        orderbook = self.get_orderbook()

        if order_type == 'buy':
            price = orderbook['best_ask'] - self.increment
            balance = self.get_balance()
            amount = int(balance * self.lev * price * .95)
            return amount, price

        elif order_type == 'sell':
            price = orderbook['best_bid'] + self.increment
            current_pos, avgEntryPrice, amount = self.get_position()
            return float(amount), float(price)

    def market_trade(self, order_type, amount):
        '''
        Performs market trade detecting exchange for the given amount
        '''

        if amount > 0:
            print("Sending market {} order for {} of size {} on {} in {}".format(order_type, self.symbol, amount, "BITMEX", datetime.datetime.now()))
            order = self.exchange.create_order(self.symbol, 'market', order_type, amount, None)
            return order
        else:
            print("Doing a zero trade")
            return []

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

            single_size = int(amount / intervals)     
            final_amount = int(amount - (single_size * (intervals - 1)))

            trading_array = [single_size] * (intervals - 1)
            trading_array.append(final_amount)
        
        for amount in trading_array:
            order = self.market_trade(order_type, amount) 
            time.sleep(sleep_time)

        current_pos, avgEntryPrice, amount = self.get_position()

        if current_pos == 'LONG':
            if self.threshold_tiggered == False:
                amount, price = self.get_max_amount(order_type)
                order = self.market_trade(order_type, amount)

        

    def fill_order(self, order_type, method='ASAP'):
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
        # self.close_open_orders()

        for lp in range(self.attempts):
            if method == "5sec_average":
                self.second_average(12, 4.8, order_type)
                break
            elif method == "10sec_average":
                self.second_average(12, 9.8, order_type)
                break
            elif method == "now":
                amount, price = self.get_max_amount(order_type)
                order = self.market_trade(order_type, amount)
                break
            elif method == 'ASAP':
                amount, price = self.get_max_amount(order_type)

                if amount < 300000:
                    order = self.market_trade(order_type, amount)
                    break
                else:
                    number_of_orders = int(amount / 300000)
                    self.second_average(number_of_orders, 10, order_type)