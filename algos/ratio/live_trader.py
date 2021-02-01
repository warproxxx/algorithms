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
        self.symbol_here = symbol.replace("BTC", "") + "/BTC"
        
        self.threshold_tiggered = False
        self.attempts = 5

        apiKey = os.getenv('binance_ratio_ID')
        apiSecret = os.getenv('binance_ratio_SECRET')
        self.r = redis.Redis(host='localhost', port=6379, db=0)   
    
        self.exchange = ccxt.binance({
                        'apiKey': apiKey,
                        'secret': apiSecret,
                        'enableRateLimit': True,
                        'options': {'defaultMarket': 'futures'}
                    })

        self.exchange.load_markets()
        self.min_notional_base = float(self.exchange.markets[self.symbol_here]['info']['filters'][2]['stepSize'])
        self.min_notional_quote = 0.0001

        self.round_step = len(str(self.min_notional_base).split('.')[1])

        if self.round_step == 1:
            self.round_step = 0


        self.method = "now"

        config = pd.read_csv('algos/ratio/config.csv')

        try:
            curr_config = config[config['name'] == self.symbol].iloc[0]
            self.method = curr_config['method']
        except:
            self.method = 'now'

        self.update_parameters()

    def transfer_to_subaccount(self, amount, symbol, destination='ISOLATED_MARGIN', source='SPOT', coin='BTC'):
        if amount > 0:
            print("Moving {} {} @ {} from {} to {}".format(amount, coin, symbol, source, destination))
            self.exchange.sapi_post_margin_isolated_transfer({'asset': coin, 'symbol': symbol, 'transFrom': source, 'transTo': destination, 'amount': amount})

    def update_parameters(self):
        try:
            config = pd.read_csv('algos/ratio/config.csv')
            curr_config = config[config['name'] == self.symbol].iloc[0]
            self.lev = int(curr_config['mult'])
        except:
            self.lev = 2

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
        book = self.exchange.fetch_order_book(self.symbol_here)

        orderbook['best_ask'] = book['asks'][0][0]
        orderbook['best_bid'] = book['bids'][0][0]

        return orderbook

    def get_avg_entry(self):
        trades = self.exchange.sapi_get_margin_mytrades({'symbol': self.symbol, 'isIsolated': 'TRUE'})
        trades_df = pd.DataFrame(trades)
        trades_df['price'] = trades_df['price'].astype(float)
        trades_df['qty'] = trades_df['qty'].astype(float)
        reverse_df = trades_df[::-1].reset_index(drop=True)

        select_df = pd.DataFrame()

        for idx, row in reverse_df.iterrows():
            if row['isBuyer'] != reverse_df.iloc[0]['isBuyer']:
                break

            select_df = select_df.append(row, ignore_index=True)

        return (select_df['price'] * select_df['qty']).sum()/(select_df['qty'].sum())


    def get_position(self):
        '''
        Returns position (LONG, SHORT, NONE), average entry price and current quantity
        '''
        for lp in range(self.attempts):
            try:
                
                threshold = self.min_notional_base * 3
                quote_threshold = self.min_notional_quote * 3


                details = self.exchange.sapi_get_margin_isolated_account({'symbols': self.symbol})['assets'][0]
                current_pos = "NONE"
                amount = 0
                avgEntryPrice = 0
                asset = ""

                if float(details['baseAsset']['borrowed']) <= threshold and float(details['quoteAsset']['borrowed']) > quote_threshold:
                    current_pos = "LONG"
                    amount = float(details['baseAsset']['free'])

                    if abs(amount) <= threshold:
                        return "NONE", 0, 0

                    asset = details['baseAsset']['asset']
                    avgEntryPrice = self.get_avg_entry()

                elif float(details['baseAsset']['borrowed']) > threshold and float(details['quoteAsset']['borrowed']) <= quote_threshold:
                    current_pos = "SHORT"
                    amount = float(details['baseAsset']['borrowed'])

                    if abs(amount) <= threshold:
                        return "NONE", 0, 0

                    asset = details['baseAsset']['asset']
                    avgEntryPrice = self.get_avg_entry()
                    
                amount = round_down(amount,self.round_step)
                
                return current_pos, avgEntryPrice, amount
            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break
            except:
                return "NONE", 0, 0

        return "NONE", 0, 0

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
            balances = pd.DataFrame(self.exchange.fetch_balance()['info']['balances'])
            return float(balances[balances['asset'] == 'BTC'].iloc[0]['free'])
        except:
            return 0

    def get_main_balance(self, symbol='BTC'):
        df = pd.DataFrame(self.exchange.fetch_balance()['info']['balances'])
        return float(df[df['asset'] == symbol].iloc[0]['free'])

    def get_subaccount_btc_balance(self, symbol):
        try:
            details = self.exchange.sapi_get_margin_isolated_account({'symbols': symbol})['assets'][0]
            balance = float(details['quoteAsset']['netAssetOfBtc']) + float(details['baseAsset']['netAssetOfBtc'])
            return balance
        except:
            return 0

    def get_subaccount_balance(self):
        #its equivalent in altcoin is get_balance
        try:
            return float(self.exchange.sapi_get_margin_isolated_account({'symbols': self.symbol})['assets'][0]['quoteAsset']['totalAsset'])
        except:
            return 0

    def get_max_amount(self, order_type):
        '''
        Get the max buyable/sellable amount
        '''
        orderbook = self.get_orderbook()

        if order_type == 'open':
            price = orderbook['best_ask']
            balance = self.get_subaccount_balance()
            amount = round_down(((balance * self.lev)/price) * 0.99, self.round_step)
            return float(abs(amount)),float(abs(price))

        elif order_type == 'close':
            price = orderbook['best_bid']
            current_pos, avgEntryPrice, amount = self.get_position()
            return float(abs(amount)), float(price)

    def market_trade(self, trade_direction, amount, side_effect):
        '''
        Performs market trade detecting exchange for the given amount
        '''

        print("Sending market {} order for {} of size {} in {}".format(trade_direction, self.symbol, amount, datetime.datetime.now()))
        order = self.exchange.sapi_post_margin_order({'symbol': self.symbol, 'isIsolated': 'TRUE', 'side': trade_direction.upper(), 'type': 'MARKET', 'quantity': amount, 'sideEffectType': side_effect})
        return order

    
    def second_average(self, intervals, sleep_time, type, direction, trade_direction, side_effect):
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
            amount = round_down(amount, self.round_step)
            order = self.market_trade(trade_direction, amount, side_effect)
            time.sleep(sleep_time)

        if self.threshold_tiggered == False:
            amount, price = self.get_max_amount(type)
            order = self.market_trade(trade_direction, amount, side_effect)

    def trade_now(self, type, trade_direction, side_effect):
        amount, price = self.get_max_amount(type)
        order = self.market_trade(trade_direction, amount, side_effect)
        self.set_position()

    def fill_order(self, type, direction):
        '''
        Parameters:
        ___________

        type (string):
        open or close

        direction (string):
        long or short

        5sec_average: Divides into 24 parts and makes market order of that every 5 second
        now: Market buy instantly

        '''

        #it seems for closing, i need run it twice just in case
        method = self.method

        self.set_position()
        print("Time at filling order is: {}".format(datetime.datetime.now()))

        if direction == 'long' and type == 'close':
            trade_direction = 'sell'
            side_effect = 'AUTO_REPAY'
        elif direction == 'long' and type == 'open':
            trade_direction = 'buy'
            side_effect = 'MARGIN_BUY'
        elif direction == 'short' and type == 'open':
            trade_direction = 'sell'
            side_effect = 'MARGIN_BUY'
        elif direction == 'short' and type == 'close':
            trade_direction = 'buy'
            side_effect = 'AUTO_REPAY'


        for lp in range(self.attempts):         
            curr_pos = self.r.get('{}_current_pos'.format(self.symbol)).decode()

            if curr_pos == "NONE" and type=='close': #to fix issue caused by backtrader verification idk why tho.
                print("Had to manually prevent close order")
                break
                
            
            if method == "5sec_average":
                self.second_average(12, 4.8, type, direction, trade_direction, side_effect)
                self.set_position()
            elif method == "10sec_average":
                self.second_average(12, 9.8, type, direction, trade_direction, side_effect)
                self.set_position()
            elif method == "1min_average":
                self.second_average(12, 60, type, direction, trade_direction, side_effect)
                self.set_position()
            elif method == "10min_average":
                self.second_average(12, 600, type, direction, trade_direction, side_effect)
                self.set_position()
            elif method == "now":
                self.trade_now(type, trade_direction, side_effect)
            

            if type == "close":
                try:
                    self.trade_now(type, trade_direction, side_effect)
                except Exception as e:
                    print(str(e))

            return