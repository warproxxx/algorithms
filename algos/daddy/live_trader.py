import ccxt
from huobi.HuobiDMService import HuobiDM

import os
import time
import numpy as np
import json
import pandas as pd
import redis
import datetime
import decimal
import inspect
from defines import trade_methods
import sys
from utils import print


def round_down(value, decimals):
    with decimal.localcontext() as ctx:
        d = decimal.Decimal(value)
        ctx.rounding = decimal.ROUND_DOWN
        return float(round(d, decimals))

        
class liveTrading():
    def __init__(self, exchange, symbol='BTC/USD', testnet=True):
        self.symbol = symbol
        self.parameters = json.load(open('parameters.json'))
        self.lev = self.parameters['mult']
        self.symbol_here = ""
        self.exchange_name = exchange
        self.threshold_tiggered = False
        self.attempts = 5
        
        if exchange == 'bitmex':
            if testnet == True:
                apiKey = os.getenv('BITMEX_TESTNET_ID')
                apiSecret = os.getenv('BITMEX_TESTNET_SECRET')
            else:
                apiKey = os.getenv('BITMEX_ID')
                apiSecret = os.getenv('BITMEX_SECRET')

            self.exchange = ccxt.bitmex({
                            'apiKey': apiKey,
                            'secret': apiSecret,
                            'enableRateLimit': True,
                        })
            
            if testnet == True:
                self.exchange.urls['api'] = self.exchange.urls['test']
            
            if symbol == "BTC/USD":
                self.symbol_here = "XBTUSD"

            self.increment = .5

        elif exchange == 'binance_futures':
            if testnet == True:
                apiKey = os.getenv('BINANCE_TESTNET_ID')
                apiSecret = os.getenv('BINANCE_TESTNET_SECRET')
            else:
                apiKey = os.getenv('BINANCE_ID')
                apiSecret = os.getenv('BINANCE_SECRET')

            self.exchange = ccxt.binance({
                            'apiKey': apiKey,
                            'secret': apiSecret,
                            'enableRateLimit': True,
                        })
            
            if testnet == True:
                self.exchange.urls['api'] = self.exchange.urls['test']
            
            if symbol == "BTC/USDT":
                self.symbol_here = "BTCUSDT"

            self.increment = .1
        elif exchange == 'bybit':
            if testnet == True:
                apiKey = os.getenv('BYBIT_TESTNET_ID')
                apiSecret = os.getenv('BYBIT_TESTNET_SECRET')
            else:
                apiKey = os.getenv('BYBIT_ID')
                apiSecret = os.getenv('BYBIT_SECRET')

            self.exchange = ccxt.bybit({
                            'apiKey': apiKey,
                            'secret': apiSecret,
                            'enableRateLimit': True,
                        })
            
            if testnet == True:
                self.exchange.urls['api'] = self.exchange.urls['test']
            
            if symbol == "BTC/USD":
                self.symbol_here = "BTCUSD"

            self.increment = .5
        elif exchange == 'ftx':
            if testnet == True:
                sys.exit("Testnet is not available for this exchange")
            else:
                apiKey = os.getenv('FTX_ID')
                apiSecret = os.getenv('FTX_SECRET')

            
            self.exchange = ccxt.ftx({
                            'apiKey': apiKey,
                            'secret': apiSecret,
                            'enableRateLimit': True,
                            'options': {'defaultMarket': 'futures'}
                        })
                    
            if symbol == "BTC-PERP":
                self.symbol_here = "BTC-PERP"

            self.increment = 0.5
        elif exchange == 'okex':
            if testnet == True:
                sys.exit("Testnet is not available for this exchange")
            else:
                apiKey = os.getenv('OKEX_ID')
                apiSecret = os.getenv('OKEX_SECRET')
                password = os.getenv('OKEX_PASSWORD')

            
            self.exchange = ccxt.okex({
                            'apiKey': apiKey,
                            'secret': apiSecret,
                            'password': password,
                            'enableRateLimit': True                        
                        })

            if self.symbol == "BTC-USD-SWAP":
                self.symbol_here = "BTC-USD-SWAP"

            self.increment = 0.1
        elif exchange == 'huobi_swap':
            if testnet == True:
                sys.exit("Testnet is not available for this exchange")
            else:
                apiKey = os.getenv('HUOBI_ID')
                apiSecret = os.getenv('HUOBI_SECRET')
                self.exchange = HuobiDM("https://api.hbdm.com", apiKey, apiSecret)

            if self.symbol == "BTC-USD":
                self.symbol_here = "BTC-USD"

            self.increment = 0.1
            self.lev = 20
                        

        self.r = redis.Redis(host='localhost', port=6379, db=0)            
        self.update_parameters()
    
    def update_parameters(self):
        self.parameters = json.load(open('parameters.json'))
        self.lev = self.parameters['mult']
        count = 0
        
        while count < 5:
            try:
                if self.exchange_name == 'bitmex':
                    stats = self.exchange.private_post_position_leverage({"symbol": self.symbol_here, "leverage": str(self.lev)})
                    break
                elif self.exchange_name == 'binance_futures':
                    stats = self.exchange.fapiPrivate_post_leverage({"symbol": self.symbol_here, "leverage": str(self.lev)})
                    break
                elif self.exchange_name == 'bybit':
                    stats = self.exchange.user_post_leverage_save({"symbol": self.symbol_here, "leverage": str(self.lev)})
                elif self.exchange_name == 'ftx':
                    try:
                        stats = self.exchange.private_post_account_leverage({"leverage": 20}) #only allows [1,3,5,10,20,50,100]
                        break
                    except Exception as e:
                        break
                elif self.exchange_name == 'okex':
                    try:
                        stats = self.exchange.swapPostAccountsInstrumentIdLeverage({'instrument_id': self.symbol, 'leverage':str(self.lev), 'side':'3' })
                        break
                    except Exception as e:
                        break
                elif self.exchange_name == 'huobi_swap':
                    self.lev = 20 #only allows some
                    try:
                        stats = self.exchange.send_post_request('/swap-api/v1/swap_switch_lever_rate', {'contract_code': self.symbol, 'lever_rate': self.lev}) 
                        break
                    except Exception as e:
                        break

            except ccxt.BaseError as e:

                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break
                
                if self.exchange_name == 'bitmex':
                    if ("insufficient Available Balance" in str(e)):
                        break
                elif self.exchange_name == 'binance_futures':
                    if ("insufficient" in str(e)):
                        break
                elif self.exchange_name == 'bybit':
                    if ("same to the old" in str(e)):
                        break
                    if ("balance not enough" in str(e)):
                        break
                
                count = count + 1
            
    def close_open_orders(self, close_stop=False):
        self.update_parameters()
        
        for lp in range(self.attempts):
            try:

                if self.exchange_name == 'bitmex':
                    orders = self.exchange.fetch_open_orders()
                elif self.exchange_name == 'binance_futures':
                    orders = self.exchange.fapiPrivate_get_openorders()
                elif self.exchange_name == 'bybit':
                    self.exchange.cancel_all_orders(symbol=self.symbol)

                    if close_stop == True:
                        orders = self.exchange.openapi_get_stop_order_list()['result']['data']
                    else:
                        orders = self.exchange.fetch_open_orders()
                elif self.exchange_name == 'ftx':
                    if close_stop == True:
                        self.exchange.cancel_all_orders()

                    orders = self.exchange.fetch_open_orders()

                elif self.exchange_name == 'okex':

                    if close_stop == True:
                        stop_orders = self.exchange.swap_get_order_algo_instrument_id({'instrument_id': self.symbol, 'order_type': "1", "status": "1"})['orderStrategyVOS']

                        for order in stop_orders:
                            self.exchange.swap_post_cancel_algos({'instrument_id': self.symbol, "order_type": "1", "algo_ids": [order['algo_id']]})

                    orders = self.exchange.swap_get_orders_instrument_id({'instrument_id': self.symbol, 'state': '0'})['order_info']
                elif self.exchange_name == 'huobi_swap':
                    if close_stop == True:
                        self.exchange.send_post_request('/swap-api/v1/swap_trigger_cancelall', {'contract_code': self.symbol})

                    self.exchange.send_post_request('/swap-api/v1/swap_cancelall', {'contract_code': self.symbol})
                    orders = []

        
                if len(orders) > 0:
                    for order in orders:
                        if self.exchange_name == 'bitmex':
                            if close_stop == True:
                                self.exchange.cancel_order(order['info']['orderID'])
                                print("Closing Order: {}".format(order['info']['orderID']))
                            else:
                                if order['info']['ordType'] != 'Stop':
                                    self.exchange.cancel_order(order['info']['orderID'])
                                    print("Closing Order: {}".format(order['info']['orderID']))
                                    
                        elif self.exchange_name == 'binance_futures':
                            if close_stop == True:
                                self.exchange.fapiPrivate_delete_order(order)
                                print("Closing Order: {}".format(order['orderId']))
                            else:
                                if order['origType'] != 'STOP_MARKET':
                                    self.exchange.fapiPrivate_delete_order(order)
                                    print("Closing Order: {}".format(order['orderId']))
                        elif self.exchange_name == 'bybit':
                            if order['stop_order_status'] == 'Untriggered':
                                self.exchange.openapi_post_stop_order_cancel(params={'stop_order_id': order['stop_order_id']})
                        elif self.exchange_name == 'ftx':
                            self.exchange.cancel_order(order['info']['id'])
                        elif self.exchange_name == 'okex':
                            self.exchange.swap_post_cancel_order_instrument_id_order_id({'instrument_id': self.symbol, 'order_id': order['order_id']})
                
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
        orderbook = {}
        orderbook['best_ask'] = float(self.r.get('{}_best_ask'.format(self.exchange_name)).decode())
        orderbook['best_bid'] = float(self.r.get('{}_best_bid'.format(self.exchange_name)).decode())

        return orderbook

    def get_position(self):
        '''
        Returns position (LONG, SHORT, NONE), average entry price and current quantity
        '''

        for lp in range(self.attempts):
            try:
                if self.exchange_name == 'bitmex':

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

                elif self.exchange_name == 'binance_futures':
                    pos = pd.DataFrame(self.exchange.fapiPrivate_get_positionrisk())
                    pos = pos[pos['symbol'] == self.symbol_here].iloc[0]

                    if float(pos['positionAmt']) == 0:
                        return 'NONE', 0, 0
                    else:
                        if float(pos['positionAmt']) < 0:
                            current_pos = "SHORT"
                        else:
                            current_pos = "LONG"

                    return current_pos, float(pos['entryPrice']), float(pos['positionAmt'])

                elif self.exchange_name == 'bybit':
                    pos = self.exchange.private_get_position_list(params={'symbol': self.symbol_here})['result']

                    if float(pos['size']) == 0:
                        return 'NONE', 0, 0
                    else:
                        if float(pos['size']) < 0:
                            current_pos = "SHORT"
                        else:
                            current_pos = "LONG"

                    return current_pos, float(pos['entry_price']), float(pos['size'])
                elif self.exchange_name == 'ftx':
                    pos = pd.DataFrame(self.exchange.private_get_positions(params={'showAvgPrice': True})['result'])
                    pos = pos[pos['future'] == self.symbol_here].iloc[0]

                    if float(pos['openSize']) == 0:
                        return 'NONE', 0, 0

                    if float(pos['openSize']) > 0:
                        current_pos = "LONG"
                    elif float(pos['openSize']) < 0:
                        current_pos = "SHORT" 
                    
                    return current_pos, float(pos['recentAverageOpenPrice']), float(pos['openSize'])
                elif self.exchange_name == 'okex':
                    pos = self.exchange.swap_get_position()

                    if len(pos) > 0:
                        pos = pd.DataFrame(pos[0]['holding'])
                        pos = pos[pos['instrument_id'] == self.symbol_here].iloc[0]

                        return "LONG", float(pos['avg_cost']), int(pos['avail_position'])
                    else:
                        return 'NONE', 0, 0
                elif self.exchange_name == 'huobi_swap':
                    pos = pd.DataFrame(self.exchange.send_post_request('/swap-api/v1/swap_position_info', {'contract_code': self.symbol})['data'])
                    if len(pos) > 0:
                        pos = pos[pos['contract_code'] == self.symbol_here].iloc[0]
                        return "LONG", float(pos['cost_open']), int(pos['available'])
                    else:
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
                    self.r.set('{}_position_since'.format(self.exchange_name), 0)

                try:
                    self.r.get('{}_position_since'.format(self.exchange_name)).decode()
                except:
                    print("Error getting position since. Setting to ten")
                    self.r.set('{}_position_since'.format(self.exchange_name), 10)
        
                self.r.set('{}_avgEntryPrice'.format(self.exchange_name), avgEntryPrice)
                self.r.set('{}_current_pos'.format(self.exchange_name), current_pos)
                self.r.set('{}_pos_size'.format(self.exchange_name), amount)

                balance = self.actually_get_balance()
                self.r.set('{}_balance'.format(self.exchange_name), balance)
                break

            except ccxt.BaseError as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break

                print(e)
                time.sleep(1)
                pass

    def get_stop(self):
        start_time = time.time()

        for lp in range(self.attempts):
            try:
                if self.exchange_name == 'bitmex':
                    orders = self.exchange.fetch_open_orders()
                elif self.exchange_name == 'binance_futures':
                    orders = self.exchange.fapiPrivate_get_openorders()
                elif self.exchange_name == 'bybit':
                    orders = self.exchange.openapi_get_stop_order_list()['result']['data']
                elif self.exchange_name == 'ftx':
                    orders = self.exchange.request('conditional_orders', api='private', method='GET', params={'market': self.symbol_here})['result']
                elif self.exchange_name == 'okex':
                    orders = self.exchange.swap_get_order_algo_instrument_id({'instrument_id': self.symbol, 'order_type': "1", "status": "1"})['orderStrategyVOS']
                elif self.exchange_name == 'huobi_swap':
                    orders = self.exchange.send_post_request('/swap-api/v1/swap_trigger_openorders', {'contract_code': self.symbol})['data']['orders']

                if len(orders) > 0:
                    for order in orders:
                        if self.exchange_name == 'bitmex':
                            if order['info']['ordType'] == 'Stop':
                                return [order['info']['stopPx']]
                        elif self.exchange_name == 'binance_futures':
                            if order['origType'] == 'STOP_MARKET':
                                return [order['stopPrice']]
                        elif self.exchange_name == 'bybit':
                            if order['stop_order_status'] == 'Untriggered':
                                return [order['stop_px']]
                        elif self.exchange_name == 'ftx':
                            if order['triggeredAt'] == None:
                                return [order['triggerPrice']]
                        elif self.exchange_name == 'okex':
                            return [order['trigger_price']]
                        elif self.exchange_name == 'huobi_swap':
                            return [order['trigger_price']]
                    
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


                if self.exchange_name == 'bitmex':
                    params = {
                        'stopPx': close_at,
                        'execInst': 'LastPrice'
                        }
                    
                    order = self.exchange.create_order(self.symbol, "Stop", "Sell", amount, None, params)
                    return order
                    break
                elif self.exchange_name == 'binance_futures':
                    params = {
                        'workingType': 'CONTRACT_PRICE'
                        }

                    order = self.exchange.fapiPrivatePostOrder({'symbol': self.symbol_here, 'type': 'STOP_MARKET', 'side': 'SELL', 'stopPrice': close_at, 'quantity': str(amount), 'params': params})
                    return order
                    break
                elif self.exchange_name == 'bybit':
                    order = self.exchange.openapi_post_stop_order_create({"order_type":"Market","side":"Sell","symbol":self.symbol_here,"qty":int(amount),"base_price":close_at,"stop_px":close_at,"time_in_force":"GoodTillCancel","reduce_only":True,"trigger_by":'LastPrice'})['result']
                    return order
                elif self.exchange_name == 'ftx':
                    params = {
                        'triggerPrice': close_at
                    }

                    order = self.exchange.create_order(self.symbol, "stop", "sell", amount, None, params)
                    return order
                elif self.exchange_name == 'okex':
                    order = self.exchange.swap_post_order_algo({'instrument_id': self.symbol, 'type': '3', 'order_type': '1', 'size': str(amount), 'algo_type': "2", "trigger_price": str(close_at)})
                    return order
                elif self.exchange_name == 'huobi_swap':
                    order = self.exchange.send_post_request('/swap-api/v1/swap_trigger_order', {'contract_code': self.symbol, 'trigger_type': 'le', 'trigger_price': close_at, 'order_price': close_at-1000, 'volume': amount, 'direction': 'sell', 'offset': 'close'})
                    return order
            except Exception as e:
                if "many requests" in str(e).lower():
                    print("Too many requests in {}".format(inspect.currentframe().f_code.co_name))
                    break
                
                print(str(e))
                pass

    def update_stop(self):
        current_pos = self.r.get('{}_current_pos'.format(self.exchange_name)).decode()

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
        if self.exchange_name == 'bitmex':
            symbol_only = self.symbol.split("/")[0]
            return float(self.exchange.fetch_balance()['free'][symbol_only])
        elif self.exchange_name == 'binance_futures':
            balance = pd.DataFrame(self.exchange.fapiPrivate_get_balance())
            balance = balance[balance['asset'] == 'USDT']

            if len(balance) > 0:
                free_balance = balance.iloc[0]['withdrawAvailable']
                return float(free_balance)
            else:
                return 0
        elif self.exchange_name == 'bybit':
            return float(self.exchange.fetch_balance()['info']['result']['BTC']['available_balance'])
        elif self.exchange_name == 'ftx':
            return float(self.exchange.fetch_balance()['USD']['free'])
        elif self.exchange_name == 'okex':
            return float(self.exchange.request('{}/accounts'.format(self.symbol), api='swap', method='GET')['info']['max_withdraw'])
        elif self.exchange_name == 'huobi_swap':
            return float(self.exchange.send_post_request('/swap-api/v1/swap_account_position_info', {'contract_code': self.symbol})['data'][0]['margin_available'])


    def get_balance(self):
        exchanges = pd.read_csv('exchanges.csv')
        balance_threshold = float(exchanges[exchanges['exchange'] == self.exchange_name].iloc[0]['max_trade'])
        actual_balance = self.actually_get_balance()

        if balance_threshold == 0:
            return actual_balance
        else:
            if actual_balance > balance_threshold:
                self.threshold_tiggered = True

            return min(actual_balance, balance_threshold)
        
    def get_max_amount(self, order_type):
        '''
        Get the max buyable/sellable amount
        '''
        orderbook = self.get_orderbook()

        if order_type == 'buy':
            price = orderbook['best_ask'] - self.increment
            balance = self.get_balance()

            if self.exchange_name == 'bitmex':
                amount = int(balance * self.lev * price * .95)
                return amount, price
            elif self.exchange_name == 'binance_futures':
                amount = round_down(((balance * self.lev)/price) * 0.97, 3)
                return amount, price
            elif self.exchange_name == 'bybit':
                amount = int(balance * self.lev * price * .96)
                return amount, price
            elif self.exchange_name == 'ftx':
                amount = round_down(((balance * self.lev)/price) * 0.97, 3)
                return amount, price
            elif self.exchange_name == 'okex':
                amount = int((balance * self.lev * price) // 100)
                return amount, round(price, 1)
            elif self.exchange_name == 'huobi_swap':
                amount = int((balance * self.lev * price) // 100)
                return amount, round(price, 1)

        elif order_type == 'sell':
            price = orderbook['best_bid'] + self.increment
            current_pos, avgEntryPrice, amount = self.get_position()

            if self.exchange_name == 'okex' or self.exchange_name == 'huobi_swap':
                return int(amount), float(round(price,1))
            else:
                return float(amount), float(price)

    #reached here
    def limit_trade(self, order_type, amount, price):
        '''
        Performs limit trade detecting exchange for the given amount
        '''
        if amount > 0:
            print("Sending limit {} order for {} of size {} @ {} on {} in {}".format(order_type, self.symbol, amount, price, self.exchange_name, datetime.datetime.now()))

            if self.exchange_name == 'bitmex':
                params = {
                            'execInst': 'ParticipateDoNotInitiate'
                        }

                order = self.exchange.create_order(self.symbol, 'limit', order_type, amount, price, params)
                
                if 'info' in order:
                    if 'text' in order['info']:
                        if "execInst of ParticipateDoNotInitiate" in order['info']['text']:
                            return []

                return order
            elif self.exchange_name == 'binance_futures':
                order = self.exchange.fapiPrivatePostOrder({'symbol': self.symbol_here, 'type': 'LIMIT', 'side': order_type.upper(),'price': price, 'quantity': str(amount), 'timeInForce': 'GTX'})

                if self.exchange.fapiPrivate_get_order(order)['status'] == 'EXPIRED':
                    return []

                return order

            elif self.exchange_name == 'bybit':
                params = {
                            'time_in_force': 'PostOnly'
                }

                order = self.exchange.create_order(self.symbol, type='limit', side=order_type, amount=amount, price=price, params=params)
                
                try:
                    order_id = order['info']['order_id']
                except:
                    order_id = order['info'][0]['order_id']

                order = self.exchange.fetch_order(order_id, symbol=self.symbol)

                if order['info']['order_status'] == 'Cancelled':
                    return []

                return order
            elif self.exchange_name == 'ftx':

                params = {
                    'postOnly': True
                    }
                order = self.exchange.create_order(self.symbol, type="limit", side=order_type.lower(), amount=amount, price=price, params=params)
                order = self.exchange.fetch_order(order['info']['id'])

                if order['status'] == 'canceled':
                    return []

                return order
            elif self.exchange_name == 'okex':

                if order_type == 'buy':
                    order = self.exchange.swap_post_order({'instrument_id': self.symbol, 'size': str(amount), 'type': '1', 'price': str(price), 'order_type': 1})
                elif order_type == 'sell':
                    order = self.exchange.swap_post_order({'instrument_id': self.symbol, 'size': str(amount), 'type': '3', 'price': str(price), 'order_type': 1})

                order = self.exchange.swap_get_orders_instrument_id_order_id({'instrument_id': self.symbol, 'order_id': order['order_id']})

                if order['status'] == '-1':
                    return []
                
                return order
            elif self.exchange_name == 'huobi_swap':
                
                if order_type == 'buy':
                    order = self.exchange.send_post_request('/swap-api/v1/swap_order', {'contract_code': self.symbol, 'price': price, 'volume': int(amount), 'direction': 'buy', 'offset': 'open', 'order_price_type': 'post_only', 'lever_rate': self.lev})
                elif order_type == 'sell':
                    order = self.exchange.send_post_request('/swap-api/v1/swap_order', {'contract_code': self.symbol, 'price': price, 'volume': int(amount), 'direction': 'sell', 'offset': 'close', 'order_price_type': 'post_only', 'lever_rate': self.lev})

                try:
                    order_id = order['data']['order_id']
                except:
                    order_id = order['data'][0]['order_id']

                order = self.exchange.send_post_request('/swap-api/v1/swap_order_info', {'contract_code': self.symbol, 'order_id': order_id})

                if order['data'][0]['status'] == 7:
                    return []

                return order
        else:
            print("Doing a zero trade")
            return []

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

            if self.exchange_name == 'bitmex':
                order = self.exchange.create_order(self.symbol, 'market', order_type, amount, None)
                return order
            elif self.exchange_name == 'binance_futures':
                order = self.exchange.fapiPrivatePostOrder({'symbol': self.symbol_here, 'type': 'MARKET', 'side': order_type.upper(), 'quantity': str(amount)})
                return order
            elif self.exchange_name == 'bybit':
                order = self.exchange.create_order(self.symbol, 'market', order_type, amount, None)
                return order
            elif self.exchange_name == 'ftx':
                order = self.exchange.create_order(self.symbol, 'market', order_type.lower(), amount, None)
            elif self.exchange_name == 'okex':
                if order_type == 'buy':
                    order = self.exchange.swap_post_order({'instrument_id': self.symbol, 'size': int(amount), 'type': '1', 'order_type': 4})
                elif order_type == 'sell':
                    order = self.exchange.swap_post_order({'instrument_id': self.symbol, 'size': int(amount), 'type': '3', 'order_type': 4})
            elif self.exchange_name == 'huobi_swap':
                
                if order_type == 'buy':
                    order = self.exchange.send_post_request('/swap-api/v1/swap_order', {'contract_code': self.symbol, 'volume': int(amount), 'direction': 'buy', 'offset': 'open', 'order_price_type': 'optimal_20', 'lever_rate': int(self.lev)})
                elif order_type == 'sell':
                    order = self.exchange.send_post_request('/swap-api/v1/swap_order', {'contract_code': self.symbol, 'volume': int(amount), 'direction': 'sell', 'offset': 'close', 'order_price_type': 'optimal_20', 'lever_rate': int(self.lev)})
                
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

            if self.exchange_name == 'bitmex':
                single_size = int(amount / intervals)     
                final_amount = int(amount - (single_size * (intervals - 1)))

            elif self.exchange_name == 'binance_futures':
                single_size = round_down(amount / intervals, 3)
                final_amount = round_down(amount - (single_size * (intervals - 1)), 3)

            elif self.exchange_name == 'bybit':
                single_size = int(amount / intervals)     
                final_amount = int(amount - (single_size * (intervals - 1)))

            elif self.exchange_name == 'ftx':
                single_size = round_down(amount / intervals, 3)
                final_amount = round_down(amount - (single_size * (intervals - 1)), 3)
            elif self.exchange_name == 'okex' or self.exchange_name == 'huobi_swap':
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
        # self.close_open_orders()

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

                        if self.exchange_name == 'bitmex':
                            orderId = order['info']['orderID']
                            order = self.exchange.fetch_order(orderId)
                            order_status = order['info']['ordStatus']
                            filled_string = 'Filled'
                        elif self.exchange_name == 'binance_futures':
                            order = self.exchange.fapiPrivate_get_order(order)
                            order_status = order['status']
                            filled_string = 'FILLED'
                        elif self.exchange_name == 'bybit':
                            try:
                                order_id = order['info']['order_id']
                            except:
                                order_id = order['info'][0]['order_id']

                            order = self.exchange.fetch_order(order_id, symbol=self.symbol)
                            order_status = order['info']['order_status']
                            filled_string = 'Filled'
                        elif self.exchange_name == 'ftx':
                            order = self.exchange.fetch_order(order['info']['id'])
                            order_status = order['info']['size']
                            filled_string = order['info']['filledSize']
                        elif self.exchange_name == 'okex':
                            try:
                                order = self.exchange.swap_get_orders_instrument_id_order_id({'instrument_id': self.symbol, 'order_id': order['order_id']})
                                order_status = order['state']
                            except:
                                order_status = '0'

                            filled_string = '2'
                        elif self.exchange_name == 'huobi_swap':
                            try:
                                order_id = order['data']['order_id']
                            except:
                                order_id = order['data'][0]['order_id']
                            order = self.exchange.send_post_request('/swap-api/v1/swap_order_info', {'contract_code': self.symbol, 'order_id': order_id})
                            order_status = order['data'][0]['status']
                            filled_string = 6


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
