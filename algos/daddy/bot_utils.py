import os
import sys
from glob import glob
import requests

import time
import datetime

import pandas as pd
import numpy as np
import json

import threading

import ta
import redis
import schedule
import shutil

from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK

from algos.daddy.live_trader import liveTrading
from algos.daddy.plot import create_chart
from algos.daddy.trades import update_trades, run_backtest

from utils import print as utils_print

from algos.daddy.trade_analysis import get_trade_funding_data, process_data, get_details

r = redis.Redis(host='localhost', port=6379, db=0)

async def daddy_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):
    pass           

async def daddy_book(book, receipt_timestamp):

    bid = float(list(book.book[BID].keys())[-1])
    ask = float(list(book.book[ASK].keys())[0])

    print(book)

    r.set('{}_{}_best_bid'.format(book.feed.lower(), book.symbol.lower()), bid)
    r.set('{}_{}_best_ask'.format(book.feed.lower(), book.symbol.lower()), ask)

async def daddy_ticker(feed, pair, bid, ask, timestamp, receipt_timestamp):  
    pass

class daddyBot():
    def __init__(self, symbol, config_file, parameter_file, TESTNET):
        self.config_file = config_file
        self.parameter_file = parameter_file
        self.symbol = symbol

        self.EXCHANGES = pd.read_csv(config_file)
        self.r = redis.Redis(host='localhost', port=6379, db=0)

        self.lts = {}

        for idx, details in self.EXCHANGES.iterrows():
            exchange_name = details['exchange']
            name = details['name']

            if details['trade'] == 1:       
                self.lts[name] = liveTrading(exchange_name, name, symbol=details['ccxt_symbol'],testnet=TESTNET, parameter_file=parameter_file, config_file=config_file) 
                self.lts[name].set_position()
                self.stop_loss_work(name)

    def print(self, to_print):
        utils_print(to_print, symbol=self.symbol)

    def stop_loss_work(self, name):
        lt = self.lts[name]
        current_pos, avgEntryPrice, _ = lt.get_position()
        orders = lt.get_orders()

        if current_pos != "NONE":
            if len(orders) == 0:
                print("There is no stop in {}, adding it".format(name))
                lt.add_stop_loss()
            elif len(orders) > 1:
                print("There are multiple stops in {}. Removing to readd".format(name))
                lt.close_stop_order()
                lt.add_stop_loss()
            elif len(orders) == 1:
                lt.update_stop()
        else:
            if len(orders) == 1:
                print("There is an open stop without position in {}. Removing".format(name))
                lt.close_stop_order()

    def after_stuffs(self, name):
        lt = self.lts[name]

        try:
            lt.set_position()
        except Exception as e:
            self.print("Error in setting pos in {}".format(name))

        self.stop_loss_work(name)
        lt.update_parameters()

    def perform_backtrade_verification(self, details, analysis):
        lts = self.lts

        if details['trade'] == 1:
            lt = lts[details['name']]
            
            current_pos, avgEntryPrice, _ = lt.get_position()
            obook = lt.get_orderbook()

            try:
                position_since = float(self.r.get('{}_position_since'.format(details['name'])).decode())
            except:
                position_since = 0

            try:
                pnl_percentage = ((obook['best_bid'] - avgEntryPrice)/avgEntryPrice) * 100 * float(lt.parameters['mult'])
            except:
                pnl_percentage = 0

            self.print("\n{}:".format(datetime.datetime.utcnow()))
            self.print("Exchange      : {}\nAvg Entry     : {}\nPnL Percentage: {}%\nPosition Since: {}".format(details['name'], avgEntryPrice, round(pnl_percentage,2), position_since))

            if 'open' in analysis['total']:
                if analysis['total']['open'] == 1 and current_pos == "NONE":
                    self.print("Opening position from backtest_verification for {}".format(details['name']))
                    lt.fill_order('buy', method=details['buy_method'])
                    lt.add_stop_loss()
                elif analysis['total']['open'] == 0 and current_pos != "NONE":
                    self.print("Closed long position from backtest_verification for {}".format(details['name']))
                    lt.fill_order('sell', method=details['sell_method'])
                    lt.close_stop_order()
                else:
                    self.print("As required for {}".format(details['name']))
            else:
                self.print("As required for {}".format(details['name']))

            self.after_stuffs(details['name'])


    def process_trades(self):
        lts = self.lts
        self.EXCHANGES = pd.read_csv(self.config_file) #update exchanges
        EXCHANGES = self.EXCHANGES 

        analysis, backtest_date = run_backtest(self.symbol, self.parameter_file)
        save_file_name = self.r.get('{}_save_file_name'.format(self.symbol)).decode()
        backtest_date = pd.to_datetime(backtest_date) - pd.Timedelta(minutes=10)
        save_file_name = pd.to_datetime(save_file_name)

        backtest_thread = {}

        self.print("Symbol:{} Backtest Date: {} Save file name: {}".format(self.symbol, backtest_date, save_file_name))
        if backtest_date == save_file_name:
            for idx, details in EXCHANGES.iterrows():
                if details['trade'] == 1:
                    backtest_thread[details['name']] = threading.Thread(target=self.perform_backtrade_verification, args=(details, analysis, ))
                    backtest_thread[details['name']].start()

    def save_trades(self):
        for idx, row in self.EXCHANGES.iterrows():
            today = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")

            if row['trade'] == 1:     
                try:
                    trades_file = "data/{}_{}_trades.csv".format(today, row['name'])
                    funding_file = "data/{}_{}_fundings.csv".format(today, row['name'])
                    json_file = "data/{}_{}_summary.json".format(today, row['name'])

                    if not os.path.isfile(trades_file):
                        trades, fundings = get_trade_funding_data(row)
                        trades = process_data(trades, row)
                        summary, print_trades, buys, sells = get_details(trades, fundings)
                        print_trades.to_csv(trades_file, index=None)
                        fundings.to_csv(funding_file, index=None)
                        json.dump(summary, open(json_file, 'w'))
                except:
                    pass
            
            time.sleep(60 * 60)

    def call_every(self):
        save_thread = threading.Thread(target=self.save_trades)
        save_thread.start()
        
        while True:
            time.sleep(1)

            curr_time = datetime.datetime.utcnow()

            df = pd.DataFrame(pd.Series({'Time': curr_time})).T
            save_file = str(df.groupby(pd.Grouper(key='Time', freq="10Min", label='left')).sum().index[0])
            unique_save_file = save_file + "_" + self.symbol

            timestamp = pd.to_datetime(curr_time)
            current_full_time = str(timestamp.minute)
            current_time_check = current_full_time[1:]

            if current_full_time == '8' or current_time_check == '8':
                self.r.set('{}_save_file_name'.format(self.symbol), save_file)
                if (self.r.get(unique_save_file) == None):
                    self.r.set(unique_save_file, 1)
                    self.print("\n\033[1m" + str(datetime.datetime.utcnow()) + "\033[0;0m:")
                    self.process_trades()

def start_bot(symbol, TESTNET, config_file, parameter_file):
    update_trades(symbol=symbol)

    bot = daddyBot(symbol=symbol, config_file=config_file, parameter_file=parameter_file, TESTNET=TESTNET)

    call_every_thread = threading.Thread(target=bot.call_every())
    call_every_thread.start()