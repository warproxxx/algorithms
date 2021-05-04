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

r = redis.Redis(host='localhost', port=6379, db=0)

async def daddy_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):
    pass           

async def daddy_book(feed, pair, book, timestamp, receipt_timestamp):
    if float(r.get('daddy_enabled').decode()) == 1:
        bid = float(list(book[BID].keys())[-1])
        ask = float(list(book[ASK].keys())[0])

        r.set('{}_{}_best_bid'.format(feed.lower(), pair.lower()), bid)
        r.set('{}_{}_best_ask'.format(feed.lower(), pair.lower()), ask)

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

    def print(self, to_print):
        utils_print(to_print, symbol=self.symbol)

    def after_stuffs(self, name):
        lt = self.lts[name]

        try:
            lt.set_position()
        except Exception as e:
            self.print("Error in setting pos in {}".format(name))
            
        lt.update_parameters()

        current_pos = self.r.get('{}_current_pos'.format(name)).decode()

        if current_pos == 'NONE':
            lt.close_stop_order()
        else:
            lt.update_stop()

    def perform_backtrade_verification(self, details, analysis):
        lts = self.lts

        if details['trade'] == 1:
            lt = lts[details['name']]
            
            current_pos, avgEntryPrice, _ = lt.get_position()
            obook = lt.get_orderbook()
            position_since = float(self.r.get('{}_position_since'.format(details['name'])).decode())

            try:
                pnl_percentage = ((obook['best_bid'] - avgEntryPrice)/avgEntryPrice) * 100 * float(lt.parameters['mult'])
            except:
                pnl_percentage = 0

            self.print("\nExchange      : {}\nAvg Entry     : {}\nPnL Percentage: {}%\nPosition Since: {}".format(details['name'], avgEntryPrice, round(pnl_percentage,2), position_since))
            
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

        #so this isn't same for some reason
        self.print("Symbol:{} Backtest Date: {} Save file name: {}".format(self.symbol, backtest_date, save_file_name))
        if backtest_date == save_file_name:
            for idx, details in EXCHANGES.iterrows():
                if details['trade'] == 1:
                    backtest_thread[details['name']] = threading.Thread(target=self.perform_backtrade_verification, args=(details, analysis, ))
                    backtest_thread[details['name']].start()

    def call_every(self):
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