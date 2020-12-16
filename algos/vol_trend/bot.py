import pandas as pd
import json

import time

import threading
import schedule

from algos.vol_trend.backtest import perform_backtests

def perform():
    perform_backtests()
    #then perform others



def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)

def vol_bot():
    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()