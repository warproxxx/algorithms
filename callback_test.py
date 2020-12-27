from cryptofeed.feedhandler import FeedHandler
from cryptofeed.exchanges import BinanceFutures, Bitmex, OKEx, Bybit, HuobiSwap, FTX
from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK
import redis
import pandas as pd
import os
import time

async def trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):  
    print(pair, 'trade')

async def trade2(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price):  
    print(pair, 'trade2')

f = FeedHandler(retries=100000)
f.add_feed(BinanceFutures(pairs=['BTC-USDT'],channels=[TRADES], callbacks={TRADES: [trade, trade2]}), timeout=-1)
f.add_feed(BinanceFutures(pairs=['ETH-USDT'],channels=[TRADES], callbacks={TRADES: [trade, trade2]}), timeout=-1)


f.run()