from cryptofeed.feedhandler import FeedHandler
from cryptofeed.exchanges import BinanceFutures, Bitmex, OKEx, Bybit, HuobiSwap, FTX
from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK,TICKER
import redis
import pandas as pd
import os
import time

async def ticker_call(feed, pair, bid, ask, timestamp, receipt_timestamp):  
    print(feed, pair, bid, ask, timestamp, receipt_timestamp)

f = FeedHandler(retries=100000)
f.add_feed(BinanceFutures(pairs=['BTC-USDT'],channels=[TICKER], callbacks={TICKER: [ticker_call]}), timeout=-1)


f.run()