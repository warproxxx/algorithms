from cryptofeed.defines import TRADES
from cryptofeed.feedhandler import FeedHandler
from cryptofeed.exchanges import Coinbase, Bitmex
from cryptofeed.callback import Callback
import threading

from algos.chadlor.bot import chadlor_bot, chadlor_trade

import os


if __name__ == "__main__":
    if os.path.isfile("logs/chadlor_bot.log"):
        os.remove("logs/chadlor_bot.log")

    bot_thread = threading.Thread(target=chadlor_bot)
    bot_thread.start()

    f = FeedHandler(retries=100000)
    f.add_feed(Bitmex(pairs=['XBTUSD'], channels=[TRADES], callbacks={TRADES: chadlor_trade}), timeout=-1)
    f.add_feed(Coinbase(pairs=['BTC-USD'], channels=[TRADES], callbacks={TRADES: chadlor_trade}), timeout=-1)
    f.run()
