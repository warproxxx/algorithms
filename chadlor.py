from cryptofeed.defines import TRADES
from cryptofeed.feedhandler import FeedHandler
from cryptofeed.exchanges import Coinbase, Bitmex
from cryptofeed.callback import Callback

from algos.chadlor.bot import chadlor_bot, chadlor_trade

f = FeedHandler(retries=100000)
f.add_feed(Bitmex(pairs=['XBTUSD'], channels=[TRADES], callbacks={TRADES: chadlor_trade}), timeout=-1)
f.add_feed(Coinbase(pairs=['BTC-USD'], channels=[TRADES], callbacks={TRADES: chadlor_trade}), timeout=-1)
f.run()