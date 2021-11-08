from cryptofeed.feedhandler import FeedHandler
from cryptofeed import exchanges
from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK, TICKER

from algos.daddy.bot_utils import daddy_trade, daddy_book, daddy_ticker

import pandas as pd

EXCHANGES = pd.read_csv('algos/eth_daddy/exchanges.csv')
f = FeedHandler()

for idx, row in EXCHANGES.iterrows():
    if row['trade'] == 1:
        exchange = row['cryptofeed_name']
        b = getattr(exchanges, exchange)

        pairs = [row['cryptofeed_symbol']]

        trade_callbacks = []
        obook_callbacks = []
        ticker_callbacks = []

        if row['types'] != "":
            for type in row['types'].split(";"):
                if 'stream' in type:
                    trade_callbacks.append(daddy_trade)

                if 'book' in type:
                    obook_callbacks.append(daddy_book)

                if 'ticker' in type:
                    ticker_callbacks.append(daddy_ticker)

            channels = []
            callbacks = {}

            if len(trade_callbacks) > 0:
                channels.append(TRADES)
                callbacks[TRADES] = trade_callbacks

            if len(obook_callbacks) > 0:
                channels.append(L2_BOOK)
                callbacks[L2_BOOK] = obook_callbacks

            if len(ticker_callbacks) > 0:
                channels.append(TICKER)
                callbacks[TICKER] = ticker_callbacks

            print("{} {} {}".format(pairs, callbacks, channels))
            f.add_feed(b(symbols=pairs, channels=channels, callbacks=callbacks, retries=1000000000000), timeout=-1)

f.run()