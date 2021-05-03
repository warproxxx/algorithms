import pandas as pd
import redis
import threading
import multiprocessing

from cryptofeed.feedhandler import FeedHandler
from cryptofeed import exchanges
from cryptofeed.defines import TRADES, L2_BOOK, BID, ASK, TICKER

from algos.daddy.bot import daddy_bot, daddy_trade, daddy_book, daddy_ticker
from algos.eth_daddy.bot import eth_daddy_bot

from algos.interface.bot import perform

from main import initial_tasks

from utils import print

f = FeedHandler(retries=100000)

EXCHANGES = pd.concat([pd.read_csv('algos/daddy/exchanges.csv'), pd.read_csv('algos/eth_daddy/exchanges.csv')])
EXCHANGES = EXCHANGES[EXCHANGES['trade'] == 1]
EXCHANGES = EXCHANGES.drop_duplicates(subset=['exchange', 'symbol']) 
EXCHANGES = EXCHANGES.fillna("")

r = redis.Redis(host='localhost', port=6379, db=0)
r.set('update_running', 0)

def bot():
    daddy_thread = multiprocessing.Process(target=daddy_bot, args=())
    daddy_thread.start()
    
    eth_daddy_thread = multiprocessing.Process(target=eth_daddy_bot, args=())
    eth_daddy_thread.start()

    while True:
        try:
            if float(r.get('daddy_enabled').decode()) != 1:
                if daddy_thread.is_alive():
                    print("Daddy Bot terminated")
                    daddy_thread.terminate()

            if daddy_thread.is_alive() == False:
                if float(r.get('daddy_enabled').decode()) == 1:
                    print("Daddy Bot started")
                    daddy_thread = multiprocessing.Process(target=daddy_bot, args=())
                    daddy_thread.start()

            if float(r.get('eth_daddy_enabled').decode()) != 1:
                if eth_daddy_thread.is_alive():
                    print("ETH Daddy Bot terminated")
                    eth_daddy_thread.terminate()

            if eth_daddy_thread.is_alive() == False:
                if float(r.get('eth_daddy_enabled').decode()) == 1:
                    print("ETH Daddy Bot started")
                    eth_daddy_thread = multiprocessing.Process(target=eth_daddy_thread, args=())
                    eth_daddy_thread.start()


        except Exception as e:
            print(str(e))

if __name__ == "__main__":
    initial_tasks()

    bot_thread = threading.Thread(target=bot)
    bot_thread.start()

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
                f.add_feed(b(pairs=pairs, channels=channels, callbacks=callbacks), timeout=-1)

    f.run()