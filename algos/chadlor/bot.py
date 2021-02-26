import redis
import pandas as pd

r = redis.Redis(host='localhost', port=6379, db=0)

async def chadlor_trade(feed, pair, order_id, timestamp, receipt_timestamp, side, amount, price, order_type=None):
    try:
        old_timestamp = r.get('{}_timestamp'.format(feed)).decode()
    except:
        old_timestamp = "1970-01-01"

    processed_timestamp = pd.to_datetime(timestamp, unit='s')
    processed_timestamp = str(processed_timestamp.replace(second=0, microsecond=0, nanosecond=0))

    if processed_timestamp != old_timestamp:
        close_price = float(r.get('{}_close'.format(feed)))
        print("{} Timestamp: {} processed_timestamp: {} Price: {} Close Price: {}".format(feed, pd.to_datetime(timestamp, unit='s'), processed_timestamp, price, close_price))
        r.set('{}_timestamp'.format(feed), processed_timestamp)
    
    r.set('{}_close'.format(feed), float(price))

def chadlor_bot():
    pass