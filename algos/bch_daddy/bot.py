import schedule
from algos.daddy.bot_utils import start_bot, create_chart
import threading
import time

def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)

def bch_daddy_bot():
    print("BCH daddy bot")
    schedule.every().day.at("00:30").do(create_chart, symbol='BCH')

    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()

    bot_thread = threading.Thread(target=start_bot, args=('BCH', False, 'algos/bch_daddy/exchanges.csv', "algos/bch_daddy/parameters.json", ))
    bot_thread.start()