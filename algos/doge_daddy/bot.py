import schedule
from algos.daddy.bot_utils import start_bot, create_chart
import threading
import time

def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)

def doge_daddy_bot():
    print("DOGE daddy bot")
    schedule.every().day.at("00:30").do(create_chart, symbol='DOGE')

    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()

    bot_thread = threading.Thread(target=start_bot, args=('DOGE', False, 'algos/doge_daddy/exchanges.csv', "algos/doge_daddy/parameters.json", ))
    bot_thread.start()