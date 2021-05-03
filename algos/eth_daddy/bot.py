import schedule
from algos.daddy.bot import start_bot, create_chart
import threading
import time

def start_schedlued():
    while True:
        schedule.run_pending()
        time.sleep(1)


def eth_daddy_bot():
    schedule.every().day.at("00:30").do(create_chart, symbol='ETH')

    schedule_thread = threading.Thread(target=start_schedlued)
    schedule_thread.start()

    start_bot(symbol='ETH', TESTNET=False, config_file='algos/eth_daddy/exchanges.csv', parameter_file="algos/eth_daddy/parameters.json")