import sys 
import inspect
import os

def print(to_print):
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    filename = module.__file__.split("/")[-1].split(".")[0]

    if isinstance(to_print, str) == False:
        try:
            to_print = "{}".format(to_print)
        except:
            to_print = "Error Printing"
        
    sys.stdout.write(to_print + "\n")
    open("logs/{}.log".format(filename), "a").write(to_print + "\n")


def flush_redis(r, EXCHANGES):
    backups = {}

    for var in ['daddy_enabled', 'vol_trend_enabled', 'buy_missed', 'buy_at', 'close_and_stop', 'stop_trading']:
        try:
            backups[var] = int(r.get(var).decode())
        except:
            backups[var] = 0


    for idx, details in EXCHANGES.iterrows():
        for var in ['{}_position_since', '{}_avgEntryPrice', '{}_current_pos', '{}_pos_size', '{}_best_ask', '{}_best_bid']:
            try:
                backups[var.format(details['exchange'])] = r.get(var.format(details['exchange'])).decode()
            except:
                pass
    
    r.flushdb()

    for idx, row in backups.items():
        r.set(idx, row)


    r.set('first_execution', 1)
    r.set('first_nine', 1)
    r.set('got_this_turn', 0)