import sys 
import inspect
import os

def print(to_print):
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    dirs = module.__file__.split("/")

    if len(dirs) > 1:
        filename = dirs[-2] + "_" + dirs[-1].split(".")[0]
    else:
        filename = dirs[-1].split(".")[0]

    if isinstance(to_print, str) == False:
        try:
            to_print = "{}".format(to_print)
        except:
            to_print = "Error Printing"
        
    sys.stdout.write(to_print + "\n")
    open("logs/{}.log".format(filename), "a").write(to_print + "\n")


def flush_redis(r, EXCHANGES):
    backups = {}

    for var in ['daddy_enabled', 'vol_trend_enabled', 'buy_missed', 'buy_at', 'close_and_stop', 'stop_trading', 'MOVE_mult', 'PERP_mult', 'buy_missed_perp', 'perp_long_or_short', 'price_perp', 'buy_missed_move', 'move_long_or_short', 'price_move', 'override_perp', 'perp_override_direction', 'override_move', 'move_override_direction', 'enable_per_close_and_stop', 'enable_move_close_and_stop', 'stop_perp', 'stop_move']:
        try:
            backups[var] = float(r.get(var).decode())
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