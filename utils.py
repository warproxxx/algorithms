import sys 
import inspect
import os
import redis

if not os.path.isdir("logs/"):
    os.makedirs("logs/")

def print(to_print):
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    dirs = module.__file__.split("/")

    # caller = inspect.getframeinfo(inspect.stack()[1][0])
    # sys.stdout.write("%s:%d - %s" % (caller.filename, caller.lineno)) 

    if len(dirs) > 1:
        filename = dirs[-2] + "_" + dirs[-1].split(".")[0]
    else:
        filename = dirs[-1].split(".")[0]


    if "daddy_" in filename:
        if "eth_daddy" in filename:
            filename = "ETH_daddy_bot"
        else:
            filename = "XBT_daddy_bot"
            
    if "vol_trend_" in filename:
        filename = "vol_trend_bot"
    if "altcoin_" in filename:
        filename = "altcoin_bot"
    if "ratio_" in filename:
        filename = "ratio_bot"
    if "chadlor_" in filename:
        filename = "chadlor_bot"
        
    if isinstance(to_print, str) == False:
        try:
            to_print = "{}".format(to_print)
        except:
            to_print = "Error Printing"
        
    sys.stdout.write(to_print + "\n")
    open("logs/{}.log".format(filename), "a").write(to_print + "\n")


def flush_redis():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    for key in r.scan_iter("202*"):
        r.delete(key)

    r.set('first_execution', 1)
    r.set('first_nine', 1)
    r.set('got_this_turn', 0)

    try:
        r.get('daddy_enabled').decode()
    except:
        r.set('daddy_enabled', 0)

    try:
        r.get('vol_trend_enabled').decode()
    except:
        r.set('vol_trend_enabled', 0)

    try:
        r.get('altcoin_enabled').decode()
    except:
        r.set('altcoin_enabled', 0)

    try:
        r.get('ratio_enabled').decode()
    except:
        r.set('ratio_enabled', 0)