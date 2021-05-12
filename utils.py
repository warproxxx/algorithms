import sys 
import inspect
import os
import redis

if not os.path.isdir("logs/"):
    os.makedirs("logs/")

def print(to_print, symbol=""):
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    dirs = module.__file__.split("/")

    caller = inspect.getframeinfo(inspect.stack()[1][0])
    

    if len(dirs) > 1:
        filename = dirs[-2] + "_" + dirs[-1].split(".")[0]
    else:
        filename = dirs[-1].split(".")[0]

    if symbol == "":
        if "vol_trend_" in filename:
            filename = "vol_trend_bot"
        if "altcoin_" in filename:
            filename = "altcoin_bot"
        if "ratio_" in filename:
            filename = "ratio_bot"
        if "chadlor_" in filename:
            filename = "chadlor_bot"
    else:
        filename = "{}_daddy_bot".format(symbol)
        
    if isinstance(to_print, str) == False:
        try:
            to_print = "{}".format(to_print)
        except:
            to_print = "Error Printing"
    
    # sys.stdout.write("%s:%d - %s\n" % (caller.filename, caller.lineno, to_print)) 
    sys.stdout.write(to_print + "\n")
    open("logs/{}.log".format(filename), "a").write(to_print + "\n")


def flush_redis():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    for key in r.scan_iter("202*"):
        r.delete(key)

    try:
        r.get('daddy_enabled').decode()
    except:
        r.set('daddy_enabled', 0)
        
    try:
        r.get('eth_daddy_enabled').decode()
    except:
        r.set('eth_daddy_enabled', 0)

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