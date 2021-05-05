import pandas as pd
import numpy as np
import json


import os
import subprocess
import threading

import requests

import time
import datetime

import ta

import pytz

from arctic import Arctic, TICK_STORE
from arctic.date import DateRange

from scipy.ndimage import gaussian_filter
import requests
import redis

import uuid

from algos.daddy.backtest import perform_backtest

store = Arctic('localhost')
config = json.load(open("daddy.json"))

if store.library_exists('daddy') == False:
    store.initialize_library('daddy', lib_type=TICK_STORE)

library = store['daddy']
library._chunk_size = 500000

def get_data(url, index, proxy):    
    global results
    global threads
        
    if proxy == None:
        res = requests.get(url)
    else:
        proxies = {
          "http": "http://" + proxy,
          "https": "https://" + proxy,
        }

        res = requests.get(url, proxies=proxies)

        
    results[index] = pd.DataFrame(json.loads(res.text))

def get_df(start_time, symbol, proxy=None, total_range=30):
    global threads
    global results
    
    start_time = pd.to_datetime(start_time).tz_localize(None)
    
    if start_time.date() == datetime.datetime.utcnow().date():
        urls = ["https://www.bitmex.com/api/v1/trade?symbol={}{}&count={}&start={}&reverse=false&startTime={}".format(symbol, config['secondary_currency'][symbol], 1000, i * 1000, start_time) for i in range(total_range)]
    else:
        urls = ["https://www.bitmex.com/api/v1/trade?symbol={}{}&count={}&start={}&reverse=false&startTime={}&endTime={}".format(symbol, config['secondary_currency'][symbol], 1000, i * 1000, start_time, pd.to_datetime(start_time.date() + pd.Timedelta(days=1))) for i in range(total_range)]
    
    threads = [None] * len(urls)
    results = [None] * len(urls)
    
    for i in range(len(threads)):
        threads[i] = threading.Thread(target=get_data, args=(urls[i], i, proxy))
        threads[i].start()
    
    for i in range(len(threads)):
        threads[i].join()

    df = pd.DataFrame()

    for curr_df in results:
        df = df.append(curr_df, ignore_index=True)
                    
    return df

def manual_scrape(scrape_from, symbol, sleep=True):
    print("Manual scrape for {} from {}".format(symbol, scrape_from))
    proxy_df = pd.read_csv('proxies_{}'.format(symbol), sep=':', header=None)
    proxy_df.columns = ['proxy', 'port', 'username', 'password']

    proxy_df['proxy_string'] =  proxy_df['username'] + ":" + proxy_df['password'] + "@" + proxy_df['proxy'] + ":" + proxy_df['port'].astype(str)
    proxy_list = list(proxy_df['proxy_string'])
    at_once = len(proxy_list)
    all_df = pd.DataFrame()
    completed = False
    
    while True:
        start_time = time.time()
        
        for i in range(at_once):
            curr_df = get_df(scrape_from, symbol, proxy=proxy_list[i-1])
                
            all_df = all_df.append(curr_df, ignore_index=True)
            all_df = all_df.dropna(subset=['timestamp'], how='all')
            
            scrape_from = all_df.iloc[-1]['timestamp']
            print("Got {} {} data till {}".format(len(curr_df), symbol, scrape_from))
            
            if len(curr_df) < 1000:
                completed = True
                break
         
        total_time_taken = time.time() - start_time
        
        to_sleep = int(60 - total_time_taken) + 1
        
        if completed == True:
            break

        if to_sleep > 0:
            if sleep == True:
                print("Sleeping {} seconds".format(to_sleep))
                time.sleep(to_sleep)
        else:
            print("No need to sleep")
            
    
    all_df['timestamp'] = pd.to_datetime(all_df['timestamp'])
    all_df['timestamp'] = all_df['timestamp'].dt.tz_localize(None)
    all_df = all_df.sort_values('timestamp').reset_index(drop=True)
            
    return all_df

def aws_scrape(name, symbol):
    print("AWS Scrape for {}".format(name))
    url = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{}".format(name)
    r = requests.get(url)
    uid = uuid.uuid4()
    temp = uid.hex[:8]
    
    with open(temp, 'wb') as f:
        f.write(r.content)
        
    df = pd.read_csv(temp, compression='gzip')
    os.remove(temp)

    aws_df = df[df['symbol'] == '{}{}'.format(symbol, config['secondary_currency'][symbol])]
    aws_df['timestamp'] = pd.to_datetime(aws_df['timestamp'], format="%Y-%m-%dD%H:%M:%S.%f")
    aws_df = aws_df.sort_values('timestamp').reset_index(drop=True)
    return aws_df

def get_bitmex_data(start, end, symbol, sleep=True):
    all_df = []

    for scrape_date in pd.date_range(start, end):
        if scrape_date.date() == datetime.datetime.utcnow().date() - pd.Timedelta(days=1):
            curr_time = datetime.datetime.utcnow()
            if curr_time.time() > datetime.time(5,41):
                df = aws_scrape(scrape_date.strftime("%Y%m%d.csv.gz"), symbol)
            else:
                df = manual_scrape(scrape_date, symbol, sleep=sleep)
        elif scrape_date.date() == datetime.datetime.utcnow().date():
            df = manual_scrape(scrape_date, symbol, sleep=sleep)
        else:
            df = aws_scrape(scrape_date.strftime("%Y%m%d.csv.gz"), symbol)

        all_df.append(df)
    
    return pd.concat(all_df, axis=0)

def update_trades(symbol='XBT'):
    end = pd.to_datetime(datetime.datetime.utcnow()).date()
    original_start = end - pd.Timedelta(days=20)
    
    try:
        start = pd.to_datetime(library.max_date('{}_trades'.format(symbol)).astimezone(pytz.UTC)).tz_localize(None)
        
        if start.hour == 23 and start.minute >= 58:
            start = pd.to_datetime(start.date() + pd.Timedelta(days=1))
    except:
        start = original_start

    while True:
        try:
            end = pd.to_datetime(datetime.datetime.utcnow())

            print("{} to {}".format(start, end))
            df = get_bitmex_data(start, end, symbol=symbol)
            df = df[['timestamp', 'symbol', 'side', 'size', 'price', 'homeNotional', 'foreignNotional']]
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
            df = df.tz_localize(tz='UTC')
            library.write('{}_trades'.format(symbol), df)               
            break
        except Exception as e:
            error_mess = str(e)

            if "Document already exists with" in error_mess:
                splitted = error_mess.split(" ")
                exist_date = splitted[6].replace("end:", "")
                exist_date_2 = splitted[7]
                exist_till = pd.to_datetime(exist_date + " " + exist_date_2)
                new_df = df[df.index > exist_till]

                if len(new_df) == 0:
                    print("This timeframe already exists")
                else:
                    print("Writing from middle")
                    library.write('{}_trades'.format(symbol), new_df)               
                
                break
            elif 'timestamp' in str(e):
                print("Timestamp error")
            else:
                print("Exception: {}. Retrying in 20 secs".format(str(e)))
                time.sleep(20)

def get_significant_traders(df):
    df = df[['timestamp', 'side', 'homeNotional', 'foreignNotional']]
    df = df.groupby(['timestamp', 'side']).sum() 
    df = df.reset_index()
    df = df[df['foreignNotional'] > 500]
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['price'] = df['foreignNotional']/df['homeNotional']
    df = df.sort_values('timestamp')
    df = df.drop_duplicates()
    return df

def get_features(curr_df, coin):
    ser = {}
    curr_df = curr_df.sort_values('timestamp')
    
    if len(curr_df) > 0:
        ser['open'] = curr_df.iloc[0]['price']
        ser['high'] = curr_df['price'].max()
        ser['low'] = curr_df['price'].min()
        ser['close'] = curr_df.iloc[-1]['price']
        ser['volume'] = curr_df['foreignNotional'].sum()
    else:
        ser['open'] = np.nan
        ser['high'] = np.nan
        ser['low'] = np.nan
        ser['close'] = np.nan
        ser['volume'] = np.nan
        
    buy_orders = curr_df[curr_df['side'] == 'Buy']
    sell_orders = curr_df[curr_df['side'] == 'Sell']

    total_buy = buy_orders['homeNotional'].sum()
    total_sell = sell_orders['homeNotional'].sum()
    total = total_buy + total_sell

    ser['buy_percentage'] = total_buy/total
    ser['buy_volume'] = total_buy
    ser['all_volume'] = total
    
    readable_bins = []
    

    select_bins = config['bin_size'][coin]
    readable_bins = select_bins.copy()
    readable_bins.append(np.inf)
        
    readable_labels = ['small', 'medium', 'large']
    curr_df['new_range'] = pd.cut(curr_df['homeNotional'], readable_bins, include_lowest=True, labels=readable_labels).astype(str)
    
        
    for curr_range in set(readable_labels):
        group = curr_df[curr_df['new_range'] == curr_range]
        ser["percentage_{}".format(curr_range)] = np.nan_to_num(group['homeNotional'].sum()/total, 0)
        buy_orders = group[group['side'] == 'Buy']
        ser['buy_percentage_{}'.format(curr_range)] = np.nan_to_num((buy_orders['homeNotional'].sum())/group['homeNotional'].sum(), 0)

    return pd.Series(ser)

def get_features_from_sig(df, symbol):
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    minute_only = df['timestamp'].dt.minute.astype(str)
    minute_only_two = minute_only.apply(lambda x: str(x)[1:]) #there is a mistake here.
    df = df[~((minute_only == '9') | (minute_only_two == '9') | (minute_only == '8')  | (minute_only_two == '8'))]

    features = df.groupby(pd.Grouper(key='timestamp', freq="10Min", label='left')).apply(get_features, coin=symbol)
    features = features.reset_index()

    features['timestamp'] = pd.to_datetime(features['timestamp'])
    features = features.drop_duplicates(subset=['timestamp'])
    features = features.sort_values('timestamp')
    return features

def get_intervaled_date(startTime):
    time_df = pd.DataFrame(pd.Series({'Time': startTime})).T
    return time_df.groupby(pd.Grouper(key='Time', freq="10Min", label='left')).sum().index[0]

def update_price(symbol='XBT'):
    start_time = "2020-01-01"

    if os.path.isfile("data/{}USD_daily.csv".format(symbol)):
        start_time = pd.read_csv('data/{}USD_daily.csv'.format(symbol)).iloc[-1]['timestamp']

    if (pd.to_datetime(start_time).date() < pd.Timestamp.utcnow().date()):
        try:
            new_url = 'https://www.bitmex.com/api/v1/trade/bucketed?binSize=1d&partial=false&symbol={}{}&count=500&reverse=false&startTime={}'.format(symbol, config['secondary_currency'][symbol], start_time)
            res = requests.get(new_url)
            price_df = pd.DataFrame(json.loads(res.text))
            price_df['timestamp'] = pd.to_datetime(price_df['timestamp'])
            price_df = price_df.set_index('timestamp').tz_localize(None).reset_index()


            if os.path.isfile("data/{}USD_daily.csv".format(symbol)):
                old_df = pd.read_csv("data/{}USD_daily.csv".format(symbol))
                old_df['timestamp'] = pd.to_datetime(old_df['timestamp'])
                df = pd.concat([old_df, price_df])
                df = df.drop_duplicates(subset=['timestamp'])
                df.to_csv('data/{}USD_daily.csv'.format(symbol), index=None)
            else:
                price_df.to_csv('data/{}USD_daily.csv'.format(symbol), index=None)
        except Exception as e:
            print("Exception in parameter performer: {}".format(str(e)))
    else:
        pass

def get_trends(symbol='XBT'):
    update_price(symbol=symbol)
    df = pd.read_csv("data/{}USD_daily.csv".format(symbol))
    df = df[['timestamp', 'close']]
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df["30D_volatility"] = df['close'].rolling(30).std()/10
    df['30D_volatility'] = df['30D_volatility'].fillna(method='bfill').fillna(method='ffill')

    gaussian_vols = []

    for idx, row in df.iterrows():
        gaussian_vols.append(gaussian_filter(df[:idx+1]['30D_volatility'], 3.)[-1])

    df['30D_volatility'] = gaussian_vols
    
    price_df = df.copy()
    curr_group = ""
    new_price_df = pd.DataFrame()

    for i in range(1, len(price_df)):
        row = price_df.iloc[i]
        curr_vol = price_df.iloc[i]['30D_volatility']
        prev_vol = price_df.iloc[i-1]['30D_volatility']
        three_vol = price_df.iloc[i-2]['30D_volatility']

        if pd.isnull(prev_vol) == False:
            if curr_group == "":
                curr_group = price_df.iloc[i]['timestamp']


            if (three_vol - prev_vol) * (prev_vol - curr_vol) < 0:
                curr_group = price_df.iloc[i]['timestamp']



            row['curr_group'] = curr_group
            new_price_df = new_price_df.append(row, ignore_index=True)
            
    return new_price_df

def save_features(features, symbol):
    features.to_csv('data/{}_features.csv'.format(symbol), index=None)

def run_backtest(symbol='XBT', parameter_file='algos/daddy/parameters.json'):
    update_trades(symbol=symbol)
    last_date = pd.to_datetime(library.max_date('{}_trades'.format(symbol)).astimezone(pytz.UTC)).tz_localize(None)

    minute = str(last_date.time().minute)

    if len(minute) == 1:
        minute_only = int(minute)
    else:
        minute_only = int(minute[1:])
        
    if (minute_only < 6):
        have_till_calc = last_date - pd.Timedelta(minutes=10)
    else:
        have_till_calc = last_date

    
    have_till = get_intervaled_date(have_till_calc)


    min_date = pd.to_datetime(library.min_date('{}_trades'.format(symbol)).astimezone(pytz.UTC)).tz_localize(None)
    startTime = get_intervaled_date(min_date)

    if os.path.isfile('data/{}_features.csv'.format(symbol)):
        startTime = pd.to_datetime(subprocess.check_output(["tail", "-1", "data/{}_features.csv".format(symbol)]).decode().split(",")[0])
    
    have_till = have_till.tz_localize(tz='UTC')
    startTime = startTime.tz_localize(tz='UTC')

    if have_till + pd.Timedelta(minutes=10) != startTime:
        df = library.read('{}_trades'.format(symbol), date_range = DateRange(start=startTime, end=have_till + pd.Timedelta(minutes=10)))
        df = df.tz_convert('UTC').tz_localize(None)
        df = df.reset_index()

        df = df.rename(columns={'index': 'timestamp'})
        
        #calculate and save features
        df = get_significant_traders(df)
        features = get_features_from_sig(df, symbol=symbol)

        features['change'] = ((features['close'] - features['open'])/features['open']) * 100
        features = features[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'change', 'percentage_large', 'buy_percentage_large']]
        
        if os.path.isfile('data/{}_features.csv'.format(symbol)):
            old_features = pd.read_csv('data/{}_features.csv'.format(symbol))
            old_features['timestamp'] = pd.to_datetime(old_features['timestamp'])
            features = pd.concat([old_features, features])
            features = features.drop_duplicates(subset=['timestamp']).reset_index(drop=True)

        features['macd'] = ta.trend.macd_signal(features['close'])
        features['rsi'] = ta.momentum.rsi(features['close'])
        
        save_fe_thread = threading.Thread(target=save_features, args=(features,symbol))
        save_fe_thread.start()
    
    # features = pd.read_csv('data/{}_features.csv'.format(symbol))
    features['timestamp'] = pd.to_datetime(features['timestamp'])
    trends = get_trends(symbol=symbol)
    curr_group = trends.iloc[-1]['curr_group'].date()
    last_date = features.iloc[-1]['timestamp'].date()

    if last_date.day - curr_group.day < 4:
        curr_group = last_date - pd.Timedelta(days=4)
    
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        trend_start_date = pd.to_datetime(r.get('trend_start_date_{}'.format(symbol)).decode())
        features = features[features['timestamp'] >= trend_start_date]
    except Exception as e:
        print(str(e))
        curr_group =pd.to_datetime(curr_group)
        features = features[features['timestamp'] >= curr_group]
        
    dupe = features.iloc[-1]
    dupe['timestamp'] = dupe['timestamp'] + pd.Timedelta(minutes=10)
    features = features.append(dupe, ignore_index=True)

    parameters = json.load(open(parameter_file))
    run = perform_backtest(features, parameters)
    analysis = run[0].analyzers.getbyname('tradeanalyzer').get_analysis()
    portfolio, trades, operations, stops_triggered = run[0].get_logs()
    
    trades.to_csv("data/{}USD_trades.csv".format(symbol), index=None)
    return analysis, portfolio.iloc[-1]['Date']