import pandas as pd
import requests
import json
import pandas as pd
import os
import subprocess
from scipy.ndimage import gaussian_filter
import schedule

from algos.interface.plot import create_multiple_plot, create_gaussian_plot

def get_data(start, end):
    
    headers = {'apikey' : 'eab6d14e-c11f-4cfe-9925-37e55d9cb3b2 '}
    all_df = pd.DataFrame()
    
    while True:
        response = requests.get("http://api.bitdataset.com/v1/trades/history/coinbasepro:BTCUSD?start={}&end={}&limit=100000".format(start, end), headers=headers)
        curr_json = json.loads(response.text)
        curr_df = pd.DataFrame.from_dict(curr_json)
        start = curr_df.iloc[-1]['time']
        
        all_df = all_df.append(curr_df, ignore_index=True)
        
        if len(curr_df) < 100000:
            break
    
    all_df['time'] = pd.to_datetime(all_df['time'])
    return all_df

def process_trades(curr_df):
    ser = {}
    
    curr_df['size_usd'] = curr_df['price'] * curr_df['size']
    buys = curr_df[curr_df['side'] == 'B']
    sells = curr_df[curr_df['side'] == 'S']
    
    bigger = curr_df[curr_df['size_usd'] > 10000]
    buy_large = buys[buys['size_usd'] > 10000]
    sell_large = sells[sells['size_usd'] > 10000]
    
    try:
        ser['open'] = curr_df.iloc[0]['price']
        ser['high'] = max(curr_df['price'])
        ser['low'] = min(curr_df['price'])
        ser['close'] = curr_df.iloc[-1]['price']
        ser['volume'] = sum(curr_df['size'])
    except:
        ser['open'] = np.nan
        ser['high'] = np.nan
        ser['low'] = np.nan
        ser['close'] = np.nan
        ser['volume'] = np.nan
    
        
    ser['total_trades'] = len(curr_df)
    ser['total_sells'] = len(sells)
    ser['total_buys'] = len(buys)
    ser['total_big'] = len(bigger)
    
    ser['mean_size'] = curr_df['size_usd'].mean()
    ser['buys_mean'] = buys['size_usd'].mean()
    ser['sells_mean'] = sells['size_usd'].mean()
    ser['big_mean'] = bigger['size_usd'].mean()
    
    ser['median_size'] = curr_df['size_usd'].median()
    ser['buys_median'] = buys['size_usd'].median()
    ser['sells_median'] = sells['size_usd'].median()
    ser['big_median'] = bigger['size_usd'].median()
    ser['big_buy_median'] = buy_large['size_usd'].median()
    ser['big_sell_median'] = sell_large['size_usd'].median()
    
    ser['buy_percentage'] = buys['size_usd'].sum()/curr_df['size_usd'].sum()
    ser['buy_large_percentage'] = buy_large['size_usd'].sum()/curr_df['size_usd'].sum()
    ser['sell_large_percentage'] = sell_large['size_usd'].sum()/curr_df['size_usd'].sum()
    
    return pd.Series(ser)

def perform():
    df = pd.read_csv('processed.csv')
    df['time'] = pd.to_datetime(df['time'])
    start = df.iloc[-1]['time'] + pd.Timedelta(days=1)
    startTime = start.tz_localize(None).isoformat()
    endTime = pd.to_datetime(pd.to_datetime('now').date()).isoformat()
    cbase = get_data(startTime, endTime)
    features = cbase.groupby(pd.Grouper(key='time', freq='1D', label='left')).apply(process_trades)
    features.reset_index().to_csv('processed.csv', header=None, index=None, mode='a')

def create_selected_plot(col, days=7, gaussian=3., calc='mean'):
    if calc == "mean":
        df['selected'] = df[col].rolling(days).mean()
    elif calc == "std":
        df['selected'] = df[col].rolling(days).std()
    
    gaussian_vols = []

    for idx, row in df.iterrows():
        gaussian_vols.append(gaussian_filter(df[:idx+1]['selected'], gaussian)[-1])

    df['selected'] = gaussian_vols
    df['selected'] = df['selected'].fillna(method='bfill')
    
    fig = create_multiple_plot(df, ['selected', 'close'], 'time')
    return create_gaussian_plot(df, fig, col='selected', time='time')

def create_selected_plot(df, col, days=7, gaussian=3., calc='mean'):
    if calc == "mean":
        df['selected'] = df[col].rolling(days).mean()
    elif calc == "std":
        df['selected'] = df[col].rolling(days).std()
    
    gaussian_vols = []

    for idx, row in df.iterrows():
        gaussian_vols.append(gaussian_filter(df[:idx+1]['selected'], gaussian)[-1])

    df['selected'] = gaussian_vols
    df['selected'] = df['selected'].fillna(method='bfill')
    
    fig = create_multiple_plot(df, ['selected', 'close'], 'time')
    return create_gaussian_plot(df, fig, col='selected', time='time')

def create_charts():
    df = pd.read_csv('processed.csv')
    df['time'] = pd.to_datetime(df['time'])
    df['ratio'] = df['big_buy_median']/df['big_sell_median']
    fig = create_selected_plot(df, 'close', days=30, gaussian=4, calc='std')
    html = fig.to_html()

    with open('frontend_interface/static/30_close_plot.html', 'w') as file:
        file.write(html)

    fig = create_selected_plot(df, 'close', days=20, gaussian=4, calc='std')
    html = fig.to_html()

    with open('frontend_interface/static/20_close_plot.html', 'w') as file:
        file.write(html)

    fig = create_selected_plot(df, 'big_buy_median')
    html = fig.to_html()

    with open('frontend_interface/static/big_buy_plot.html', 'w') as file:
        file.write(html)

    fig = create_selected_plot(df, 'ratio')
    html = fig.to_html()

    with open('frontend_interface/static/ratio_plot.html', 'w') as file:
        file.write(html)

def daily_task():
    try:
        perform()
        create_charts()
    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    create_charts()