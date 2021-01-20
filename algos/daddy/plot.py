import pandas as pd
import numpy as np
import requests
import json
import os
from scipy.ndimage import gaussian_filter
import plotly.graph_objects as go
import time
from utils import print
from algos.daddy.trades import get_trends
import redis

def create_plot(biased=True):
    df = pd.read_csv("data/btc_daily.csv")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df["30D_volatility"] = df['close'].rolling(30).std()/10
    df['30D_volatility'] = df['30D_volatility'].fillna(method='bfill').fillna(method='ffill')

    if biased == True:
        df['30D_volatility'] = gaussian_filter(df['30D_volatility'], 3.)
    elif biased == False:
        gaussian_vols = []

        for idx, row in df.iterrows():
            gaussian_vols.append(gaussian_filter(df[:idx+1]['30D_volatility'], 3.)[-1])

        df['30D_volatility'] = gaussian_vols

    df = df[df['timestamp'] > "2020-02-20"].reset_index(drop=True)
    
    decrease_to_increase = pd.to_datetime(df[(df['30D_volatility'] < df['30D_volatility'].shift(1)) & (df['30D_volatility'].shift(-1) > df['30D_volatility'])]['timestamp'])
    increase_to_decrease = pd.to_datetime(df[(df['30D_volatility'] > df['30D_volatility'].shift(1)) & (df['30D_volatility'].shift(-1) < df['30D_volatility'])]['timestamp'])

    hovertexts = list(("30D volatility : " + df['30D_volatility'].replace(np.nan, 0).round(2).astype(str)).values)
    fig = go.Figure(layout=go.Layout(xaxis={'spikemode': 'across'}))

    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['close'], name='Close Price', yaxis="y1", hovertext = hovertexts, line={"color": "#636EFA"}, fillcolor="black"))
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['30D_volatility'], name='30D volatility', yaxis="y2", line={"color": "#EF553B"}))


    fig.update_layout(
                yaxis1=dict(
                    titlefont=dict(
                        color="#000000"
                    ),
                    tickfont=dict(
                        color="#000000"
                    ),
                    anchor="free",
                    domain=[0.25, 1], 
                    position=0.0    
                ),
                yaxis2=dict(
                    tickfont=dict(
                        color="#727272"
                    ),
                    anchor="free",
                    domain=[0, 0.18]
                )
    )
                
    fig.update_layout(hovermode="x unified")

    max_y = df['close'].max() + 1000

    for increase_point in decrease_to_increase:
        fig.add_shape(dict(type="line", x0=increase_point, y0=3000, x1=increase_point, y1=max_y, line=dict(color="green", width=1)))

    for decrease_point in increase_to_decrease:
        fig.add_shape(dict(type="line", x0=decrease_point, y0=3000, x1=decrease_point, y1=max_y, line=dict(color="red", width=1)))

    fig.update_layout(
            xaxis=go.layout.XAxis(
                rangeslider=dict(
                    visible=True,
                    thickness=0.05
                ),
                type="date",
            )
        )

    html = fig.to_html()

    if biased == True:
        with open('frontend_interface/static/plotly.html', 'w') as file:
            file.write(html)
    elif biased == False:
        with open('frontend_interface/static/plot_unbiased.html', 'w') as file:
            file.write(html)

    date_ranges = list(decrease_to_increase) + list(increase_to_decrease)
    date_ranges.sort()

    ranges = []
    prev_date = None

    for rec_date in date_ranges:
        if prev_date != None:
            curr_data = {}
            curr_data['Start'] = prev_date
            curr_data['End'] = rec_date
            ranges.append(curr_data)
            
        prev_date = rec_date
        
    ranges.append({'Start': rec_date, 'End': np.nan})

    ranges = pd.DataFrame(ranges)

    if biased == True:
        ranges.to_csv('data/ranges.csv', index=None)
    elif biased == False:
        ranges.to_csv('data/ranges_unbiased.csv', index=None)

def create_chart():
    
    trends = get_trends()

    if trends.iloc[0]['curr_group'] != trends.iloc[-1]['curr_group']:
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.set('stop_trading', 1)

    create_plot(biased=True)
    create_plot(biased=False)

if __name__ == '__main__':
    create_chart()