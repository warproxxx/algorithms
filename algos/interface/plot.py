import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import pandas as pd
import numpy as np

def create_gaussian_plot(df, fig, col='30D_volatility', time='Time'):
    decrease_to_increase = pd.to_datetime(df[(df[col] < df[col].shift(1)) & (df[col].shift(-1) > df[col])][time])
    increase_to_decrease = pd.to_datetime(df[(df[col] > df[col].shift(1)) & (df[col].shift(-1) < df[col])][time])
    
    max_y = df[col].max() + 0.1 * df[col].max() 
    min_y = df[col].min() - 0.3 * df[col].min() 

    for increase_point in decrease_to_increase:
        fig.add_shape(dict(type="line", x0=increase_point, y0=min_y, x1=increase_point, y1=max_y, line=dict(color="green", width=1)))

    for decrease_point in increase_to_decrease:
        fig.add_shape(dict(type="line", x0=decrease_point, y0=min_y, x1=decrease_point, y1=max_y, line=dict(color="red", width=1)))

    
    return fig

def create_trend_plot(df):
    decrease_to_increase = pd.to_datetime(df[(df['30D_volatility'] < df['30D_volatility'].shift(1)) & (df['30D_volatility'].shift(-1) > df['30D_volatility'])]['startTime'])
    increase_to_decrease = pd.to_datetime(df[(df['30D_volatility'] > df['30D_volatility'].shift(1)) & (df['30D_volatility'].shift(-1) < df['30D_volatility'])]['startTime'])

    hovertexts = list(("30D volatility : " + df['30D_volatility'].replace(np.nan, 0).round(2).astype(str)).values)
    fig = go.Figure(layout=go.Layout(xaxis={'spikemode': 'across'}))

    fig.add_trace(go.Scatter(x=df['startTime'], y=df['close'], name='Close Price', yaxis="y1", hovertext = hovertexts, line={"color": "#636EFA"}, fillcolor="black"))
    fig.add_trace(go.Scatter(x=df['startTime'], y=df['30D_volatility'], name='30D volatility', yaxis="y2", line={"color": "#EF553B"}))


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

    max_y = df['close'].max() + 0.1 * df['close'].max() 
    min_y = df['close'].min() - 0.3 * df['close'].min() 

    min_y = max(0, min_y)

    for increase_point in decrease_to_increase:
        fig.add_shape(dict(type="line", x0=increase_point, y0=min_y, x1=increase_point, y1=max_y, line=dict(color="green", width=1)))

    for decrease_point in increase_to_decrease:
        fig.add_shape(dict(type="line", x0=decrease_point, y0=min_y, x1=decrease_point, y1=max_y, line=dict(color="red", width=1)))

    fig.update_layout(
            xaxis=go.layout.XAxis(
                rangeslider=dict(
                    visible=True,
                    thickness=0.05
                ),
                type="date",
            )
        )
    return fig

def create_multiple_plot(df, variable_names, time='Time', verbose=False):        
    fig = go.Figure(layout=go.Layout(xaxis={'spikemode': 'across'}))
    colors = ['#727272', '#56b4e9', "#009E73", "#000000"]
    last = len(variable_names) - 1
    
    var_one = variable_names[0]
    var_two = variable_names[1]
    
    
    for i in range(0, len(variable_names)):
        var = variable_names[i]
        
        if i <= (len(colors)):
            color = colors[i]
        else:
            color = ''
        
        if verbose == True:
            print("i: {} var: {} color: {}".format(i, var, color))
        
        if i != last:
            fig.add_trace(go.Scatter(x=df[time], y=df[var], name=var, marker={'color': color}, yaxis="y1"))
        else:
            fig.add_trace(go.Scatter(x=df[time], y=df[var], name=var, marker={'color': color}, yaxis="y2"))
    


    fig.update_layout(
            yaxis=dict(
                titlefont=dict(
                    color="#000000"
                ),
                tickfont=dict(
                    color="#000000"
                )
            ),
            yaxis2=dict(
                tickfont=dict(
                    color=color
                ),
                anchor="free",
                overlaying="y",
                side="left",
                position=1
            ))
            
    fig.update_layout(
        xaxis=go.layout.XAxis(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label="1m",
                         step="month",
                         stepmode="backward"),
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="YTD",
                         step="year",
                         stepmode="todate"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
                visible=True,
            ),
            type="date",
        )
    )
    
    fig = fig.update_xaxes(spikemode='across+marker')
    fig = fig.update_layout(hovermode="x")

    return fig

def create_triple_plot(combined, variable_names, date_col, axis='different'):    
    fig = go.Figure(layout=go.Layout(xaxis={'spikemode': 'across'}))
    first = variable_names[0]
    second = variable_names[1]
    third = variable_names[2]
    
    if axis == 'different':
        fig.add_trace(go.Scatter(x=combined[date_col], y=combined[first], name=first, yaxis="y1"))
        fig.add_trace(go.Scatter(x=combined[date_col], y=combined[second], name=second, yaxis="y2"))
        fig.add_trace(go.Scatter(x=combined[date_col], y=combined[third], name=third, yaxis="y3"))

        fig.update_layout(
                yaxis=dict(
                    titlefont=dict(
                        color="#000000"
                    ),
                    tickfont=dict(
                        color="#000000"
                    )
                ),
                yaxis2=dict(
                    anchor="free",
                    overlaying="y",
                    side="left",
                    position=1
                ),
                yaxis3=dict(
                    anchor="x",
                    overlaying="y",
                    side="left",
                    position=0.5
                )

        )
    elif axis == 'same':
        fig.add_trace(go.Scatter(x=combined[date_col], y=combined[first], name=first))
        fig.add_trace(go.Scatter(x=combined[date_col], y=combined[second], name=second))
        fig.add_trace(go.Scatter(x=combined[date_col], y=combined[third], name=third))

    fig.update_layout(
        xaxis=go.layout.XAxis(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label="1m",
                         step="month",
                         stepmode="backward"),
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="YTD",
                         step="year",
                         stepmode="todate"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
                visible=True,
            ),
            type="date",
            showspikes=True,
            spikemode="across"
        )
    )
    fig = fig.update_xaxes(spikemode='across+marker')
    fig = fig.update_layout(hovermode="x")
    
    return fig