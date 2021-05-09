from django.shortcuts import render
from django.http import HttpResponseRedirect, HttpResponse 
from .forms import adminLoginForm

import os
import io
from shutil import copy
from glob import glob

import pandas as pd
import json

import redis

from algos.daddy.defines import trade_methods

from algos.vol_trend.bot import get_position_balance
from algos.altcoin.bot import get_positions
from algos.ratio.bot import get_positions as get_ratio_positions

from algos.altcoin.defines import trade_methods as altcoin_methods

import random

import datetime

def calc_slippage(ser):
    if ser['side'] == 'BUY':
        return (((ser['expectedPrice'] - ser['actualPrice'])/ser['actualPrice']) * 100)
    elif ser['side'] == 'SELL':
        return (((ser['actualPrice'] - ser['expectedPrice'])/ser['expectedPrice']) * 100)

def get_long_short_details(details_df):
    longs = details_df[details_df['backtest_position'] == 'LONG']
    shorts = details_df[details_df['backtest_position'] == 'SHORT']

    long_allocation = (longs['allocation'] * longs['live_lev']).sum()
    short_allocation = (shorts['allocation'] * shorts['live_lev']).sum()

    details = {}
    details['longs'] = len(longs)
    details['shorts'] = len(shorts)
    details['long_allocation_per'] = round((long_allocation/(long_allocation+short_allocation)) * 100, 2)
    details['short_allocation_per'] = round((short_allocation/(long_allocation+short_allocation)) * 100, 2)

    return details

#create login then interface is done
def index(request):
    if request.user.is_authenticated:
        if request.POST:
            dic = request.POST.dict()
        
        r = redis.Redis(host='localhost', port=6379, db=0)
        algo_details = []
        for file in glob("algos/*"):
            config = json.load(open(file + "/config.json"))
            config['code_name'] = file.split('/')[-1]

            try:
                config['enabled'] = float(r.get('{}_enabled'.format(config['code_name'])).decode())
            except:
                config['enabled'] = 0

            algo_details.append(config)

        return render(request, "frontend_interface/index.html", {'algos': algo_details})
    else:
        return HttpResponseRedirect('/login')

def nissan(request):
    try:
        details_df = get_positions()
        altcoin_balance = details_df['ftx_balance'].sum()

        pnl = altcoin_balance - 4500

        if pnl > 0:
            pnl = pnl / 10

        if pnl < 0:
            pnl = pnl / 5
        
        amount = 1910 + pnl

        amount = amount + random.randint(-10,10)
        return HttpResponse("Under mt gimme few days to fix. It was at 2100")
    except:
        return HttpResponse("error")

def reverse_status(request):
    if request.user.is_authenticated:
        r = redis.Redis(host='localhost', port=6379, db=0)
        var_name = request.GET['code_name'] + "_enabled"
        try:
            old_status = float(r.get(var_name).decode())
        except:
            old_status = 0

        new_status = 1 - old_status
        r.set(var_name, new_status)

        return HttpResponseRedirect('/')
    else:
        return HttpResponseRedirect('/login')

def altcoin_interface(request):
    if request.user.is_authenticated:
        config_file = 'algos/altcoin/config.csv'
        r = redis.Redis(host='localhost', port=6379, db=0)

        if request.POST:
            dic = request.POST.dict()

            if '0[gaussian]' in dic:
                dic.pop('csrfmiddlewaretoken', None)
                
                curr_df = pd.DataFrame()

                for idx, value in dic.items():
                    splitted = idx.split("[")
                    exchange = splitted[0]
                    column = splitted[1].replace("]", "")
                    curr_df = curr_df.append(pd.Series({'col_name': exchange, 'column': column, 'value': value}), ignore_index=True)

                new_exchanges = {}

                for exchange, exchange_values in curr_df.groupby('col_name'):
                    new_exchanges[exchange] = {}
                    
                    for idx, row in exchange_values.iterrows():
                        new_exchanges[exchange][row['column']] = row['value']

                

                new_exchanges = pd.DataFrame(new_exchanges)

                new_exchanges = new_exchanges.T.reset_index().rename(columns={'index': '0'})
                new_exchanges = new_exchanges.drop(columns=['0'])
                new_exchanges.to_csv(config_file, index=None)

            elif 'csv_file' in dic:
                open(config_file, 'w').write(dic['csv_file'])
            elif 'move_free_form' in dic:
                if 'move_free' in dic:
                    r.set('move_free', 1)
                else:
                    r.set('move_free', 0)

            elif 'enable_close_and_main_form' in dic:
                if 'close_and_main' in dic:
                    r.set('close_and_main', 1)
                else:
                    r.set('close_and_main', 0)

            elif 'enable_close_and_rebalance_form' in dic:
                if 'close_and_rebalance' in dic:
                    r.set('close_and_rebalance', 1)
                else:
                    r.set('close_and_rebalance', 0)
            elif 'enter_now_form' in dic:
                if 'enter_now' in dic:
                    r.set('enter_now', 1)
                else:
                    r.set('enter_now', 0)
            elif 'sub_account_form' in dic:
                if 'sub_account' in dic:
                    r.set('sub_account', 1)
                else:
                    r.set('sub_account', 0)
            elif 'alt_starting_capital' in dic:
                r.set('alt_starting_capital', dic['alt_starting_capital'])
                r.set('altcoin_close', dic['altcoin_close'])
        
        config_df = pd.read_csv(config_file)
        config_df = config_df.round(4)

        config = config_df.T.to_dict()

        csv_file = open(config_file, 'r').read()

        details_df = get_positions()
        details_df['live_pnl'] = details_df['live_pnl'].clip(-100)

        try:
            run_log = open("logs/altcoin_bot.log").read()
        except:
            run_log = ""

        try:
            move_free = float(r.get('move_free').decode())
        except:
            move_free = 0
        
        try:
            close_and_rebalance = float(r.get('close_and_rebalance').decode())
        except:
            close_and_rebalance = 0

        try:
            close_and_main = float(r.get('close_and_main').decode())
        except:
            close_and_main = 0

        try:
            enter_now = float(r.get('enter_now').decode())
        except:
            enter_now = 0

        try:
            sub_account = float(r.get('sub_account').decode())
        except:
            sub_account = 0

        try:
            alt_starting_capital = float(r.get('alt_starting_capital').decode())
        except:
            alt_starting_capital = 0

        try:
            altcoin_close = r.get('altcoin_close').decode()
        except:
            altcoin_close = ""

        backtest_pnl = round((details_df['backtest_pnl'] * details_df['allocation']).sum(), 2)
        

        details = get_long_short_details(details_df)

        total_balance = round(details_df['ftx_balance'].sum(), 2)

        live_pnl = round(round((total_balance-alt_starting_capital)/alt_starting_capital, 4) * 100, 2)

        
        check_days=[3,5,10,15,20,25]

        config_df = pd.read_csv(config_file)
        backtest_details = {}
        
        for subalgo, rows in config_df.groupby('subalgo'):
            porfolios = pd.read_csv("data/altcoin_port_{}.csv".format(subalgo))
            porfolios = porfolios.set_index('Date')
            ret = porfolios.sum(axis=1)            
            backtest_details['{} EOD Backtest Return'.format(subalgo)] = round(((ret.iloc[-1] - ret.iloc[0])/ret.iloc[0]) * 100, 2)


        return render(request, "frontend_interface/altcoin_index.html", {'details_df': details_df.T.to_dict(), 'backtest_details': backtest_details, 'altcoin_close': altcoin_close,'backtest_pnl': backtest_pnl, 'live_pnl': live_pnl, 'config': config, 'trade_methods': altcoin_methods, 'csv_file': csv_file, 'run_log': run_log, 'move_free': move_free, 'close_and_rebalance': close_and_rebalance, 'close_and_main': close_and_main, 'enter_now': enter_now, 'sub_account': sub_account, 'details': details, 'total_balance': total_balance, 'alt_starting_capital': alt_starting_capital})
    else:
        return HttpResponseRedirect('/login')

def ratio_interface(request):
    if request.user.is_authenticated:
        config_file = 'algos/ratio/config.csv'
        r = redis.Redis(host='localhost', port=6379, db=0)

        if request.POST:
            dic = request.POST.dict()
            if 'ETHBTC[allocation]' in dic:
                dic.pop('csrfmiddlewaretoken', None)
                curr_df = pd.DataFrame()

                for idx, value in dic.items():
                    splitted = idx.split("[")
                    exchange = splitted[0]
                    column = splitted[1].replace("]", "")
                    
                    curr_df = curr_df.append(pd.Series({'name': exchange, 'column': column, 'value': value}), ignore_index=True)

                new_exchanges = {}

                for exchange, exchange_values in curr_df.groupby('name'):
                    new_exchanges[exchange] = {}
                    
                    for idx, row in exchange_values.iterrows():
                        new_exchanges[exchange][row['column']] = row['value']

                new_exchanges = pd.DataFrame(new_exchanges)
                new_exchanges = new_exchanges.T.reset_index().rename(columns={'index': 'name'})
                old_exchanges = pd.read_csv(config_file)
                final_exchanges = old_exchanges[list(set(old_exchanges.columns) - set(new_exchanges.columns)) + ['name']].merge(new_exchanges, on='name')
                final_exchanges = final_exchanges[old_exchanges.columns]
                final_exchanges.to_csv(config_file, index=None)

            elif 'csv_file' in dic:
                open(config_file, 'w').write(dic['csv_file'])
            elif 'move_free_form' in dic:
                if 'move_free_ratio' in dic:
                    r.set('move_free_ratio', 1)
                else:
                    r.set('move_free_ratio', 0)

            elif 'enable_close_and_main_form' in dic:
                if 'close_and_main_ratio' in dic:
                    r.set('close_and_main_ratio', 1)
                else:
                    r.set('close_and_main_ratio', 0)

            elif 'enable_close_and_rebalance_form' in dic:
                if 'close_and_rebalance_ratio' in dic:
                    r.set('close_and_rebalance_ratio', 1)
                else:
                    r.set('close_and_rebalance_ratio', 0)
            elif 'enter_now_form' in dic:
                if 'enter_now_ratio' in dic:
                    r.set('enter_now_ratio', 1)
                else:
                    r.set('enter_now_ratio', 0)
            elif 'sub_account_form' in dic:
                if 'sub_account_ratio' in dic:
                    r.set('sub_account_ratio', 1)
                else:
                    r.set('sub_account_ratio', 0)
            elif 'ratio_starting_capital' in dic:
                r.set('ratio_starting_capital', dic['ratio_starting_capital'])
        
        config_df = pd.read_csv(config_file)
        config_df = config_df.round(4)

        config = config_df.set_index('name').T.to_dict()

        csv_file = open(config_file, 'r').read()

        details_df = get_ratio_positions()
        details_df['live_pnl'] = details_df['live_pnl'].clip(-100)

        try:
            run_log = open("logs/ratio_bot.log").read()
        except:
            run_log = ""

        try:
            move_free_ratio = float(r.get('move_free_ratio').decode())
        except:
            move_free_ratio = 0
        
        try:
            close_and_rebalance_ratio = float(r.get('close_and_rebalance_ratio').decode())
        except:
            close_and_rebalance_ratio = 0

        try:
            close_and_main_ratio = float(r.get('close_and_main_ratio').decode())
        except:
            close_and_main_ratio = 0

        try:
            enter_now_ratio = float(r.get('enter_now_ratio').decode())
        except:
            enter_now_ratio = 0

        try:
            sub_account_ratio = float(r.get('sub_account_ratio').decode())
        except:
            sub_account_ratio = 0

        try:
            ratio_starting_capital = float(r.get('ratio_starting_capital').decode())
        except:
            ratio_starting_capital = 0

        backtest_pnl = round((details_df['backtest_pnl'] * details_df['allocation']).sum(), 2)
        
        details = get_long_short_details(details_df)
        
        btc_balance = round(details_df['binance_balance'].sum(), 4)
        total_balance = round((details_df['btc_price'] * details_df['binance_balance']).sum(), 2)

        live_pnl = round(round((total_balance-ratio_starting_capital)/ratio_starting_capital, 4) * 100, 2)

        details_df['binance_balance'] = details_df['binance_balance'].round(5)

        porfolios = pd.read_csv("data/ratio_port.csv")
        check_days=[1,2,3,5,6,7,8,9,10,15,20,25]

        porfolios = porfolios.set_index('Date')
        ret = porfolios.sum(axis=1)

        backtest_details = {}

        for d in check_days:
            if len(ret) > d:
                backtest_details['{} Day Return'.format(d)] = round(((ret.iloc[d] - ret.iloc[0])/ret.iloc[0]) * 100, 2)

        backtest_details['EOD Backtest Return'] = round(((ret.iloc[-1] - ret.iloc[0])/ret.iloc[0]) * 100, 2)

        return render(request, "frontend_interface/ratio_index.html", {'details_df': details_df.T.to_dict(), 'backtest_details': backtest_details, 'backtest_pnl': backtest_pnl, 'live_pnl': live_pnl, 'config': config, 'trade_methods': altcoin_methods, 'csv_file': csv_file, 'run_log': run_log, 'move_free_ratio': move_free_ratio, 'close_and_rebalance_ratio': close_and_rebalance_ratio, 'close_and_main_ratio': close_and_main_ratio, 'enter_now_ratio': enter_now_ratio, 'sub_account_ratio': sub_account_ratio, 'details': details, 'btc_balance': btc_balance, 'total_balance': total_balance, 'ratio_starting_capital': ratio_starting_capital })
    else:
        return HttpResponseRedirect('/login')

def csv_downloader(request):
    if request.user.is_authenticated:
        get = request.GET.dict()
        df = pd.read_csv('data/{}.csv'.format(get['fname']))
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename={}.csv'.format(get['fname'])
        df.to_csv(path_or_buf=response,index=None)
        return response

def show_trades(request):
    if request.user.is_authenticated:
        get = request.GET.dict()
        today = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")

        if get['type'] == 'trade':
            file = "data/{}_{}_trades.csv".format(today, get['exchange'])
        elif get['type'] == 'funding':
            file = "data/{}_{}_fundings.csv".format(today, get['exchange'])
        elif get['type'] == 'summary':
            file = "data/{}_{}_summary.json".format(today, get['exchange'])


        name = file.split("/")[-1]
        ext = name.split(".")[-1]
        response = HttpResponse(content_type='text/{}'.format(ext))
        response['Content-Disposition'] = 'attachment; filename={}'.format(name)
        return response

def interface(request):
    if request.user.is_authenticated:
        return render(request, "frontend_interface/interface.html")
    else:
        return HttpResponseRedirect('/login')


def vol_trend_interface(request):
    if request.user.is_authenticated:
        r = redis.Redis(host='localhost', port=6379, db=0)

        if request.POST:
            dic = request.POST.dict()
            if 'MOVE_mult' in dic:
                r.set('MOVE_mult', dic['MOVE_mult'])
                r.set('MOVE_method', dic['MOVE_method'])

                r.set('PERP_mult', dic['PERP_mult'])
                r.set('PERP_method', dic['PERP_method'])
            elif 'buy_missed_form' in dic:
                if 'buy_missed_perp' in dic:
                    r.set('buy_missed_perp', 1)
                    r.set('perp_long_or_short', dic['perp_long_or_short'])
                    r.set('price_perp', dic['price_perp'])
                else:
                    r.set('buy_missed_perp', 0)
                    r.set('perp_long_or_short', 0)
                    r.set('price_perp', 0)

                if 'buy_missed_move' in dic:
                    r.set('buy_missed_move', 1)
                    r.set('move_long_or_short', dic['move_long_or_short'])
                    r.set('price_move', dic['price_move'])
                else:
                    r.set('buy_missed_move', 0)
                    r.set('move_long_or_short', 0)
                    r.set('price_move', 0)

            elif 'override_form' in dic:
                if 'override_perp' in dic:
                    r.set('override_perp', 1)
                    r.set('perp_override_direction', dic['perp_override_direction'])
                else:
                    r.set('override_perp', 0)
                    r.set('perp_override_direction', 0)

                if 'override_move' in dic:
                    r.set('override_move', 1)
                    r.set('move_override_direction', dic['move_override_direction'])
                else:
                    r.set('override_move', 0)
                    r.set('move_override_direction', 0)
                
            elif 'enable_close_and_stop_form' in dic:
                if 'enable_per_close_and_stop' in dic:
                    r.set('enable_per_close_and_stop', 1)
                else:
                    r.set('enable_per_close_and_stop', 0)

                if 'enable_move_close_and_stop' in dic:
                    r.set('enable_move_close_and_stop', 1)
                else:
                    r.set('enable_move_close_and_stop', 0)

                if 'stop_perp' in dic:
                    r.set('stop_perp', 1)
                else:
                    r.set('stop_perp', 0)
                
                if 'stop_move' in dic:
                    r.set('stop_move', 1)
                else:
                    r.set('stop_move', 0)
            elif 'vol_trend_starting_capital' in dic:
                r.set('vol_trend_starting_capital', dic['vol_trend_starting_capital'])

        try:
            vol_trend_starting_capital = float(r.get('vol_trend_starting_capital').decode())
        except:
            vol_trend_starting_capital = 0

        details_df, balances = get_position_balance()

        pars = {}

        for var in ['MOVE_mult', 'PERP_mult', 'buy_missed_perp', 'perp_long_or_short', 'price_perp', 'buy_missed_move', 'move_long_or_short', 'price_move', 'override_perp', 'perp_override_direction', 'override_move', 'move_override_direction', 'enable_per_close_and_stop', 'enable_move_close_and_stop', 'stop_perp', 'stop_move']:
            try:
                pars[var] = float(r.get(var).decode())
            except:
                pars[var] = 0

        for var in ['MOVE_method', 'PERP_method']:
            try:
                pars[var] = r.get(var).decode()
            except:
                pars[var] = "now"
        
        try:
            run_log = open("logs/vol_trend_bot.log").read()
        except:
            run_log = ""

        trade_methods = ['attempt_limit', '5sec_average', '10sec_average', '1min_average', '10min_average', 'now']

        
        total_balance = round(details_df[details_df['name'].str.contains('MOVE')].iloc[0]['ftx_balance'] + details_df[details_df['name'].str.contains('PERP')].iloc[0]['ftx_balance'], 2)

        live_pnl = round(round((total_balance-vol_trend_starting_capital)/vol_trend_starting_capital, 4) * 100, 2)

        return render(request, "frontend_interface/vol_index.html", {'details_df': details_df.T.to_dict(), 'balances': balances, 'pars': pars, 'run_log': run_log, 'trade_methods': trade_methods, 'total_balance': total_balance, 'vol_trend_starting_capital': vol_trend_starting_capital, 'live_pnl': live_pnl})
    else:
        return HttpResponseRedirect('/login')

def chadlor_interface(request):

    r = redis.Redis(host='localhost', port=6379, db=0)

    
    if request.user.is_authenticated:
        if request.POST:
            dic = request.POST.dict()

            if 'mult' in dic:
                parameters = json.load(open('algos/chadlor/parameters.json'))
                new_pars = pd.Series(dic)[parameters.keys()].to_dict()

                new_pars['mult'] = float(new_pars['mult'])
                new_pars['position_since'] = float(new_pars['position_since'])
                new_pars['bigger_than'] = float(new_pars['bigger_than'])
                new_pars['check_from'] = float(new_pars['check_from'])
                new_pars['loss_cap'] = float(new_pars['loss_cap'])
                new_pars['stop_percentage'] = float(new_pars['stop_percentage'])

                with open('algos/chadlor/parameters.json', 'w') as f:
                    json.dump(new_pars, f)
        
        
        parameters = json.load(open('algos/chadlor/parameters.json'))

        details = {}

        try:
            details['position_since'] = int(r.get('chadlor_position_since').decode())
        except:
            details['position_since'] = 0

        try:
            details['avgEntryPrice'] = round(float(r.get('chadlor_avgEntryPrice').decode()), 2)
        except:
            details['avgEntryPrice'] = 0

        try:
            details['pos_size'] = round(float(r.get('chadlor_pos_size').decode()), 2)
        except:
            details['pos_size'] = 0

        try:
            details['balance'] = round(float(r.get('chadlor_balance').decode()), 4)
        except:
            details['balance'] = 0

        try:
            details['coinbase_price'] = round(float(r.get('coinbase_price').decode()), 2)
        except:
            details['coinbase_price'] = 0

        try:
            details['bitmex_price'] = round(float(r.get('bitmex_price').decode()), 2)
        except:
            details['bitmex_price'] = 0

        try:
            details['price_diff'] = round(details['coinbase_price']/details['bitmex_price'], 4)
        except:
            details['price_diff'] = 0

        try:
            details['pnl_percentage'] = round(((details['bitmex_price'] - details['avgEntryPrice'])/details['avgEntryPrice']) * 100 * parameters['mult'], 2)
        except:
            details['pnl_percentage'] = 0
        
        print(details)

        try:
            run_log = open("logs/chadlor_bot.log").read()
        except:
            run_log = ""
        
        return render(request, "frontend_interface/chadlor_index.html", {'parameters': parameters, 'details': details, 'run_log': run_log})

    else:
        return HttpResponseRedirect('/login')

def daddy_core(request, symbol, pars_file, config_file):
    r = redis.Redis(host='localhost', port=6379, db=0)
    pars_folder = pars_file.replace(".json", "")

    if request.POST:
        dic = request.POST.dict()

        if 'mult' in dic:
            parameters = json.load(open(pars_file))
            new_pars = pd.Series(dic)[parameters.keys()].to_dict()

            new_pars['mult'] = float(new_pars['mult'])
            new_pars['percentage_large'] = float(new_pars['percentage_large'])
            new_pars['buy_percentage_large'] = float(new_pars['buy_percentage_large'])
            new_pars['macd'] = float(new_pars['macd'])
            new_pars['rsi'] = float(new_pars['rsi'])
            new_pars['previous_days'] = float(new_pars['previous_days'])
            new_pars['position_since'] = float(new_pars['position_since'])
            new_pars['position_since_diff'] = float(new_pars['position_since_diff'])
            new_pars['change'] = float(new_pars['change'])
            new_pars['pnl_percentage'] = float(new_pars['pnl_percentage'])
            new_pars['close_percentage'] = float(new_pars['close_percentage'])
            new_pars['profit_macd'] = float(new_pars['profit_macd'])
            new_pars['stop_percentage'] = float(new_pars['stop_percentage'])
            new_pars['profit_cap'] = float(new_pars['profit_cap'])
            new_pars['name'] = dic['pars_name']

            with open(pars_file, 'w') as f:
                json.dump(new_pars, f)

            with open('{}/{}.json'.format(pars_folder, new_pars['name']), 'w') as f:
                json.dump(new_pars, f)

        elif 'update_exchanges_form' in dic:
            dic.pop('csrfmiddlewaretoken', None)
            curr_df = pd.DataFrame()

            for idx, value in dic.items():
                if idx != "update_exchanges_form":
                    splitted = idx.split("[")
                    name = splitted[0]
                    column = splitted[1].replace("]", "")
                    
                    curr_df = curr_df.append(pd.Series({'name': name, 'column': column, 'value': value}), ignore_index=True)

            new_exchanges = {}

            for exchange, exchange_values in curr_df.groupby('name'):
                new_exchanges[exchange] = {}
                
                for idx, row in exchange_values.iterrows():
                    new_exchanges[exchange][row['column']] = row['value']

            new_exchanges = pd.DataFrame(new_exchanges)
            new_exchanges = new_exchanges.T.reset_index().rename(columns={'index': 'name'})
            old_exchanges = pd.read_csv(config_file)
            final_exchanges = old_exchanges[list(set(old_exchanges.columns) - set(new_exchanges.columns)) + ['name']].merge(new_exchanges, on='name')
            final_exchanges = final_exchanges[old_exchanges.columns]
            final_exchanges.to_csv(config_file, index=None)


        elif 'csv_file' in dic:
            open(config_file, 'w').write(dic['csv_file'])

        #this needs to be diff now
        elif 'trend_start_date' in dic:
            r.set('trend_start_date_{}'.format(symbol), dic['trend_start_date'])

    
    parameters = json.load(open(pars_file))

    exchanges = pd.read_csv(config_file)

    new_df = []

    for idx, row in exchanges.iterrows():      
        try:
            position_since = round(float(r.get('{}_position_since'.format(row['name'])).decode()), 2)
        except:
            position_since = 0
        
        try:
            avgEntryPrice = round(float(r.get('{}_avgEntryPrice'.format(row['name'])).decode()), 2)
        except:
            avgEntryPrice = 0

        try:
            pos_size = round(float(r.get('{}_pos_size'.format(row['name'])).decode()), 2)
        except:
            pos_size = 0
        
        try:
            pnl_percentage = round(((float(r.get('{}_{}_best_ask'.format(row['exchange'], row['cryptofeed_symbol'].lower())).decode())- float(avgEntryPrice))/float(avgEntryPrice)) * 100 * parameters['mult'], 2)
        except:
            pnl_percentage = 0

        try:
            free_balance = round(float(r.get('{}_balance'.format(row['name'])).decode()), 3)
        except:
            free_balance = 0
        
        row['position_since'] = position_since
        row['avgEntryPrice'] = avgEntryPrice
        row['pnl_percentage'] = pnl_percentage
        row['pos_size'] = pos_size

        row['balance'] = free_balance
        
        to_append = 1

        if str(request.user) != "warproxxx":
            if "bitmex_" in row['name']:
                to_append = 0

        if to_append == 1:
            new_df.append(row.to_dict())


    exchanges = pd.read_csv(config_file)
    exchanges = exchanges[['exchange', 'name', 'ccxt_symbol', 'symbol', 'cryptofeed_symbol', 'trade', 'max_trade', 'buy_method', 'sell_method']]

    exchanges = exchanges.set_index('name').T.to_dict()
    new_df = pd.DataFrame(new_df).set_index('name').T.to_dict()
    csv_file = open(config_file, 'r').read()

    try:
        run_log = open("logs/{}_daddy_bot.log".format(symbol)).read()
    except:
        run_log = ""


    all_parameters = {}

    for f in glob("{}/*".format(pars_folder)):
        all_parameters[f.split("/")[-1].replace(".json", "")] = json.load(open(f))

    all_parameters_json = json.dumps(all_parameters)

    try:
        trend_start_date = r.get('trend_start_date_{}'.format(symbol)).decode()
    except:
        trend_start_date = ""

    trades = pd.read_csv('data/{}USD_trades.csv'.format(symbol))[['Date', 'Type', 'Price']]
    trades = trades[-6:]
    trades = trades.round(3)
    trades.loc[-1] = trades.columns
    trades.index = trades.index + 1 
    trades = trades.sort_index()
    trades = trades.T.to_dict()

    features = pd.read_csv('data/{}_features.csv'.format(symbol))[['timestamp', 'macd', 'rsi', 'close', 'change', 'percentage_large', 'buy_percentage_large']]
    features = features[-6:]
    features = features.round(3)
    features.loc[-1] = features.columns
    features.index = features.index + 1 
    features = features.sort_index()
    features = features.T.to_dict()

    return {'all_parameters': all_parameters, 'symbol': symbol, 'all_parameters_json': all_parameters_json, 'trades': trades, 'parameters': parameters, 'exchanges': exchanges, 'new_df': new_df, 'trade_methods': trade_methods, 'csv_file': csv_file, 'run_log': run_log, 'features': features, "trend_start_date": trend_start_date}
    

def eth_daddy_interface(request):
    if request.user.is_authenticated:
        pars = daddy_core(request, 'ETH', 'algos/eth_daddy/parameters.json', 'algos/eth_daddy/exchanges.csv')
        return render(request, "frontend_interface/daddy_index.html", pars)
    else:
        return HttpResponseRedirect('/login')

def doge_daddy_interface(request):
    if request.user.is_authenticated:
        pars = daddy_core(request, 'DOGE', 'algos/doge_daddy/parameters.json', 'algos/doge_daddy/exchanges.csv')
        return render(request, "frontend_interface/daddy_index.html", pars)
    else:
        return HttpResponseRedirect('/login')

def xbt_daddy_interface(request):
    if request.user.is_authenticated:
        pars = daddy_core(request, 'XBT', 'algos/xbt_daddy/parameters.json', 'algos/xbt_daddy/exchanges.csv')
        return render(request, "frontend_interface/daddy_index.html", pars)
    else:
        return HttpResponseRedirect('/login')

def ltc_daddy_interface(request):
    if request.user.is_authenticated:
        pars = daddy_core(request, 'LTC', 'algos/ltc_daddy/parameters.json', 'algos/ltc_daddy/exchanges.csv')
        return render(request, "frontend_interface/daddy_index.html", pars)
    else:
        return HttpResponseRedirect('/login')

def bch_daddy_interface(request):
    if request.user.is_authenticated:
        pars = daddy_core(request, 'BCH', 'algos/bch_daddy/parameters.json', 'algos/bch_daddy/exchanges.csv')
        return render(request, "frontend_interface/daddy_index.html", pars)
    else:
        return HttpResponseRedirect('/login')

def daddy_interface(request):
    if request.user.is_authenticated:
        r = redis.Redis(host='localhost', port=6379, db=0)
        bots = [x.split("/")[1] for x in glob('algos/*') if "_daddy" in x]
        print(bots)
        positions = {}
        new_df = []

        for bot in bots:
            exchanges = pd.read_csv('algos/{}/exchanges.csv'.format(bot))

            for idx, row in exchanges.iterrows():             
                if row['trade'] == 1: 
                    try:
                        position_since = round(float(r.get('{}_position_since'.format(row['name'])).decode()), 2)
                    except:
                        position_since = 0
                    
                    try:
                        avgEntryPrice = round(float(r.get('{}_avgEntryPrice'.format(row['name'])).decode()), 2)
                    except:
                        avgEntryPrice = 0

                    try:
                        pos_size = round(float(r.get('{}_pos_size'.format(row['name'])).decode()), 2)
                    except:
                        pos_size = 0
                    
                    try:
                        parameters = json.load(open('algos/{}/parameters.json'.format(bot)))
                        pnl_percentage = round(((float(r.get('{}_{}_best_ask'.format(row['exchange'], row['cryptofeed_symbol'].lower())).decode())- float(avgEntryPrice))/float(avgEntryPrice)) * 100 * parameters['mult'], 2)
                    except:
                        pnl_percentage = 0

                    try:
                        free_balance = round(float(r.get('{}_balance'.format(row['name'])).decode()), 3)
                    except:
                        free_balance = 0
                    
                    row['position_since'] = position_since
                    row['avgEntryPrice'] = avgEntryPrice
                    row['pnl_percentage'] = pnl_percentage
                    row['pos_size'] = pos_size
                    row['balance'] = free_balance
                    row['url'] = bot

                    new_df.append(row.to_dict())


        new_df = pd.DataFrame(new_df).set_index('name').T.to_dict()

        all_trades = {}

        for bot in bots:
            symbol = bot.replace("_daddy", "").upper()
            file = 'data/{}USD_trades.csv'.format(symbol)

            if os.path.isfile(file):
                trades = pd.read_csv(file)[['Date', 'Type', 'Price']]
                trades = trades[-6:]
                trades = trades.round(3)
                trades.loc[-1] = trades.columns
                trades.index = trades.index + 1 
                trades = trades.sort_index()
                trades = trades.T.to_dict()
                all_trades[bot] = trades

        print(all_trades)
        try:
            run_log = open("logs/daddy_live_trader.log").read()
        except:
            run_log = ""

        return render(request, "frontend_interface/daddy.html", {'new_df': new_df, 'run_log': run_log, 'all_trades': all_trades})
    else:
        return HttpResponseRedirect('/login')

def delete(request):
    req = request.GET.dict()
    file = "algos/daddy/parameters/" + req['name'] + ".json"

    if os.path.isfile(file):
        os.remove(file)
    return HttpResponseRedirect('/daddy')

def clearLog(request):
    req = request.GET.dict()
    try:
        file = "logs/" + req['from'] + "_bot.log"
        open(file, 'w').close()
    except:
        pass
    
    return HttpResponseRedirect('/' + req['from'])

def addParms(request):
    #update parameters.json too if it is current
    req = request.GET.dict()

    if 'key' in req:
        if req['key'] == 'cQyv3TuVGc9m7KTQ66q33hcjtyjvMD9RsPBogkYc4idhMDQhpcNUfZHRBMrepCRR7XdAPD9TYbMMU5Dr':
            try:
                folder = req['folder']
            except:
                folder = 'daddy'

            parameters = json.load(open('algos/{}/parameters.json'.format(folder)))
            new_pars = {}
            new_pars['mult'] = float(req['mult'])
            new_pars['percentage_large'] = float(req['p_large'])
            new_pars['buy_percentage_large'] = float(req['bp_lar'])
            new_pars['macd'] = float(req['macd'])
            new_pars['rsi'] = float(req['rsi'])
            new_pars['previous_days'] = float(float(req['prev_d']))
            new_pars['position_since'] = float(float(req['pos_s']))
            new_pars['position_since_diff'] = float(float(req['pos_diff']))
            new_pars['change'] = float(req['change'])
            new_pars['pnl_percentage'] = float(req['pnl_per'])
            new_pars['close_percentage'] = float(req['close_p'])
            new_pars['profit_macd'] = float(req['p_macd'])
            new_pars['stop_percentage'] = float(req['stop'])
            new_pars['profit_cap'] = float(req['profit_cap'])
            new_pars['name'] = req['name']
            

            with open('algos/{}/parameters/{}.json'.format(folder, new_pars['name']), 'w') as f:
                json.dump(new_pars, f)

            parameters = json.load(open('algos/{}/parameters.json'.format(folder)))
            
            if parameters['name'] == req['name']:
                with open('algos/{}/parameters.json'.format(folder), 'w') as f:
                    json.dump(new_pars, f)

            return HttpResponseRedirect('/{}'.format(folder))
        else:
            return HttpResponse("Fuck Off")

    return HttpResponse("Fuck Off")

def adminLogout(request):
    del request.session['Adminlogin']
    return HttpResponseRedirect('/')