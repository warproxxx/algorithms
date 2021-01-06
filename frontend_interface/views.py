from django.shortcuts import render
from django.http import HttpResponseRedirect, HttpResponse 
from .forms import adminLoginForm

import os
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
    if 'Adminlogin' in request.session:
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
        details_df = get_ratio_positions()
        ratio_pnl = round((details_df['live_pnl'] * details_df['allocation']).sum(), 2)

        details_df = get_positions()
        altcoin_pnl = round((details_df['live_pnl'] * details_df['allocation']).sum(), 2)

        details_df, balances = get_position_balance()    
        bitcoin_pnl = details_df[details_df['name'] == 'BTC-PERP'].iloc[0]['live_pnl']

        amount = 1910
        total_pnl = (0.37*-25 + 0.44*altcoin_pnl + 0.19*bitcoin_pnl)/100

        if total_pnl > 0:
            total_pnl = total_pnl/2

        if total_pnl < -0.04:
            total_pnl = total_pnl/3

        amount = round(amount * (1 + total_pnl), 2)

        return HttpResponse(amount)
    except:
        return HttpResponse("error")

def reverse_status(request):
    if 'Adminlogin' in request.session:
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
    if 'Adminlogin' in request.session:
        config_file = 'algos/altcoin/config.csv'
        r = redis.Redis(host='localhost', port=6379, db=0)

        if request.POST:
            dic = request.POST.dict()
            if 'BTC-PERP[allocation]' in dic:
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
        
        config_df = pd.read_csv(config_file)
        config_df = config_df.round(4)

        config = config_df.set_index('name').T.to_dict()

        csv_file = open(config_file, 'r').read()

        details_df = get_positions()

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

        backtest_pnl = round((details_df['backtest_pnl'] * details_df['allocation']).sum(), 2)
        live_pnl = round((details_df['live_pnl'] * details_df['allocation']).sum(), 2)

        details = get_long_short_details(details_df)

        return render(request, "frontend_interface/altcoin_index.html", {'details_df': details_df.T.to_dict(), 'backtest_pnl': backtest_pnl, 'live_pnl': live_pnl, 'config': config, 'trade_methods': altcoin_methods, 'csv_file': csv_file, 'run_log': run_log, 'move_free': move_free, 'close_and_rebalance': close_and_rebalance, 'close_and_main': close_and_main, 'enter_now': enter_now, 'sub_account': sub_account, 'details': details})
    else:
        return HttpResponseRedirect('/login')

def ratio_interface(request):
    if 'Adminlogin' in request.session:
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
        
        config_df = pd.read_csv(config_file)
        config_df = config_df.round(4)

        config = config_df.set_index('name').T.to_dict()

        csv_file = open(config_file, 'r').read()

        details_df = get_ratio_positions()

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

        backtest_pnl = round((details_df['backtest_pnl'] * details_df['allocation']).sum(), 2)
        live_pnl = round((details_df['live_pnl'] * details_df['allocation']).sum(), 2)

        details = get_long_short_details(details_df)
        

        return render(request, "frontend_interface/ratio_index.html", {'details_df': details_df.T.to_dict(), 'backtest_pnl': backtest_pnl, 'live_pnl': live_pnl, 'config': config, 'trade_methods': altcoin_methods, 'csv_file': csv_file, 'run_log': run_log, 'move_free_ratio': move_free_ratio, 'close_and_rebalance_ratio': close_and_rebalance_ratio, 'close_and_main_ratio': close_and_main_ratio, 'enter_now_ratio': enter_now_ratio, 'sub_account_ratio': sub_account_ratio, 'details': details})
    else:
        return HttpResponseRedirect('/login')

def vol_trend_interface(request):
    if 'Adminlogin' in request.session:
        r = redis.Redis(host='localhost', port=6379, db=0)

        if request.POST:
            dic = request.POST.dict()
            if 'MOVE_mult' in dic:
                print(dic)
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

        


        return render(request, "frontend_interface/vol_index.html", {'details_df': details_df.T.to_dict(), 'balances': balances, 'pars': pars, 'run_log': run_log, 'trade_methods': trade_methods})
    else:
        return HttpResponseRedirect('/login')

def daddy_interface(request):

    r = redis.Redis(host='localhost', port=6379, db=0)

    if 'Adminlogin' in request.session:
        if request.POST:
            dic = request.POST.dict()

            if 'mult' in dic:
                #if file not exist make, else update

                parameters = json.load(open('algos/daddy/parameters.json'))
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
                new_pars['name'] = dic['pars_name']

                with open('algos/daddy/parameters.json', 'w') as f:
                    json.dump(new_pars, f)

                with open('algos/daddy/parameters/{}.json'.format(new_pars['name']), 'w') as f:
                    json.dump(new_pars, f)

            elif 'bitmex[trade]' in dic:
                dic.pop('csrfmiddlewaretoken', None)
                curr_df = pd.DataFrame()

                for idx, value in dic.items():
                    splitted = idx.split("[")
                    exchange = splitted[0]
                    column = splitted[1].replace("]", "")
                    
                    curr_df = curr_df.append(pd.Series({'exchange': exchange, 'column': column, 'value': value}), ignore_index=True)

                new_exchanges = {}

                for exchange, exchange_values in curr_df.groupby('exchange'):
                    new_exchanges[exchange] = {}
                    
                    for idx, row in exchange_values.iterrows():
                        new_exchanges[exchange][row['column']] = row['value']

                new_exchanges = pd.DataFrame(new_exchanges)
                new_exchanges = new_exchanges.T.reset_index().rename(columns={'index': 'exchange'})
                old_exchanges = pd.read_csv('exchanges.csv')
                final_exchanges = old_exchanges[list(set(old_exchanges.columns) - set(new_exchanges.columns)) + ['exchange']].merge(new_exchanges, on='exchange')
                final_exchanges = final_exchanges[old_exchanges.columns]
                final_exchanges.to_csv('exchanges.csv', index=None)

            elif 'csv_file' in dic:
                open('exchanges.csv', 'w').write(dic['csv_file'])
            elif 'buy_missed_form' in dic:
                if 'buy_missed' in dic:
                    r.set('buy_missed', 1)
                    r.set('buy_at', dic['buy_at'])
                else:
                    r.set('buy_missed', 0)
                    r.set('buy_at', 0)

            elif 'enable_close_and_stop_form' in dic:
                if 'close_and_stop' in dic:
                    r.set('close_and_stop', 1)
                else:
                    r.set('close_and_stop', 0)
            elif 'stop_trading_form' in dic:
                if 'stop_trading' in dic:
                    r.set('stop_trading', 1)
                else:
                    r.set('stop_trading', 0)

        
        parameters = json.load(open('algos/daddy/parameters.json'))

        exchanges = pd.read_csv('exchanges.csv')

        new_df = []

        for idx, row in exchanges.iterrows():                
            try:
                position_since = round(float(r.get('{}_position_since'.format(row['exchange'])).decode()), 2)
            except:
                position_since = 0
            
            try:
                avgEntryPrice = round(float(r.get('{}_avgEntryPrice'.format(row['exchange'])).decode()), 2)
            except:
                avgEntryPrice = 0

            try:
                pos_size = round(float(r.get('{}_pos_size'.format(row['exchange'])).decode()), 2)
            except:
                pos_size = 0

            try:
                pnl_percentage = round(((float(r.get('{}_best_ask'.format(row['exchange'])).decode()) - float(avgEntryPrice))/float(avgEntryPrice)) * 100 * parameters['mult'], 2)
            except:
                pnl_percentage = 0

            try:
                free_balance = round(float(r.get('{}_balance'.format(row['exchange'])).decode()), 3)
            except:
                free_balance = 0
            
            row['position_since'] = position_since
            row['avgEntryPrice'] = avgEntryPrice
            row['pnl_percentage'] = pnl_percentage
            row['pos_size'] = pos_size

            row['balance'] = free_balance
                
            
            new_df.append(row.to_dict())


        exchanges = pd.read_csv('exchanges.csv')
        exchanges = exchanges[['exchange', 'ccxt_symbol', 'symbol', 'cryptofeed_symbol', 'trade', 'max_trade', 'buy_method', 'sell_method']]

        exchanges = exchanges.set_index('exchange').T.to_dict()
        new_df = pd.DataFrame(new_df).set_index('exchange').T.to_dict()
        csv_file = open('exchanges.csv', 'r').read()
        try:
            run_log = open("logs/daddy_bot.log").read()
        except:
            run_log = ""


        all_parameters = {}

        for f in glob("algos/daddy/parameters/*"):
            all_parameters[f.split("/")[-1].replace(".json", "")] = json.load(open(f))

        all_parameters_json = json.dumps(all_parameters)

        try:
            buy_missed = float(r.get('buy_missed').decode())
        except:
            buy_missed = 0

        try:
            buy_at = float(r.get('buy_at').decode())
        except:
            buy_at = 0
        
        try:
            close_and_stop = float(r.get('close_and_stop').decode())
        except:
            close_and_stop = 0

        try:
            stop_trading = float(r.get('stop_trading').decode())
        except:
            stop_trading = 0

        try:
            mex_trades =  open("data/mex_trades.csv").read()
        except:
            mex_trades = ""
        return render(request, "frontend_interface/daddy_index.html", {'all_parameters': all_parameters, 'all_parameters_json': all_parameters_json, 'parameters': parameters, 'exchanges': exchanges, 'new_df': new_df, 'trade_methods': trade_methods, 'csv_file': csv_file, 'run_log': run_log, 'mex_trades': mex_trades, 'buy_missed': buy_missed, 'buy_at': buy_at, 'close_and_stop': close_and_stop, 'stop_trading': stop_trading})

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
    req = request.GET.dict()

    if 'key' in req:
        if req['key'] == 'cQyv3TuVGc9m7KTQ66q33hcjtyjvMD9RsPBogkYc4idhMDQhpcNUfZHRBMrepCRR7XdAPD9TYbMMU5Dr':
            parameters = json.load(open('algos/daddy/parameters.json'))
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
            new_pars['name'] = req['name']

            with open('algos/daddy/parameters/{}.json'.format(new_pars['name']), 'w') as f:
                json.dump(new_pars, f)

            return HttpResponseRedirect('/daddy')
        else:
            return HttpResponse("Fuck Off")

    return HttpResponse("Fuck Off")

def adminLogin(request):
    template_name = 'frontend_interface/login.html'

    if request.method == 'GET':
        if not "Adminlogin" in request.session:
            return render(request, template_name, {"form":adminLoginForm})
        else:
            return HttpResponseRedirect('/')
    if request.method == 'POST':
            form = adminLoginForm(request.POST)
            if form.is_valid():
                uname  = form.cleaned_data.get("username")
                pword  = form.cleaned_data.get('password')

                username = os.getenv('DJANGO_USERNAME')
                actual_pword = os.getenv('DJANGO_PASSWORD')

                if uname == username and pword == actual_pword:
                        request.session["Adminlogin"] = "True"
                        return HttpResponseRedirect('/')
                else:
                    return render(request, template_name, {"form":adminLoginForm, "msg":"Incorrect Username or Password"})

def adminLogout(request):
    del request.session['Adminlogin']
    return HttpResponseRedirect('/')