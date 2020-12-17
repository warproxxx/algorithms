from django.shortcuts import render
import bcrypt
from shutil import copy
import pandas as pd
from .forms import adminLoginForm
from django.http import HttpResponseRedirect, HttpResponse 
from algos.daddy.defines import trade_methods
import json
import redis
from glob import glob
import os
from shutil import copy

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
                config['enabled'] = int(r.get('{}_enabled'.format(config['code_name'])).decode())
            except:
                config['enabled'] = 0

            algo_details.append(config)

        print(algo_details)
        return render(request, "frontend_interface/index.html", {'algos': algo_details})
    else:
        return HttpResponseRedirect('/login')

def reverse_status(request):
    if 'Adminlogin' in request.session:
        r = redis.Redis(host='localhost', port=6379, db=0)
        var_name = request.GET['code_name'] + "_enabled"
        try:
            old_status = int(r.get(var_name).decode())
        except:
            old_status = 0

        new_status = 1 - old_status
        r.set(var_name, new_status)

        return HttpResponseRedirect('/')
    else:
        return HttpResponseRedirect('/login')

def vol_trend_interface(request):
    if 'Adminlogin' in request.session:
        move_prediction = pd.read_csv('data/trades_move.csv').iloc[-1][['Date', 'Type', 'Data']].to_dict()
        perp_prediction = pd.read_csv('data/trades_perp.csv').iloc[-1][['Date', 'Type', 'Data']].to_dict()

        return render(request, "frontend_interface/vol_index.html", {'move_prediction': move_prediction, 'perp_prediction': perp_prediction})
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

                new_pars['mult'] = int(new_pars['mult'])
                new_pars['percentage_large'] = float(new_pars['percentage_large'])
                new_pars['buy_percentage_large'] = float(new_pars['buy_percentage_large'])
                new_pars['macd'] = int(new_pars['macd'])
                new_pars['rsi'] = float(new_pars['rsi'])
                new_pars['previous_days'] = int(new_pars['previous_days'])
                new_pars['position_since'] = int(new_pars['position_since'])
                new_pars['position_since_diff'] = int(new_pars['position_since_diff'])
                new_pars['change'] = float(new_pars['change'])
                new_pars['pnl_percentage'] = float(new_pars['pnl_percentage'])
                new_pars['close_percentage'] = int(new_pars['close_percentage'])
                new_pars['profit_macd'] = int(new_pars['profit_macd'])
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

        plotly_file = 'data/plot.html'
        new_plotly_file = 'frontend_interface/static/plotly.html'
        copy(plotly_file, new_plotly_file)

        plotly_file = 'data/plot_unbiased.html'
        new_plotly_file = 'frontend_interface/static/plot_unbiased.html'
        copy(plotly_file, new_plotly_file)

        try:
            buy_missed = int(r.get('buy_missed').decode())
        except:
            buy_missed = 0

        try:
            buy_at = int(r.get('buy_at').decode())
        except:
            buy_at = 0
        
        try:
            close_and_stop = int(r.get('close_and_stop').decode())
        except:
            close_and_stop = 0

        try:
            stop_trading = int(r.get('stop_trading').decode())
        except:
            stop_trading = 0
            
        return render(request, "frontend_interface/daddy_index.html", {'all_parameters': all_parameters, 'all_parameters_json': all_parameters_json, 'parameters': parameters, 'exchanges': exchanges, 'new_df': new_df, 'trade_methods': trade_methods, 'csv_file': csv_file, 'run_log': run_log, 'buy_missed': buy_missed, 'buy_at': buy_at, 'close_and_stop': close_and_stop, 'stop_trading': stop_trading})

    else:
        return HttpResponseRedirect('/login')

def delete(request):
    req = request.GET.dict()
    file = "algos/daddy/parameters/" + req['name'] + ".json"

    if os.path.isfile(file):
        os.remove(file)
    return HttpResponseRedirect('/daddy')

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
            new_pars['previous_days'] = int(float(req['prev_d']))
            new_pars['position_since'] = int(float(req['pos_s']))
            new_pars['position_since_diff'] = int(float(req['pos_diff']))
            new_pars['change'] = float(req['change'])
            new_pars['pnl_percentage'] = float(req['pnl_per'])
            new_pars['close_percentage'] = float(req['close_p'])
            new_pars['profit_macd'] = float(req['p_macd'])
            new_pars['stop_percentage'] = float(req['stop'])
            new_pars['name'] = req['name']

            with open('algos/daddy/parameters/{}.json'.format(new_pars['name']), 'w') as f:
                json.dump(new_pars, f)

            return HttpResponseRedirect('/')
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
                pword  = form.cleaned_data.get('password').encode('UTF-8')

                actual_pword = b'$2b$12$S3Z3APk4nUOu./kra7MdP./7sElMekLV2neWc91ehoZ2ykfXODb7S'
                
                if uname =="warproxxx" and bcrypt.checkpw(pword, actual_pword):
                        request.session["Adminlogin"] = "True"
                        return HttpResponseRedirect('/')
                else:
                    return render(request, template_name, {"form":adminLoginForm, "msg":"Incorrect Username or Password"})

def adminLogout(request):
    del request.session['Adminlogin']
    return HttpResponseRedirect('/')