import os
import datetime
import time
import sys
import random
import math
import warnings
import requests
import numpy as np
import pandas as pd
import talib
from pandas_datareader import data as wb
import yfinance as yf
import investpy
import cfscrape
import xlwings as xw
import pyautogui as py
import openpyxl
import subprocess

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

import zipfile
from bs4 import BeautifulSoup
import urllib.request


warnings.simplefilter('ignore')
pd.options.display.float_format = '{:.5f}'.format
fpass = r'C:\Users\****\Documents\docker-python\topix500' # 特徴量、予測結果保存ディレクトリ
tickers = pd.read_csv(rf'{fpass}\topix500.csv') # 株式銘柄コードファイル

period = ['6y', '1y', '60d']   # 特徴量元データ取得最大期間 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
std_period = '6d' # 特徴量元データ取得標準期間
interval = ['1d', '1h', '15m'] # 特徴量元データ間隔 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
message = ''

# LINE通知
def send_line(message): 
    line_notify_token = 'eKBO3BPM1UHjvPi7G9SQkbdiQn8FBeefX9V4GCCoECj'
    line_notify_api = 'https://notify-api.line.me/api/notify'
    payload = {'message': message}
    headers = {'Authorization': 'Bearer ' + line_notify_token}
    requests.post(line_notify_api, data=payload, headers=headers)

# FXデータ取得
def get_fx_data(tickers, period, interval):
    for p, i in zip(period, interval):
        
        ticker_list = [ticker for ticker in tickers.fxs if not pd.isnull(ticker)]
        code_list = [f'{code}=X' for code in ticker_list]
        
        if i != '1d':
            df_raw = yf.download(code_list, period=std_period, interval=i, auto_adjust=False, progress=False, ignore_tz=False)
            df_raw.index = df_raw.index.tz_convert(None)
        else:
            df_raw = yf.download(code_list, period=std_period, interval=i, auto_adjust=False, progress=False)
        df_raw.index.name = 'Date'
        
        for t in ticker_list:
            if os.path.exists(rf'{fpass}\{i}\{t}.csv'):
                df0 = pd.read_csv(rf'{fpass}\{i}\{t}.csv', index_col='Date')
                df0.index = pd.to_datetime(df0.index)

                df1 = df_raw.loc[:, pd.IndexSlice[:, f'{t}=X']]
                df1.columns = df1.columns.droplevel(1)
                df1 = df1.dropna(how='all')

                df2 = df0[:-1].combine_first(df1)
                df2.to_csv(rf'{fpass}\{i}\{t}.csv')
            else:
                if i != '1d':
                    df2 = yf.download(f'{t}=X', period=p, interval=i, auto_adjust=False, progress=False, ignore_tz=False)
                    df2.index = df2.index.tz_convert(None)
                else:
                    df2 = yf.download(f'{t}=X', period=p, interval=i, auto_adjust=False, progress=False)
                df2.index.name = 'Date'
                df2.to_csv(rf'{fpass}\{i}\{t}.csv')
    print('すべてのFXデータを取得しました。')

# 商品先物データ取得
def get_comodity_data(tickers, period, interval):
    for p, i in zip(period, interval):
        
        if i == '15m':
            continue
        ticker_list = [ticker for ticker in tickers.comodity if not pd.isnull(ticker)]
        code_list = [f'{code}=F' for code in ticker_list]
        
        if i != '1d':
            df_raw = yf.download(code_list, period=std_period, interval=i, auto_adjust=False, progress=False, ignore_tz=False)
            df_raw.index = df_raw.index.tz_convert(None)
        else:
            df_raw = yf.download(code_list, period=std_period, interval=i, auto_adjust=False, progress=False)
        df_raw.index.name = 'Date'
        
        for t in ticker_list:
            if os.path.exists(rf'{fpass}\{i}\{t}.csv'):
                df0 = pd.read_csv(rf'{fpass}\{i}\{t}.csv', index_col='Date')
                df0.index = pd.to_datetime(df0.index)

                df1 = df_raw.loc[:, pd.IndexSlice[:, f'{t}=F']]
                df1.columns = df1.columns.droplevel(1)
                df1.dropna(how='all')

                df2 = df0[:-1].combine_first(df1)
                df2.to_csv(rf'{fpass}\{i}\{t}.csv')
            else:
                if i != '1d':
                    df2 = yf.download(f'{t}=F', period=p, interval=i, auto_adjust=False, progress=False, ignore_tz=False)
                    df2.index = df2.index.tz_convert(None)
                else:
                    df2 = yf.download(f'{t}=F', period=p, interval=i, auto_adjust=False, progress=False)
                df2.index.name = 'Date'
                df2.to_csv(rf'{fpass}\{i}\{t}.csv')
    print('すべての商品先物データを取得しました。')

# 経済指標、ETFデータ取得
def get_index_data(tickers, period, interval):
    for p, i in zip(period, interval):
        
        ticker_list = [ticker for ticker in tickers.indexs if not pd.isnull(ticker)]
        code_list = [f'^{code}' for code in ticker_list]
        
        if i != '1d':
            df_raw = yf.download(code_list, period=std_period, interval=i, auto_adjust=False, progress=False, ignore_tz=False)
            df_raw.index = df_raw.index.tz_convert(None)
        else:
            df_raw = yf.download(code_list, period=std_period, interval=i, auto_adjust=False, progress=False)
        df_raw.index.name = 'Date'
        
        for t in ticker_list:
            if os.path.exists(rf'{fpass}\{i}\{t}.csv'):
                df0 = pd.read_csv(rf'{fpass}\{i}\{t}.csv', index_col='Date')
                df0.index = pd.to_datetime(df0.index)

                df1 = df_raw.loc[:, pd.IndexSlice[:, f'^{t}']]
                df1.columns = df1.columns.droplevel(1)
                df1 = df1.dropna(how='all')

                df2 = df0[:-1].combine_first(df1)
                df2.to_csv(rf'{fpass}\{i}\{t}.csv')
            else:
                if i != '1d':
                    df2 = yf.download(f'^{t}', period=p, interval=i, auto_adjust=False, progress=False, ignore_tz=False)
                    df2.index = df2.index.tz_convert(None)
                else:
                    df2 = yf.download(f'^{t}', period=p, interval=i, auto_adjust=False, progress=False)
                df2.index.name = 'Date'
                df2.to_csv(rf'{fpass}\{i}\{t}.csv')
    print('すべての指数データを取得しました。')

# 金利データ、TOPIXデータ取得
def get_stooq_data(tickers, interval='1d'):
    i = interval
    ticker_list = ['TPX', '10JPY', '10USY', '10DEY']
    code_list = ['^TPX', '10JPY.B', '10USY.B', '10DEY.B']
    start_date = datetime.date.today() - datetime.timedelta(days=5)
    
    for t, c in zip(ticker_list, code_list):
        if os.path.exists(rf'{fpass}\{i}\{t}.csv'):
            df0 = pd.read_csv(rf'{fpass}\{i}\{t}.csv', index_col='Date', parse_dates=True)
            df1 = wb.DataReader(c, data_source='stooq', start=start_date).sort_index(ascending=True)
            # if (t == 'TPX') & (pd.isnull(df1['Volume'][-1])):
            #     df1['Volume'][-1] = 0
            df2 = df0[:-1].combine_first(df1)
            df2.to_csv(rf'{fpass}\{i}\{t}.csv')
        else:
            df2 = wb.DataReader(c, data_source='stooq').sort_index(ascending=True)
            df2.to_csv(rf'{fpass}\{i}\{t}.csv')
    print('すべてのstooqデータを取得しました。')

# 各国の経済指標データ取得
def get_economic_data(tickers):
    #from_date = '07/01/2023'
    #to_date = '15/01/2023'
    
    from_date = (datetime.date.today() - datetime.timedelta(days=5)).strftime('%d/%m/%Y')
    to_date = datetime.date.today().strftime('%d/%m/%Y')
    
    countries = ['japan', 'united states', 'euro zone', 'germany', 'united kingdom']
    df_raw = investpy.economic_calendar(countries=countries, from_date=from_date, to_date=to_date)
    
    df_raw = df_raw.dropna(subset=['actual'])
    df_raw.loc[df_raw['time'].str.isalpha(), 'time'] = '00:00' # 'time'列に含まれる'All Day'や'Tentative'文字列を'00:00'に置き換え
    df_raw.index = pd.to_datetime(df_raw['date'] + ' ' + df_raw['time'], format='%d/%m/%Y %H:%M')
    df_raw.index.name = 'Date'
    
    if not pd.isnull(df_raw['id'][0]):
        for country in countries:
            df_temp = pd.DataFrame()
            df_temp.index.name = 'Date'
            
            if os.path.exists(rf'C:\Users\ronin\Documents\docker-python\topix500\economic\{country}.csv'):
                for i in tickers[f'investpy_{country}']:
                    if not pd.isnull(i) and df_raw[df_raw.event.str.startswith(i)]['event'].count() > 0:
                        df1 = df_raw[df_raw.event.str.startswith(i)][['actual', 'forecast', 'previous']]
                        df1.columns = [(i, p) for p in ['actual', 'forecast', 'previous']]
                        df1 = df1[~df1.index.duplicated(keep='last')]
                        df_temp = df_temp.combine_first(df1)
                    elif not pd.isnull(i) and df_raw[df_raw.event.str.startswith(i)]['event'].count() == 0:
                        continue
                    else:
                        break

                df0 = pd.read_csv(rf'C:\Users\ronin\Documents\docker-python\topix500\economic\{country}.csv', index_col='Date', low_memory=False) # low_memory->データ型混在エラーを非表示にする
                df0.index = pd.to_datetime(df0.index)
                df0.columns = map(lambda x: eval(x), df0.columns) # 文字列になっているカラム名をタプルに変換

                df0 = df0.combine_first(df_temp)    
                df0.to_csv(rf'C:\Users\ronin\Documents\docker-python\topix500\economic\{country}.csv')
            else:

                for i in tickers[f'investpy_{country}']:
                    if not pd.isnull(i) and df_raw[df_raw.event.str.startswith(i)]['event'].count() > 0:
                        df1 = df_raw[df_raw.event.str.startswith(i)][['actual', 'forecast', 'previous']]
                        df1.columns = [(i, p) for p in ['actual', 'forecast', 'previous']]
                        df1 = df1[~df1.index.duplicated(keep='last')]
                        df_temp = df_temp.combine_first(df1)

                        # df_temp.applymap(lambda y: y.strip('%') if str(y).endswith('%') else y)               # %表記を数値のみに変換
                        # df_temp.applymap(lambda y: y.strip('K') * 1000 if str(y).endswith('M') else y)        # K表記を数値×1,000に変換
                        # df_temp.applymap(lambda y: y.strip('M') * 1000000 if str(y).endswith('M') else y)     # M表記を数値×1,000,000に変換
                        # df_temp.applymap(lambda y: y.strip('B') * 1000000000 if str(y).endswith('B') else y)  # B表記を数値×1,000,000,000に変換
                        # temp_list = '|'.join([a for a in tickers.investpy_japan if not pd.isnull(a)])         # 参考：str.contains(A | B | C...)
                    elif not pd.isnull(i) and df_raw[df_raw.event.str.startswith(i)]['event'].count() == 0:
                        continue
                    else:
                        break
                df_temp.to_csv(rf'C:\Users\ronin\Documents\docker-python\topix500\economic\{country}.csv')
    else:
        print('検索の結果、経済指標データはありませんでした。')
    print('すべての経済指標データを取得しました。')

# 欧州ETFデータ別ソース
def get_eu_etf_data():
    etfs = ['EXV1', 'EXV2', 'EXV3', 'EXV4', 'EXV6', 'EXV7', 'EXV8', 'EXV9', 'EXH1', 'EXH3', 'EXH4', 'EXH5', 'EXH6', 'EXH7', 'EXH8', 'EXI5']
    for etf in etfs:
        tmp = pd.read_html(f'https://markets.ft.com/data/etfs/tearsheet/historical?s={etf}:GER:EUR')[0]
        tmp = tmp[:3]
        for i in range(len(tmp)):
            tmp.loc[i, 'Date'] = pd.to_datetime(tmp.loc[i, 'Date'][-12:]).date()
        tmp = tmp.sort_values('Date').set_index('Date')
        tmp['Volume'] = 0
        tmp['Adj Close'] = tmp['Close']
        tmp = tmp[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        tmp1 = pd.read_csv(rf'{fpass}\1d\{etf}.DE.csv', index_col='Date', parse_dates=True)
        tmp1 = tmp1[:-1].combine_first(tmp).reset_index()
        tmp1.to_csv(rf'{fpass}\1d\{etf}.DE.csv', index=False)
        time.sleep(1)

# 商品先物データ別ソース
def get_commodities_other_source(message, sp500):
    commodities = ['cl.1', 'gc.1', 'hg.1', 'ng.1', 'pa.1', 'pl.1', 'si.1', 'c.1', 's.1']
    file_name = ['CL', 'GC', 'HG', 'NG', 'PA', 'PL', 'SI', 'ZC', 'ZS']
    
    for c, f in zip(commodities, file_name):
        tmp1 = pd.read_csv(rf'{fpass}\1d\{f}.csv', parse_dates=['Date'])
        url = urllib.request.urlopen(f'https://www.marketwatch.com/investing/future/{c}')
        soup = BeautifulSoup(url, 'html.parser')
        try:
            tmp1.loc[tmp1['Date'] == sp500, ['Close', 'Adj Close']] = float(soup.find(class_='intraday__price').find('bg-quote', class_='value').text.replace(',',''))
        except:
            try:
                tmp1.loc[tmp1['Date'] == sp500, ['Close', 'Adj Close']] = float(soup.find(class_='intraday__price').find('span', class_='value').text.replace(',',''))
            except:
                # tmp1.loc[tmp1['Date'] == sp500, ['Close', 'Adj Close']] = float(soup.find(class_='primary').find('bg-quote').text.replace(',','')) # 旧バージョン
                message += f'{f}先物別ソース更新時にエラーが発生しました\n'
                return message
            # tmp = pd.read_html(f'https://www.marketwatch.com/investing/future/{c}?mod=mw_quote_tab')[5]
            # tmp1.loc[tmp1['Date'] == sp500, ['Close', 'Adj Close']] = float(tmp[tmp.iloc[:, 0].str.contains('Front Month')]['Last'].values[0].strip('$¢').replace(',',''))
            # tmp1.loc[tmp1['Date'] == sp500, 'Open'] = float(tmp[tmp.iloc[:, 0].str.contains('Front Month')]['Open'].values[0].strip('$¢').replace(',',''))
            # tmp1.loc[tmp1['Date'] == sp500, 'High'] = float(tmp[tmp.iloc[:, 0].str.contains('Front Month')]['High'].values[0].strip('$¢').replace(',',''))
            # tmp1.loc[tmp1['Date'] == sp500, 'Low'] = float(tmp[tmp.iloc[:, 0].str.contains('Front Month')]['Low'].values[0].strip('$¢').replace(',',''))
        tmp1.to_csv(rf'{fpass}\1d\{f}.csv', index=False)
        time.sleep(1)
    message += '商品先物を別ソースに更新しました\n'
    return message

# FXデータ別ソース
def get_fx_other_source(message, sp500):
    fxs = ['usdjpy', 'eurjpy', 'audjpy', 'gbpjpy', 'eurusd']
    file_name = ['USDJPY', 'EURJPY', 'AUDJPY', 'GBPJPY', 'EURUSD']

    for fx, f in zip(fxs, file_name):
        tmp1 = pd.read_csv(rf'{fpass}\1d\{f}.csv', parse_dates=['Date'])
        url = urllib.request.urlopen(f'https://www.marketwatch.com/investing/currency/{fx}')
        soup = BeautifulSoup(url, 'html.parser')
        try:
            tmp1.loc[tmp1['Date'] == sp500, ['Close', 'Adj Close']] = float(soup.find(class_='kv__item').find('span', class_='primary').text.replace('¥','').replace('$',''))
        except:
            message += f'{f}別ソース更新時にエラーが発生しました\n'
            return message
        tmp1.to_csv(rf'{fpass}\1d\{f}.csv', index=False)
        time.sleep(1)
    message += 'FXを別ソースに更新しました\n'
    return message

# 機械学習に必要な特徴量の元データを取得(FX, commodity, index, interest, ETF)
try:
    get_fx_data(tickers, period, interval)
    time.sleep(3)
except:
    message += 'FXデータ取得時にエラーが発生しました\n'

try:
    get_comodity_data(tickers, period, interval)
    time.sleep(3)
except:
    message += '先物データ取得時にエラーが発生しました\n'

try:
    get_index_data(tickers, period, interval)
except:
    message += '先物データ取得時にエラーが発生しました\n'

try:
    get_stooq_data(tickers)
except:
    message += 'stooqデータ取得時にエラーが発生しました\n'

try:
    get_economic_data(tickers)
except:
    message += '経済指標データ取得時にエラーが発生しました\n'

# 海外ETFデータ取得
try:
    etf_codes = [e for e in tickers.etfs if not pd.isnull(e)]
    for e in etf_codes:
        df0 = yf.download(e, period=std_period, interval='1d', auto_adjust=False, progress=False)
        df0.index.name = 'Date'
        df1 = pd.read_csv(rf'{fpass}\1d\{e}.csv', index_col='Date', parse_dates=True)
        df2 = df1[:-1].combine_first(df0)
        df2.to_csv(rf'{fpass}\1d\{e}.csv')
except:
    message += '海外ETFデータ取得時にエラーが発生しました\n'

# Chrome Driver更新チェック、N225先物データ取得
try:
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_experimental_option('prefs', {'download.prompt_for_download': False,})

    # res = requests.get('https://chromedriver.storage.googleapis.com/LATEST_RELEASE')
    # driver = webdriver.Chrome(ChromeDriverManager(res.text).install(), options=options)
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.get('https://225labo.com/user.php')

    username = driver.find_element(By.ID, 'legacy_xoopsform_uname')
    password = driver.find_element(By.ID, 'legacy_xoopsform_pass')
    username.clear()
    password.clear()
    username.send_keys('****')
    password.send_keys('****')
    submit = driver.find_element(By.CLASS_NAME, 'foot')
    submit.click()

    # N225先物 ダウンロード
    download_dir = rf'{fpass}\chromedriver'
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 
              'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    driver.execute("send_command", params=params)
    driver.get('https://225labo.com/modules/downloads_data/index.php?page=visit&cid=2&lid=159')
    time.sleep(15)

    # N225先物 データ取得
    with zipfile.ZipFile(rf'{fpass}\chromedriver\N225f_2023.zip', 'r') as zf:
        with zf.open('N225f_2023.xlsx') as f:
            excel = f.read()
    tmp = pd.read_excel(excel, sheet_name='ナイト場足')
    tmp = tmp.rename(columns={'日付':'Date', '始値':'Open', '高値':'High', '安値':'Low', '終値':'Close', '出来高':'Volume'})
    tmp[['Open', 'High', 'Low', 'Close', 'Volume']] = tmp[['Open', 'High', 'Low', 'Close', 'Volume']].shift(-1)
    tmp = tmp[:-1]
    tmp['Date'] = pd.to_datetime(tmp['Date'])

    tmp1 = pd.read_csv(rf'{fpass}\1d\N225F.csv', parse_dates=['Date'])
    if tmp1.iloc[-1]['Date'] >= tmp.iloc[-1]['Date']:
        print('データは最新です')
    else:
        tmp1 = tmp1.combine_first(tmp)
    tmp1.to_csv(rf'{fpass}\1d\N225F.csv', index=False)
    driver.close()
except:
    message += '日経225先物データ取得時にエラーが発生しました\n'

# TOPIX出来高取得
try:
    kabutan = urllib.request.urlopen('https://kabutan.jp/stock/?code=0010')
    soup = BeautifulSoup(kabutan, 'html.parser')
    date = pd.to_datetime(soup.find(id='kobetsu_left').find('h2').find('time').get('datetime'))
    tmp = pd.read_csv(rf'{fpass}\1d\TPX.csv', parse_dates=['Date'])

    if (tmp.iloc[-1]['Date'] == date) & pd.isnull(tmp.iloc[-1]['Volume']):
        table_5 = soup.find_all('table')[4]
        table_5.find('tr').find('td')
        dekidaka = int(table_5.find('tr').find('td').text[:-2].replace(',', ''))
        tmp.loc[tmp['Date'] == date, 'Volume'] = dekidaka
        tmp.to_csv(rf'{fpass}\1d\TPX.csv', index=False)
except:
    message += 'TOPIX出来高データ取得時にエラーが発生しました\n'

message += 'overnight特徴量を更新しました\n'

# 今日が火～土で、かつ前日の日米欧データが揃っている場合のみ予測データ作成する
n225 = pd.read_csv(rf'{fpass}\1d\N225.csv', parse_dates=['Date']).iloc[-1]['Date']
sp500 = pd.read_csv(rf'{fpass}\1d\GSPC.csv', parse_dates=['Date']).iloc[-1]['Date']
dax = pd.read_csv(rf'{fpass}\1d\GDAXI.csv', parse_dates=['Date']).iloc[-1]['Date']
eu_etf = pd.read_csv(rf'{fpass}\1d\EXV1.DE.csv', parse_dates=['Date']).iloc[-1]['Date']
fx_date = pd.read_csv(rf'{fpass}\1d\USDJPY.csv', parse_dates=['Date']).iloc[-1]['Date']
today = datetime.date.today()

# 最新の欧州業種別ETFデータ取得できなかった場合
if eu_etf < dax:
    try:
        get_eu_etf_data()
    except:
        message += '欧州ETFデータ(別ソース)取得時にエラーが発生しました\n'

