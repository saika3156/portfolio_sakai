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
from sklearn.linear_model import Lasso
from sklearn.preprocessing import MinMaxScaler, StandardScaler

warnings.simplefilter('ignore')
pd.options.display.float_format = '{:.5f}'.format
fpass = r'C:\Users\****\Documents\docker-python\topix500' # 特徴量、予測結果保存ディレクトリ
tickers = pd.read_csv(rf'{fpass}\topix500.csv') # 株式銘柄コードファイル
message = ''

# LINE通知
def send_line(message): 
    line_notify_token = 'eKBO3BPM1UHjvPi7G9SQkbdiQn8FBeefX9V4GCCoECj'
    line_notify_api = 'https://notify-api.line.me/api/notify'
    payload = {'message': message}
    headers = {'Authorization': 'Bearer ' + line_notify_token}
    requests.post(line_notify_api, data=payload, headers=headers)
    
# 予測に必要な各データ日付の整合性を確認、整合性が取れない場合は予測しない
if ((today.weekday() == 1) & (n225 == sp500 == dax) & (n225.weekday() == 0)) | ((today.weekday() == 2) & (n225 == sp500 == dax) & (n225.weekday() == 1)) | ((today.weekday() == 3) & (n225 ==     sp500 == dax) & (n225.weekday() == 2)) | ((today.weekday() == 4) & (n225 == sp500 == dax) & (n225.weekday() == 3)) | ((today.weekday() == 5) & (n225 == sp500 == dax) & (n225.weekday() == 4)):
    try:
        message = get_commodities_other_source(message, sp500)
    except:
        message += '商品先物(別ソース)取得時にエラーが発生しました\n'
    if fx_date < sp500:
        try:
            message = get_fx_other_source(message, sp500)
        except:
            message += 'FX(別ソース)取得時にエラーが発生しました\n'
else:
    message += '学習データが揃っていないため、本日の予測データなし'
    send_line(message)
    sys.exit()


# 機械学習に使用する特徴量前処理
def preprocess(df):
    cols = [i for i in df.columns if i in ['Open', 'High', 'Low', 'Close', 'Adj Close']]
    vol = ['Volume']
    df = df.dropna(how='all').copy() # すべての値がNaNの行を削除
    df[cols] = df[cols].mask(abs(df[cols].pct_change()) > 0.2) # 前日比±20%の異常値をNaNに置換
    df[cols] = df[cols].mask((df[cols] == 0) | (df[cols] < 0), df[cols].shift()).ffill() # 0値、マイナス値は1行前のデータに置換
    if 'Volume' in df.columns: df[vol] = df[vol].mask(df[vol] < 0, 0).fillna(0) # 出来高のNaN値は0に置換
    df.dropna(inplace=True)
    return df

# 学習データ作成
def create_train_df(code):
    interval = '1d'
    tpx = pd.read_csv(rf'{fpass}\{interval}\TPX.csv', parse_dates=['Date'])
    tpx['tpx'] = tpx['Close'].pct_change(1)
    tpx['tpx_day_r'] = (tpx['Close'] - tpx['Open']) / tpx['Open']
    tpx['tpx_diffma_20'] = round((tpx['Close'] - talib.SMA(tpx['Close'], 20)) / talib.SMA(tpx['Close'], 20), 4)
    tpx['tpx_ror_1_std_20'] = round(tpx['Close'].pct_change(1).rolling(20).std(), 4)
    
    df = pd.read_csv(rf'{fpass}\{interval}\{code}.csv', index_col='Date', parse_dates=True) # TOPIX17業種ETF
    # df = pd.read_csv(rf'{fpass}\{interval}\{code}_act.csv', index_col='Date', parse_dates=True) # TOPIX17業種指数
    df = preprocess(df)
    df = df.merge(tpx[['Date', 'tpx_day_r', 'tpx_diffma_20', 'tpx_ror_1_std_20']], on='Date', how='left')
    df['ror_1'] = df['Close'].pct_change(1)    
    df['target1'] = ((df['Close'] - df['Open']) / df['Open']).shift(-1) # - df['tpx_day_r'].shift(-1) # target1はマーケットリターンを引いたセクターリターン
    df['diffma_20'] = round((df['Close'] - talib.SMA(df['Close'], 20)) / talib.SMA(df['Close'], 20), 4)

    etf_codes = [int(ticker) for ticker in tickers.topix_17_etf if (not pd.isnull(ticker)) & (ticker not in [code])]

    #日次リターン クロスセクションスコア
    for c in etf_codes:
        c = str(c) # FutureWarning回避
        tmp1 = pd.read_csv(rf'{fpass}\{interval}\{c}.csv', parse_dates=['Date']) # TOPIX17業種ETF
        # tmp1 = pd.read_csv(rf'{fpass}\{interval}\{c}_act.csv', parse_dates=['Date']) # TOPIX17業種指数
        tmp1 = preprocess(tmp1)
        tmp1 = tmp1.merge(tpx[['Date', 'tpx', 'tpx_day_r']], on='Date', how='left')
        tmp1[f'diffma_20_{c}'] = round((tmp1['Close'] - talib.SMA(tmp1['Close'], 20)) / talib.SMA(tmp1['Close'], 20), 4)
        tmp1[c] = tmp1['Close'].pct_change(1) - tmp1['tpx']
        # tmp1[c] = (tmp1['Close'] - tmp1['Open']) / tmp1['Open'] # テスト用
        df = df.merge(tmp1[['Date', c]], on='Date', how='left')
        df = df.merge(tmp1[['Date', f'diffma_20_{c}']], on='Date', how='left')
        
    # overnight特徴量
    overnight = ['USDJPY', 'EURJPY', 'AUDJPY', 'GSPC', 'GDAXI', '10JPY', '10USY', '10DEY', 'SP500-60', 'SP500-55', 'SP500-50', 'GSG',#'MME',
                 'SP500-45', 'SP500-40', 'SP500-35', 'SP500-20', 'SP500-15', 'SP500-25', 'SP500-30',
                 'EXV1.DE', 'EXV6.DE', 'EXV7.DE', 'EXV8.DE', 'EXH3.DE', 'EXV4.DE', 'EXH4.DE', 'EXH5.DE', 'EXH6.DE', 'EXH1.DE', 'EXH7.DE', 'EXH8.DE', 'EXV3.DE', 'EXV2.DE', 'EXV9.DE', 'EXI5.DE',
                 'N225F', 'GSPE', 'VIX', 'RUT', 'HG', 'NG', 'PA', 'ZC', 'ZS', 'GC', 'SI', 'PL', 'CL', 'IXIC', 'DJI','GBPJPY', 'EURUSD']

    df = df.dropna(subset=['Date'], axis=0).reset_index(drop=True)

    for c in overnight:
        tmp1 = pd.read_csv(rf'{fpass}\{interval}\{c}.csv', index_col='Date', parse_dates=True)
        tmp1 = preprocess(tmp1)
        if c == 'N225F':
            tmp1['N225F'] = (tmp1['Close'] - tmp1['Open']) / tmp1['Open']
            df = df.merge(tmp1['N225F'], on='Date', how='left')
        else:
            df = df.merge(round(tmp1['Close'], 4).pct_change(1).rename(c), on='Date', how='left')

    df = df.mask(abs(df['10JPY']) > 10, np.nan)
    df = df.dropna(subset=['Date'], axis=0)
    df['weekday'] = df['Date'].dt.weekday # 曜日 0-6 : 月-日
    df['week_of_year'] = df['Date'].dt.isocalendar().week
    # df['weekday'] = df['weekday'].replace([3, 5, 6], 2)
    # オプション プットコールIVスプレッド
    # tmp = pd.read_csv(rf'{fpass}\1d\option.csv', parse_dates=['Date'])
    # df = df.merge(tmp[['Date', 'iv_spread']], on='Date', how='left')
    return df

# 特徴量選別(多重共線性が認められる特徴量を除外)
def create_train_df_vif(code):
    interval = '1d'
    tpx = pd.read_csv(rf'{fpass}\{interval}\TPX.csv', parse_dates=['Date'])
    tpx['tpx'] = tpx['Close'].pct_change(1)
    tpx['tpx_day_r'] = (tpx['Close'] - tpx['Open']) / tpx['Open']
    # tpx['tpx_diffma_20'] = round((tpx['Close'] - talib.SMA(tpx['Close'], 20)) / talib.SMA(tpx['Close'], 20), 4)
    tpx['tpx_ror_1_std_20'] = round(tpx['Close'].pct_change(1).rolling(20).std(), 4)
    
    df = pd.read_csv(rf'{fpass}\{interval}\{code}.csv', index_col='Date', parse_dates=True) # TOPIX17業種ETF
    # df = pd.read_csv(rf'{fpass}\{interval}\{code}_act.csv', index_col='Date', parse_dates=True) # TOPIX17業種指数
    df = preprocess(df)
    df = df.merge(tpx[['Date', 'tpx_day_r', 'tpx_ror_1_std_20']], on='Date', how='left')
    df['ror_1'] = df['Close'].pct_change(1)
    df['target1'] = ((df['Close'] - df['Open']) / df['Open']).shift(-1) # - df['tpx_day_r'].shift(-1) # target1はマーケットリターンを引いたセクターリターン
    df['diffma_20'] = round((df['Close'] - talib.SMA(df['Close'], 20)) / talib.SMA(df['Close'], 20), 4)

    etf_codes = [int(ticker) for ticker in tickers.topix_17_etf if (not pd.isnull(ticker)) & (ticker not in [code])]

    #日次リターン クロスセクションスコア
    for c in etf_codes:
        c = str(c) # FutureWarning回避
        tmp1 = pd.read_csv(rf'{fpass}\{interval}\{c}.csv', parse_dates=['Date']) # TOPIX17業種ETF
        # tmp1 = pd.read_csv(rf'{fpass}\{interval}\{c}_act.csv', parse_dates=['Date']) # TOPIX17業種指数
        tmp1 = preprocess(tmp1)
        tmp1 = tmp1.merge(tpx[['Date', 'tpx', 'tpx_day_r']], on='Date', how='left')
        tmp1[f'diffma_20_{c}'] = round((tmp1['Close'] - talib.SMA(tmp1['Close'], 20)) / talib.SMA(tmp1['Close'], 20), 4)
        tmp1[c] = tmp1['Close'].pct_change(1) - tmp1['tpx']
        # tmp1[c] = (tmp1['Close'] - tmp1['Open']) / tmp1['Open'] # テスト用
        df = df.merge(tmp1[['Date', c]], on='Date', how='left')
        df = df.merge(tmp1[['Date', f'diffma_20_{c}']], on='Date', how='left')
        
    # overnight特徴量
    overnight = ['USDJPY', 'AUDJPY', '10JPY', '10USY', '10DEY', 'SP500-60', 'SP500-55', 'SP500-50', 'GSG',#'MME',
                 'SP500-45', 'SP500-40', 'SP500-35', 'SP500-20', 'SP500-15', 'SP500-25', 'SP500-30',
                 'EXV1.DE', 'EXV6.DE', 'EXV7.DE', 'EXV8.DE', 'EXH3.DE', 'EXV4.DE', 'EXH4.DE', 'EXH5.DE', 'EXH6.DE', 'EXH1.DE', 'EXH7.DE', 'EXH8.DE', 'EXV3.DE', 'EXV2.DE', 'EXV9.DE', 'EXI5.DE',
                 'N225F', 'GSPE', 'VIX', 'RUT', 'HG', 'NG', 'PA', 'ZC', 'ZS', 'GC', 'SI', 'PL', 'CL', 'GBPJPY', 'EURUSD']

    df = df.dropna(subset=['Date'], axis=0).reset_index(drop=True)

    for c in overnight:
        tmp1 = pd.read_csv(rf'{fpass}\{interval}\{c}.csv', index_col='Date', parse_dates=True)
        tmp1 = preprocess(tmp1)
        if c == 'N225F':
            tmp1['N225F'] = (tmp1['Close'] - tmp1['Open']) / tmp1['Open']
            df = df.merge(tmp1['N225F'], on='Date', how='left')
        else:
            df = df.merge(round(tmp1['Close'], 4).pct_change(1).rename(c), on='Date', how='left')

    df = df.mask(abs(df['10JPY']) > 10, np.nan)
    df = df.dropna(subset=['Date'], axis=0)
    # df['weekday'] = df['Date'].dt.weekday # 曜日 0-6 : 月-日
    # df['week_of_year'] = df['Date'].dt.isocalendar().week
    # df['weekday'] = df['weekday'].replace([3, 5, 6], 2)
    # オプション プットコールIVスプレッド
    # tmp = pd.read_csv(rf'{fpass}\1d\option.csv', parse_dates=['Date'])
    # df = df.merge(tmp[['Date', 'iv_spread']], on='Date', how='left')
    return df

# 機械学習による騰落率予測
def predict(df, pred):
    target = 'target1'
    tmp = pd.DataFrame()
    
    df1 = df[:-1].dropna(how='any', axis=0)
    df2 = df[-1:]

    X = df1.drop(['target1', 'Date'], axis=1)
    scaler = StandardScaler()
    scaler.fit(X)
    X = scaler.transform(X)
    y = df1[target]
    
    X2 = df2.drop(['target1', 'Date'], axis=1)
    X2 = scaler.transform(X2)

    model = Lasso(alpha=0.00003, random_state=42)
    model.fit(X, y)
    y_pred = model.predict(X2)
    
    tmp['Date'] = pd.to_datetime(df2['Date'])
    tmp['pred'] = y_pred
    tmp['code'] = code
    
    pred = pd.concat([pred, tmp], join='outer')
    return pred

# 予測データ後処理(売買用データ作成)
def after_process(pred, rank_etf=3, rank_stock=3, rank_type='vol_value', limit=0.01, check_return=True, vif=False): # output=True 予測結果(銘柄コード)を表示, vif:多重共線性考慮
    pred['rank_buy'] = pred.groupby('Date')['pred'].rank(method='min', ascending=False) # 予測結果をランク分け
    pred['rank_sell'] = pred.groupby('Date')['pred'].rank(method='min', ascending=True)
    df = pred.copy()
    df = df[(df['rank_buy'] <= rank_etf) | (df['rank_sell'] <= rank_etf)].reset_index(drop=True)

    tmp = pd.read_csv(rf'{fpass}\1d\vol_value.csv', index_col='Date', parse_dates=True)
    if rank_type == 'vol_value_ror':
        tmp = tmp.pct_change(1)
    elif rank_type == 'vol_value_diffma20':
        for c in tmp.columns.unique().tolist():
            tmp[c] = round((tmp[c] - talib.SMA(tmp[c], 20)) / talib.SMA(tmp[c], 20), 4)
    tmp = tmp.reset_index()
    
    margin_restrict = pd.read_csv(rf'{fpass}\1d\margin_restriction.csv', parse_dates=['実施日'], encoding='cp932') # 信用取引規制銘柄
    calendar = pd.read_csv(rf'{fpass}\calendar_2023.csv', parse_dates=['Date']) # JP2023カレンダー
    next_date = calendar[calendar['Date'] >= datetime.datetime.today()].reset_index(drop=True).iloc[0]['Date'] # 翌営業日を取得(株式分割等で一日だけ信用返済ができなくなる場合も取引規制一覧に含まれる)
    margin_restrict_code = [str(int(i)) for i in tickers['code'] if i in margin_restrict[margin_restrict['実施日'] <= next_date]['銘柄コード'].unique()] # 信用取引規制銘柄かつTOPIX500銘柄
    
    for i in range(len(df)):
        date = df.loc[i, 'Date']
        sec17_code = tickers[tickers['topix_17_etf'] == df['code'].values[i]]['topix_17_etf_code'].values[0] # 予測したTOPIX_EFTコードからTOPIX17業種コードに読み替え
        sec17_code_list = tickers[tickers['sector_17'] == sec17_code]['code'].astype(int).astype(str).tolist()
        sec17_code_list = [i for i in sec17_code_list if i in tmp.columns] # 最新の終値が20000円以上の銘柄は売買高ファイルから除外しているため、売買高ファイルに存在するコードのみ抽出する
        sec17_code_list = [i for i in sec17_code_list if i not in margin_restrict_code] # 信用取引規制銘柄は除外
        rank_buy = df.loc[i, 'rank_buy']
        rank_sell = df.loc[i, 'rank_sell']
        
        if rank_buy <= rank_etf:
            tmp1 = tmp[tmp['Date'] == date][sec17_code_list].rank(method='min', ascending=False, axis=1)
            for r in range(rank_stock):
                r = r + 1
                try:
                    code = tmp1[tmp1 == r].dropna(axis=1).columns[0]
                    df.loc[i, f'buy_result_{r}'] = int(code)
                    if check_return == True: # 実現リターン追記
                        tmp2 = pd.read_csv(rf'{fpass}\1d\{code}.csv', parse_dates=['Date'])
                        tmp2['day_return'] = ((tmp2['Close'] - tmp2['Open']) / tmp2['Open']).shift(-1)
                        tmp2['return_low'] = ((tmp2['Low'] - tmp2['Open']) / tmp2['Open']).shift(-1)
                        if tmp2[tmp2['Date'] == date]['return_low'].values[0] < -limit:
                            df.loc[i, f'buy_return_{r}'] = -limit
                        else:
                            df.loc[i, f'buy_return_{r}'] = tmp2[tmp2['Date'] == date]['day_return'].values[0]
                except:
                    continue
        elif rank_sell <= rank_etf:
            tmp1 = tmp[tmp['Date'] == date][sec17_code_list].rank(method='min', ascending=False, axis=1)
            for r in range(rank_stock):
                r = r + 1
                try:
                    code = tmp1[tmp1 == r].dropna(axis=1).columns[0]
                    df.loc[i, f'sell_result_{r}'] = int(code)
                    if check_return == True:
                        tmp2 = pd.read_csv(rf'{fpass}\1d\{code}.csv', parse_dates=['Date'])
                        tmp2['day_return'] = ((tmp2['Close'] - tmp2['Open']) / tmp2['Open']).shift(-1)
                        tmp2['return_high'] = ((tmp2['High'] - tmp2['Open']) / tmp2['Open']).shift(-1)
                        if tmp2[tmp2['Date'] == date]['return_high'].values[0] > limit:
                            df.loc[i, f'sell_return_{r}'] = limit
                        else:
                            df.loc[i, f'sell_return_{r}'] = tmp2[tmp2['Date'] == date]['day_return'].values[0]
                except:
                    continue
    # df = df.sort_values('Date')

    buy = df[(df['Date'] == df.iloc[-1]['Date']) & (df['rank_buy'] <= rank_etf)].reset_index(drop=True)
    buy_today = pd.DataFrame()
    for i in range(1, len([s for s in buy.columns if 'buy_result' in s]) + 1):
        tmp = buy[['Date', 'pred', 'code', 'rank_buy'] + [s for s in buy.columns if f'buy_result_{i}' in s]].rename(columns={'code': 'etf_code', f'buy_result_{i}': 'code', 'rank_buy': 'rank'})
        tmp['order'] = 3
        buy_today = pd.concat([buy_today, tmp], join='outer')
    buy_today[['etf_code', 'rank', 'code']] = buy_today[['etf_code', 'rank', 'code']].astype(int)
    buy_today = buy_today.sort_values('rank').reset_index(drop=True)

    sell = df[(df['Date'] == df.iloc[-1]['Date']) & (df['rank_sell'] <= rank_etf)].reset_index(drop=True)
    sell_today = pd.DataFrame()
    for i in range(1, len([s for s in sell.columns if 'sell_result' in s]) + 1):
        tmp = sell[['Date', 'pred', 'code', 'rank_sell'] + [s for s in sell.columns if f'sell_result_{i}' in s]].rename(columns={'code': 'etf_code', f'sell_result_{i}': 'code', 'rank_sell': 'rank'})
        tmp['order'] = 1
        sell_today = pd.concat([sell_today, tmp], join='outer')
    sell_today[['etf_code', 'rank', 'code']] = sell_today[['etf_code', 'rank', 'code']].astype(int)
    sell_today = sell_today.sort_values('rank').reset_index(drop=True)

    order_today = pd.concat([buy_today, sell_today], join='outer').reset_index(drop=True)
    
    if vif == False:
        order_today.to_csv(rf'{fpass}\order\order_today.csv', index=False) # 発注用データ
        order_today['target1'] = np.nan
        if os.path.exists(rf'{fpass}\predict\order_history.csv'): # 損益確認用データ
            tmp = pd.read_csv(rf'{fpass}\predict\order_history.csv', parse_dates=['Date'])
            tmp = pd.concat([tmp, order_today], join='outer').reset_index(drop=True)
            tmp.to_csv(rf'{fpass}\predict\order_history.csv', index=False)
        else:
            order_today.to_csv(rf'{fpass}\predict\order_history.csv', index=False)
    elif vif == True:
        order_today.to_csv(rf'{fpass}\order\order_today_vif.csv', index=False) # 発注用データ
        order_today['target1'] = np.nan
        if os.path.exists(rf'{fpass}\predict\order_history_vif.csv'): # 損益確認用データ
            tmp = pd.read_csv(rf'{fpass}\predict\order_history_vif.csv', parse_dates=['Date'])
            tmp = pd.concat([tmp, order_today], join='outer').reset_index(drop=True)
            tmp.to_csv(rf'{fpass}\predict\order_history_vif.csv', index=False)
        else:
            order_today.to_csv(rf'{fpass}\predict\order_history_vif.csv', index=False)        
    return

# 学習データの欠損確認＆通知
except:
    code = 1617
    df = create_train_df(code)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.iloc[-1][[i for i in df.columns if i not in ['target1']]]
    features = ', '.join(df[df.isnull()].index.tolist())
    message = message + df['Date'].strftime('%Y/%m/%d') + '\n' + '欠損データがあります\n' + features
    send_line(message)
    sys.exit()
    
# 騰落予測(多重共線性を考慮した特徴量を使用)
try:
    codes = [int(ticker) for ticker in tickers.topix_17_etf if not pd.isnull(ticker)]
    pred = pd.DataFrame()
    # today = datetime.date.today().strftime('%Y%m%d')
    for code in codes:
        df = create_train_df_vif(code)
        # if code == 1617: df.to_csv(rf'{fpass}\check_df_1617_{today}.csv', index=False) # 学習データ保存
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.drop(['Open', 'Close', 'High', 'Low', 'Adj Close', 'Volume'], axis=1)
        pred = predict(df, pred)
    if os.path.exists(rf'{fpass}\predict\pred_sec17_vif.csv'):
        tmp = pd.read_csv(rf'{fpass}\predict\pred_sec17_vif.csv', parse_dates=['Date'])
        tmp = pd.concat([tmp, pred], join='outer').reset_index(drop=True)
        tmp.to_csv(rf'{fpass}\predict\pred_sec17_vif.csv', index=False)
    else:
        pred.to_csv(rf'{fpass}\predict\pred_sec17_vif.csv', index=False)
        
    after_process(pred, rank_etf=3, rank_stock=3, rank_type='vol_value', limit=0.005, check_return=False, vif=True)
    pd.DataFrame(columns=['銘柄コード', '銘柄名称', '売買', '建玉数量', '発注数量', '建値', '建日', 
                      '建市場', '時価', '前日比', '前日比率', '評価損額']).to_csv(rf'{fpass}\order\stop_today_vif.csv',index=False, encoding='cp932') # 逆指値注文用
    message += 'vif予測が完了しました\n'
except:
    message += 'vif予測時にエラーが発生しました\n'
    
# 毎週土曜日にTOPIX500銘柄リスト更新確認
if datetime.date.today().weekday() == 5:
    try:
        url = 'https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls'
        res = requests.get(url, timeout=30)
        file_path = rf'{fpass}\list\data_j.xls'
        with open(file_path, 'wb') as f:
            for chunk in res.iter_content(chunk_size=1024):
                f.write(chunk)

        if os.path.exists(file_path):
            df = pd.read_excel(file_path)
            df.columns = ['Date', 'code', 'name', 'type', 'sector_33_code', 'sector_33_name', 'sector_17_code', 'sector_17_name', 'size_code', 'market']
            date = df.iloc[-1]['Date']
            df.to_csv(rf'{fpass}\list\list_all_{date}.csv', index=False, encoding='cp932')

            df = df[df['market'].isin(['TOPIX Core30', 'TOPIX Large70', 'TOPIX Mid400'])]
            df.to_csv(rf'{fpass}\list\list_topix500_{date}.csv', index=False, encoding='cp932')
            tickers = set([i for i in pd.read_csv(rf'{fpass}\topix500.csv')['code'].tolist() if not math.isnan(i)])
            df = set(df['code'].tolist())
            if (len(df) == len(tickers)) & (len(df - tickers) == 0) & (len(tickers - df) == 0):
                message += 'TOPIX500構成銘柄に変更はありません\n'
            elif (len(df - tickers) > 0): # TOPIX500に銘柄追加
                message += f'銘柄コード{list(df - tickers)}がTOPIX500に追加されました\n'
            elif (len(tickers - df) > 0): # TOPIX500から銘柄削除
                message += f'銘柄コード{list(tickers - df)}がTOPIX500から除外されました\n'
    except:
        message += 'TOPIX500構成銘柄確認中にエラーが発生しました\n'
    
# 予測対象日を抽出 (予測の日付の翌日をカレンダーから取得)
latest_date = pd.read_csv(rf'{fpass}\order\order_today_vif.csv', parse_dates=['Date']).iloc[-1]['Date']
calendar = pd.read_csv(rf'{fpass}\calendar_2023.csv', parse_dates=['Date'])
date = str(pd.to_datetime(calendar[calendar.index == calendar[calendar['Date'] == latest_date].index.values[0] + 1]['Date'].values[0]).date())

# LINE通知
message = message + '発注準備ができました\n' + date
send_line(message)
