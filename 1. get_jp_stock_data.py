import os
import datetime
import time
import requests
import numpy as np
import pandas as pd
import talib
from pandas_datareader import data as wb
import yfinance as yf

fpass = r'C:\Users\****\Documents\docker-python\topix500' # 株価データ保存ディレクトリ
tickers = pd.read_csv(rf'{fpass}\topix500.csv') # 銘柄コードファイル
calendar = pd.read_csv(rf'{fpass}\calendar_2023.csv', parse_dates=['Date'])['Date'].tolist() # カレンダーファイル
holiday_jp = pd.read_csv(rf'{fpass}\holiday.csv', parse_dates=['JP'])['JP'].tolist() # 休場日ファイル
now_date = pd.to_datetime(datetime.datetime.now().date())

period = ['6y'] #, '1y', '60d']   # 株価データ最大期間1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
std_period = '5d' # 株価データ標準期間
interval = ['1d'] #, '1h', '15m'] # 株価データ間隔(1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo)
message = ''

# 日本株データ更新
def get_stock_data(tickers, period, interval, message):
    for p, i in zip(period, interval):
        code_list = [int(ticker) for ticker in tickers.code if not pd.isnull(ticker)]
        code_list = code_list + [int(ticker) for ticker in tickers.topix_17_etf if not pd.isnull(ticker)]
        ticker_list = [f'{code}.T' for code in code_list]
        
        try:
            if i != '1d':
                df_raw = yf.download(ticker_list, period=std_period, interval=i, auto_adjust=False, progress=False, ignore_tz=False) # auto_adjust=True : 調整後の株価(OHLC)を取得
                df_raw.index = df_raw.index.tz_convert(None)      # JST → UTC表記に変更
            else:
                df_raw = yf.download(ticker_list, period=std_period, interval=i, auto_adjust=False, progress=False)
            df_raw.index.name = 'Date'
        except:
            message += f'\n{i}データ取得中にエラーが発生しました'
            return message
        
        error_codes = []
        for t in code_list:
            if os.path.exists(rf'{fpass}\{i}\{t}.csv'):
                df0 = pd.read_csv(rf'{fpass}\{i}\{t}.csv', index_col='Date')
                df0.index = pd.to_datetime(df0.index)
                
                try:
                    df1 = df_raw.loc[:, pd.IndexSlice[:, f'{t}.T']]
                    df1.columns = df1.columns.droplevel(1)
                    df1 = df1.dropna(how='all')

                    df2 = df0[:-1].combine_first(df1)
                    df2.to_csv(rf'{fpass}\{i}\{t}.csv')
                except:
                    error_codes.append(t)
                    if len(error_codes) >= 10: break
            else:
                try:
                    if i != '1d':
                        df2 = yf.download(f'{t}.T', period=p, interval=i, auto_adjust=False, progress=False, ignore_tz=False)
                        df2.index = df2.index.tz_convert(None)
                    else:
                        df2 = yf.download(f'{t}.T', period=p, interval=i, auto_adjust=False, progress=False)
                    df2.index.name = 'Date'
                    df2.to_csv(rf'{fpass}\{i}\{t}.csv')
                except:
                    error_codes.append(t)
                    if len(error_codes) >= 10: break

        if len(error_codes) == 0:
            print(f'{i}のデータを取得しました')
        else:
            print(f'{i}のデータを取得しましたが、エラーが{error_codes}で発生しました')
            message += f'\n{i}のデータを取得しましたが、エラーが{error_codes}で発生しました'
    message += '\n全ての株価を取得しました。\n'
    return message
    
# 株式貸借データ更新
def get_karauri_data(message, days=3): # 
    from_date = datetime.datetime.today() - datetime.timedelta(days=days)
    zandaka_cols_jp = ['申込日', 'コード', '融資新規', '融資返済', '融資残高', '貸株新規', '貸株返済', '貸株残高', '差引残高']
    zandaka_cols_en = ['Date', 'code', 'new_loan', 'loan_repayment', 'loan_balance', 'stock_new_loan', 'stock_loan_repayment', 'stock_loan_balance', 'loan_stock_balance']
    shina_cols_jp = ['貸借申込日', 'コード', '貸借値段（円）', '貸株超過株数（株・口）', '最高料率（円）', '当日品貸料率（円）', '当日品貸日数', '前日品貸料率（円）', '備考', '規制']
    shina_cols_en = ['Date', 'code', 'loan_price', 'excess_loan_stock', 'max_rate', 'daily_loan_rate', 'daily_loan_days', 'yesterday_loan_rate', 'remarks', 'regulation' ]
    code_list = [int(c) for c in tickers.code if not pd.isnull(c)]
    
    for i in range(days + 1):
        date = (from_date + datetime.timedelta(days=i)).strftime('%Y%m%d')
        url_zandaka = f'https://www.taisyaku.jp/search_admin/comp/balance/zandaka{date}.csv'
        url_shina = f'https://www.taisyaku.jp/search_admin/comp/pcsl/shina{date}.csv'

        for url, name in zip([url_zandaka, url_shina], ['zandaka', 'shina']):
            res = requests.get(url, timeout=30)
            file_path = rf'{fpass}\zandaka\{name}{date}.csv'
            csv_name = rf'{fpass}\zandaka\{name}_topix500.csv'
            if 'text/csv' in res.headers.values(): # 土日などデータが無い日を指定してもres.status_code == 200が返ってくるため、ヘッダー内にcsvが存在するかどうかでデータの有無を判断している
                with open(file_path, 'wb') as f:
                    for chunk in res.iter_content(chunk_size=1024):
                        f.write(chunk)
            else:
                continue
            if name == 'zandaka' and os.path.exists(file_path):
                df0 = pd.read_csv(file_path, encoding='cp932', header=4, usecols=zandaka_cols_jp) #, parse_dates=['申込日']) # parse_dates=:指定のカラムをdatetime型に変更(カラム名やTrue/Falseを指定)
                df0.columns = zandaka_cols_en
                df0 = df0[df0['code'].isin(code_list)]
                df0 = df0.groupby(['Date', 'code'], as_index=False).sum()
                df0 = df0.set_index('Date')
                
                if os.path.exists(csv_name):
                    df1 = pd.read_csv(csv_name, encoding='cp932', index_col='Date')
                    df2 = pd.concat([df1, df0]) # combine_firstだとなぜか重複する
                    df2 = df2.drop_duplicates().reset_index()
                    df2 = df2.dropna(subset=['Date'], axis=0).dropna(subset=['code'], axis=0).fillna(0)
                    df2.to_csv(csv_name, encoding='cp932', index=False)
                else:
                    df0 = df0.dropna(subset=['Date'], axis=0).dropna(subset=['code'], axis=0).fillna(0)
                    df0.to_csv(csv_name, encoding='cp932', index=False)
            elif name == 'shina' and os.path.exists(file_path):
                df0 = pd.read_csv(file_path, encoding='cp932', header=4, usecols=shina_cols_jp) #, parse_dates=['貸借申込日'])
                df0.columns = shina_cols_en
                df0 = df0[df0['code'].isin(code_list)]
                df0 = df0.set_index('Date')

                if os.path.exists(csv_name):
                    df1 = pd.read_csv(csv_name, encoding='cp932', index_col='Date')
                    df2 = pd.concat([df1, df0])
                    df2 = df2.drop_duplicates().reset_index()
                    df2 = df2.dropna(subset=['Date'], axis=0).dropna(subset=['code'], axis=0).fillna(0)
                    df2.to_csv(csv_name, encoding='cp932', index=False)
                else:
                    df0 = df0.dropna(subset=['Date'], axis=0).dropna(subset=['code'], axis=0).fillna(0)
                    df0.to_csv(csv_name, encoding='cp932', index=False)
            else:
                continue
    print('空売りデータを取得しました')
    message += '空売りデータを取得しました。\n'
    return message

# TOPIX 17業種ETF 最新日付チェック
def check_etf17_date(message):
    code_list = [int(ticker) for ticker in tickers.topix_17_etf if not pd.isnull(ticker)]
    latest_date = pd.read_csv(rf'{fpass}\1d\7203.csv', parse_dates=['Date']).iloc[-1]['Date']
    not_match_date = []
    for code in code_list:
        tmp = pd.read_csv(rf'{fpass}\1d\{code}.csv', parse_dates=['Date']).iloc[-1]['Date']
        if tmp != latest_date:
            not_match_date.append(code)
    if len(not_match_date) > 0:
        message += f'TOPIX ETF{not_match_date}の日付が一致しません'
    return message

# 予測データにリターン実績追加(前日の予測結果確認 & 損切りライン判定)
def add_target1(message, limit=0.005):
    df = pd.read_csv(rf'{fpass}\predict\order_history.csv', parse_dates=['Date']) # Date, pred, target1, code
    for i in df[df['target1'].isna()]['code'].unique().astype(int):
        tmp = pd.read_csv(rf'{fpass}\1d\{i}.csv', parse_dates=['Date'])
        tmp['return'] = ((tmp['Close'] - tmp['Open']) / tmp['Open']).shift(-1)
        tmp['return_high'] = ((tmp['High'] - tmp['Open']) / tmp['Open']).shift(-1)
        tmp['return_low'] = ((tmp['Low'] - tmp['Open']) / tmp['Open']).shift(-1)
        tmp[['return_high', 'return_low']] = tmp[['return_high', 'return_low']].replace([np.inf, -np.inf], np.nan).fillna(0)
        
        try:
            for d in df[(df['code'] == i) & (df['target1'].isna())]['Date']:
                order = df[(df['Date'] == d) & (df['code'] == i)]['order'].values[0]
                if order == 3: # 買い
                    if tmp[tmp['Date'] == d]['return_low'].values[0] <= -limit:
                        df.loc[(df['Date'] == d) & (df['code'] == i), 'target1'] = -limit
                    else:
                        df.loc[(df['Date'] == d) & (df['code'] == i), 'target1'] = tmp[tmp['Date'] == d]['return'].values[0]
                elif order == 1: # 売り
                    if tmp[tmp['Date'] == d]['return_high'].values[0] >= limit:
                        df.loc[(df['Date'] == d) & (df['code'] == i), 'target1'] = limit
                    else:
                        df.loc[(df['Date'] == d) & (df['code'] == i), 'target1'] = tmp[tmp['Date'] == d]['return'].values[0]
        except:
            message += f'銘柄{i}、日付{d}のリターン計算でエラーが発生しました\n'
    df.to_csv(rf'{fpass}\predict\order_history.csv', index=False)
    df = df.sort_values('Date')
    
    message += '買い当日:' + str(round(df[(df['order'] == 3) & (df['Date'] == df.iloc[-1]['Date'])]['target1'].sum(), 4)) + '\n'
    message += '売り当日:' + str(round(df[(df['order'] == 1) & (df['Date'] == df.iloc[-1]['Date'])]['target1'].sum() * -1, 4)) + '\n'
    message += '合計当日:' + str(round(df[(df['order'] == 3) & (df['Date'] == df.iloc[-1]['Date'])]['target1'].sum() - 
                                   df[(df['order'] == 1) & (df['Date'] == df.iloc[-1]['Date'])]['target1'].sum(), 4)) + '\n\n'
    
    message += '買い累計:' + str(round(df[df['order'] == 3]['target1'].sum(), 4)) + '\n'
    message += '売り累計:' + str(round(df[df['order'] == 1]['target1'].sum() * -1, 4)) + '\n'
    message += '合計累計:' + str(round(df[df['order'] == 3]['target1'].sum() - df[df['order'] == 1]['target1'].sum(), 4)) + '\n\n'
    return message


# 銘柄別売買高更新
def update_vol_value(message):
    codes = [int(c) for c in tickers.code if not pd.isnull(c)]
    vol_value = pd.DataFrame()
    for code in codes:
        tmp = pd.read_csv(rf'{fpass}\1d\{code}.csv', parse_dates=['Date'])
        if tmp[tmp['Date'] == tmp.iloc[-1]['Date']]['Close'].values[0] < 15000: # 最新の終値が20,000円以上の銘柄は発注金額上限を超えるため除外
            tmp = tmp[tmp['Date'] >= datetime.datetime(2023, 1, 1)]
            tmp[code] = tmp['Volume'] * tmp['Close']
            if len(vol_value) == 0:
                vol_value = tmp[['Date', code]]
            else:
                vol_value = vol_value.merge(tmp[['Date', code]], on='Date', how='left')
        else:
            continue
    vol_value.to_csv(rf'{fpass}\1d\vol_value.csv', index=False)
    print('売買高リストを更新しました')
    message += '売買高リストを更新しました。\n'
    return message

# 信用取引規制銘柄更新
def update_margin_restriction(message):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager
    from bs4 import BeautifulSoup

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.get('https://google.com')

    # html = driver.page_source.encode('cp932')
    res = requests.get('https://www.rakuten-sec.co.jp/ITS/Companyfile/margin_restriction.html')
    bs = BeautifulSoup(res.content, "html.parser")
    tmp = bs.find_all("table", border="1", cellspacing="0", cellpadding="3", width="100%")
    df = pd.read_html(str(tmp), keep_default_na=False)[0]
    df.to_csv(rf'{fpass}\1d\margin_restriction.csv', index=False, encoding='cp932')
    message += '取引規制銘柄を更新しました。\n'
    return message

# 休場日判定
if now_date not in holiday_jp:
    message = get_stock_data(tickers, period, interval, message)
    message = get_karauri_data(message, days=3)
    message = update_vol_value(message)
    message = check_etf17_date(message)
else:
    message += '本日は休場日のためデータ更新なし\n'
    
# 開場日の場合は前日の予測結果を記録
if now_date in calendar:
    message = add_target1(message, limit=0.005)
else:
    message += '本日は予測データなし\n'

# 信用取引規制銘柄更新
message = update_margin_restriction(message)
# データ最終更新日
latest_date = pd.read_csv(rf'{fpass}\1d\7203.csv').iloc[-1]['Date'] + '\n'

# LINE通知
line_notify_token = '****'
line_notify_api = 'https://notify-api.line.me/api/notify'
payload = {'message': message + latest_date}
headers = {'Authorization': 'Bearer ' + line_notify_token}
requests.post(line_notify_api, data=payload, headers=headers)



# 実リターン確認
# def after_process(pred, rank_etf=3, rank_stock=3, rank_type='vol_value', limit=0.01, check_return=True): # output=True 予測結果(銘柄コード)を表示
#     pred['rank_buy'] = pred.groupby('Date')['pred'].rank(method='min', ascending=False) # 予測結果をランク分け
#     pred['rank_sell'] = pred.groupby('Date')['pred'].rank(method='min', ascending=True)
#     df = pred.copy()
#     df = df[(df['rank_buy'] <= rank_etf) | (df['rank_sell'] <= rank_etf)].reset_index(drop=True)

#     tmp = pd.read_csv(rf'{fpass}\1d\vol_value.csv', index_col='Date', parse_dates=True)
#     if rank_type == 'vol_value_ror':
#         tmp = tmp.pct_change(1)
#     elif rank_type == 'vol_value_diffma20':
#         for c in tmp.columns.unique().tolist():
#             tmp[c] = round((tmp[c] - talib.SMA(tmp[c], 20)) / talib.SMA(tmp[c], 20), 4)
#     tmp = tmp.reset_index()
    
#     for i in range(len(df)):
#         date = df.loc[i, 'Date']
#         sec17_code = tickers[tickers['topix_17_etf'] == df['code'].values[i]]['topix_17_etf_code'].values[0] # 予測したTOPIX_EFTコードからTOPIX17業種コードに読み替え
#         sec17_code_list = tickers[tickers['sector_17'] == sec17_code]['code'].astype(str).tolist()
#         sec17_code_list = [i for i in sec17_code_list if i in tmp.columns] # 最新の終値が20000円以上の銘柄は売買高ファイルから除外しているため、売買高ファイルに存在するコードのみ抽出する
#         rank_buy = df.loc[i, 'rank_buy']
#         rank_sell = df.loc[i, 'rank_sell']
        
#         if rank_buy <= rank_etf:
#             tmp1 = tmp[tmp['Date'] == date][sec17_code_list].rank(method='min', ascending=False, axis=1)
#             for r in range(rank_stock):
#                 r = r + 1
#                 try:
#                     code = tmp1[tmp1 == r].dropna(axis=1).columns[0]
#                     df.loc[i, f'buy_result_{r}'] = int(code)
#                     if check_return == True: # 実現リターン追記
#                         tmp2 = pd.read_csv(rf'{fpass}\1d\{code}.csv', parse_dates=['Date'])
#                         tmp2['day_return'] = ((tmp2['Close'] - tmp2['Open']) / tmp2['Open']).shift(-1)
#                         tmp2['return_low'] = ((tmp2['Low'] - tmp2['Open']) / tmp2['Open']).shift(-1)
#                         if tmp2[tmp2['Date'] == date]['return_low'].values[0] < -limit:
#                             df.loc[i, f'buy_return_{r}'] = -limit
#                         else:
#                             df.loc[i, f'buy_return_{r}'] = tmp2[tmp2['Date'] == date]['day_return'].values[0]
#                 except:
#                     continue
#         elif rank_sell <= rank_etf:
#             tmp1 = tmp[tmp['Date'] == date][sec17_code_list].rank(method='min', ascending=False, axis=1)
#             for r in range(rank_stock):
#                 r = r + 1
#                 try:
#                     code = tmp1[tmp1 == r].dropna(axis=1).columns[0]
#                     df.loc[i, f'sell_result_{r}'] = int(code)
#                     if check_return == True:
#                         tmp2 = pd.read_csv(rf'{fpass}\1d\{code}.csv', parse_dates=['Date'])
#                         tmp2['day_return'] = ((tmp2['Close'] - tmp2['Open']) / tmp2['Open']).shift(-1)
#                         tmp2['return_high'] = ((tmp2['High'] - tmp2['Open']) / tmp2['Open']).shift(-1)
#                         if tmp2[tmp2['Date'] == date]['return_high'].values[0] > limit:
#                             df.loc[i, f'sell_return_{r}'] = limit
#                         else:
#                             df.loc[i, f'sell_return_{r}'] = tmp2[tmp2['Date'] == date]['day_return'].values[0]
#                 except:
#                     continue
#     df = df.sort_values('Date')
#     return df

# def check_profit(df, message):
#     df = df.sort_values('Date')
#     if os.path.exists(rf'{fpass}\predict\profit_history.csv'): # 毎日予測結果をhistoryファイルに追加
#         tmp = pd.read_csv(rf'{fpass}\predict\profit_history.csv', parse_dates=['Date']).sort_values('Date')
#         if df.iloc[-1]['Date'].values[0] <= tmp.iloc[-1]['Date'].values[0]:
#             message += '本日の予測データが無いため、損益結果は更新しません'
#             return message
    
#     if os.path.exists(rf'{fpass}\predict\profit_history.csv'):
#         tmp = pd.read_csv(rf'{fpass}\predict\profit_history.csv', parse_dates=['Date'])
#         tmp = pd.concat([tmp, df[df['Date'] == df.iloc[-1]['Date']]])
#     else:
#         tmp = df[df['Date'] == df.iloc[-1]['Date']]
#     tmp.to_csv(rf'{fpass}\predict\profit_history.csv', index=False)
    
#     df['total_buy_return'] = df[[s for s in df.columns if 'buy_return' in s]].sum(axis=1)
#     df['total_sell_return'] = df[[s for s in df.columns if 'sell_return' in s]].sum(axis=1)
#     df['grand_total_return'] = df['total_buy_return'] - df['total_sell_return']
    
#     message += '買い当日:' + str(round(df[df['Date'] == df.iloc[-1]['Date']]['total_buy_return'].sum(), 4)) + '\n'
#     message += '売り当日:' + str(round(df[df['Date'] == df.iloc[-1]['Date']]['total_sell_return'].sum() * -1, 4)) + '\n'
#     message += '合計当日:' + str(round(df[df['Date'] == df.iloc[-1]['Date']]['grand_total_return'].sum(), 4)) + '\n\n'
    
#     message += '買い累計:' + str(round(df['total_buy_return'].sum(), 4)) + '\n'
#     message += '売り累計:' + str(round(df['total_sell_return'].sum() * -1, 4)) + '\n'
#     message += '合計累計:' + str(round(df['grand_total_return'].sum(), 4)) + '\n'

# #     buy = df[abs(df['buy_return_1']) > 0]
# #     sell = df[abs(df['sell_return_1']) > 0]
# #     # buy = df[(abs(df['buy_return_1']) > 0) & (df['pred'] > 0)]
# #     # sell = df[(abs(df['sell_return_1']) > 0) & (df['pred'] < 0)]    
    
# #     buy = buy.sort_values('Date').reset_index(drop=True)
# #     buy = buy[['Date'] + [s for s in buy.columns if 'buy_return' in s]]
# #     buy['total_buy_return'] = buy[[s for s in buy.columns if 'buy_return' in s]].sum(axis=1)
# #     buy_today = buy[buy['Date'] == buy.iloc[-1]['Date']]
    
# #     sell = sell.sort_values('Date').reset_index(drop=True)
# #     sell = sell[['Date'] + [s for s in sell.columns if 'sell_return' in s]]
# #     sell['total_sell_return'] = sell[[s for s in sell.columns if 'sell_return' in s]].sum(axis=1)
# #     sell_today = sell[sell['Date'] == sell.iloc[-1]['Date']]
    
# #     message += '買い当日:' + str(round(buy_today['total_buy_return'].sum(), 4)) + '\n'
# #     message += '売り当日:' + str(round(sell_today['total_sell_return'].sum() * -1, 4)) + '\n'
# #     message += '合計当日:' + str(round(buy_today['total_buy_return'].sum() - sell_today['total_sell_return'].sum(), 4)) + '\n\n'
    
# #     message += '買い累計:' + str(round(buy['total_buy_return'].sum(), 4)) + '\n'
# #     message += '売り累計:' + str(round(sell['total_sell_return'].sum() * -1, 4)) + '\n'
# #     message += '合計累計:' + str(round(buy['total_buy_return'].sum() - sell['total_sell_return'].sum(), 4)) + '\n'
#     return message




