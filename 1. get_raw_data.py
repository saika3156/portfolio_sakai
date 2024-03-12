import pandas as pd
from pandas_datareader import data as wb
import os
import time
import datetime
import yfinance as yf
import investpy
import cfscrape
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import zipfile
from bs4 import BeautifulSoup
import urllib.request
        
class StockDataDownloader:
    def __init__(self, tickers, std_period, fpass):
        self.tickers = tickers
        self.std_period = std_period
        self.fpass = fpass
        self.message = ''

    # 株価データダウンロード
    def download_data(self, p, i, code_list):
        try:
            if i != '1d':
                df_raw = yf.download(code_list, period=self.std_period, interval=i, auto_adjust=False, progress=False, ignore_tz=False)
                df_raw.index = df_raw.index.tz_convert(None)
            else:
                df_raw = yf.download(code_list, period=self.std_period, interval=i, auto_adjust=False, progress=False)
            df_raw.index.name = 'Date'
        except:
            self.message += f'{i}データ取得中にエラーが発生しました\n'
            return None
        return df_raw
    
    # csvファイルに保存
    def update_existing_data(self, df_raw, ticker_list, i, front_code, end_code):
        error_codes = []
        
        for t in ticker_list:
            if os.path.exists(rf'{self.fpass}\{i}\{t}.csv'):
                df0 = pd.read_csv(rf'{self.fpass}\{i}\{t}.csv', index_col='Date')
                df0.index = pd.to_datetime(df0.index)

                try:
                    df1 = df_raw.loc[:, pd.IndexSlice[:, f'{front_code}{t}{end_code}']]
                    df1.columns = df1.columns.droplevel(1)
                    df1 = df1.dropna(how='all')

                    df2 = df0[:-1].combine_first(df1)
                    df2.to_csv(rf'{self.fpass}\{i}\{t}.csv')
                except:
                    error_codes.append(t)
                    if len(error_codes) >= 10:
                        break
            else:
                try:
                    if i != '1d':
                        df2 = yf.download(f'{front_code}{t}{end_code}', period=self.std_period, interval=i, auto_adjust=False, progress=False, ignore_tz=False)
                        df2.index = df2.index.tz_convert(None)
                    else:
                        df2 = yf.download(f'{front_code}{t}{end_code}', period=self.std_period, interval=i, auto_adjust=False, progress=False)
                    df2.index.name = 'Date'
                    df2.to_csv(rf'{self.fpass}\{i}\{t}.csv')
                except:
                    error_codes.append(t)
                    if len(error_codes) >= 10:
                        break
        return error_codes

    # 株価、FX、指数、先物データ取得
    def get_stock_data(self, periods, category, intervals, code_list, ticker_list, front_code, end_code):
        for p, i in zip(periods, intervals):
            data_result = self.download_data(p, i, code_list)

            if len(data_result) > 0:
                df_raw = data_result
                error_codes = self.update_existing_data(df_raw, ticker_list, i, front_code, end_code)

                if len(error_codes) == 0:
                    print(f'{i}のデータを取得しました\n')
                else:
                    print(f'{i}の{category}データを取得しましたが、エラーが{error_codes}で発生しました\n')
                    self.message += f'{i}の{category}データを取得しましたが、エラーが{error_codes}で発生しました\n'
        self.message += '全ての{category}データを取得しました\n'
        return self.message
    
    # 金利データ取得
    def get_stooq_data(self, intervals):
        i = intervals[0]
        ticker_list = ['TPX', '10JPY', '10USY', '10DEY']
        code_list = ['^TPX', '10JPY.B', '10USY.B', '10DEY.B']
        start_date = datetime.date.today() - datetime.timedelta(days=5)

        for t, c in zip(ticker_list, code_list):
            if os.path.exists(rf'{fpass}\{i}\{t}.csv'):
                df0 = pd.read_csv(rf'{fpass}\{i}\{t}.csv', index_col='Date', parse_dates=True)
                df1 = wb.DataReader(c, data_source='stooq', start=start_date).sort_index(ascending=True)
                df2 = df0[:-1].combine_first(df1)
                df2.to_csv(rf'{fpass}\{i}\{t}.csv')
            else:
                df2 = wb.DataReader(c, data_source='stooq').sort_index(ascending=True)
                df2.to_csv(rf'{fpass}\{i}\{t}.csv')
        self.message += 'すべてのstooqデータを取得しました\n'
        return self.message
    
    # 国別の経済指標データ取得
    def get_economic_data(self, tickers, fpass):
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

                if os.path.exists(rf'{fpass}\economic\{country}.csv'):
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

                    df0 = pd.read_csv(rf'{fpass}\economic\{country}.csv', index_col='Date', low_memory=False) # low_memory->データ型混在エラーを非表示にする
                    df0.index = pd.to_datetime(df0.index)
                    df0.columns = map(lambda x: eval(x), df0.columns) # 文字列になっているカラム名をタプルに変換
                    df0 = df0.combine_first(df_temp)    
                    df0.to_csv(rf'{fpass}\economic\{country}.csv')
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
                    df_temp.to_csv(rf'{fpass}\economic\{country}.csv')
        else:
            self.message += '経済指標データなし\n'
        self.message += 'すべての経済指標データを取得しました\n'
        return self.message
    
    # ChromeDriver更新、日経225先物データ取得(ナイト場足)
    def get_n225_data(self, fpass):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_experimental_option('prefs', {'download.prompt_for_download': False,})
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

        year = datetime.date.today().strftime('%Y') # ダウンロードファイル名(西暦)

        # N225先物 データ取得
        try:
            with zipfile.ZipFile(rf'{fpass}\chromedriver\N225f_{year}.zip', 'r') as zf:
                with zf.open(f'N225f_{year}.xlsx') as f:
                    excel = f.read()
            tmp = pd.read_excel(excel, sheet_name='ナイト場足')
            tmp = tmp.rename(columns={'日付':'Date', '始値':'Open', '高値':'High', '安値':'Low', '終値':'Close', '出来高':'Volume'})
            tmp[['Open', 'High', 'Low', 'Close', 'Volume']] = tmp[['Open', 'High', 'Low', 'Close', 'Volume']].shift(-1)
            tmp = tmp[:-1]
            tmp['Date'] = pd.to_datetime(tmp['Date'])

            tmp1 = pd.read_csv(rf'{fpass}\1d\N225F.csv', parse_dates=['Date'])
            if tmp1.iloc[-1]['Date'] >= tmp.iloc[-1]['Date']:
                self.message += 'N225先物データは最新です\n'
            else:
                tmp1 = tmp1.combine_first(tmp)
            tmp1.to_csv(rf'{fpass}\1d\N225F.csv', index=False)
            driver.close()
        except:
            self.message += '日経225先物データ取得時にエラーが発生しました\n'
        return self.message
    
    # 信用取引規制銘柄更新
    def update_margin_restriction(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        driver.get('https://google.com')

        res = requests.get('https://www.rakuten-sec.co.jp/ITS/Companyfile/margin_restriction.html')
        bs = BeautifulSoup(res.content, "html.parser")
        tmp = bs.find_all("table", border="1", cellspacing="0", cellpadding="3", width="100%")
        df = pd.read_html(str(tmp), keep_default_na=False)[0]
        df.to_csv(rf'{fpass}\1d\margin_restriction.csv', index=False, encoding='cp932')
        self.message += '取引規制銘柄を更新しました\n'
        return self.message

    # データ取得結果LINE送信
    def send_line(self, message):
        line_notify_token = '****'
        line_notify_api = 'https://notify-api.line.me/api/notify'
        payload = {'message': message}
        headers = {'Authorization': 'Bearer ' + line_notify_token}
        requests.post(line_notify_api, data=payload, headers=headers)

# main処理
if __name__ == '__main__':
    fpass = r'C:\Users\****\Documents\docker-python\topix500' # 株価データ保存ディレクトリ
    tickers = pd.read_csv(rf'{fpass}\topix500.csv') # 銘柄コードファイル
    periods = ['6y'] #, '1y', '6mo']  # 新規銘柄の場合、データ取得期間
    intervals = ['1d'] #, '1h', '15m']  # データ取得間隔
    std_period = '6d' # 既存銘柄の場合、データ取得期間
    
    for category, front_code, end_code in zip(['stock', 'fx', 'index', 'commodity'], ['', '', '^', ''], ['.T', '=X', '', '=F']): # 金融商品カテゴリ別にコードリストを作成
        if category == 'stock':
            ticker_list = [int(ticker) for ticker in tickers.code if not pd.isnull(ticker)]
            ticker_list += [int(ticker) for ticker in tickers.topix_17_etf if not pd.isnull(ticker)]
            code_list = [f'{code}.T' for code in ticker_list]
        elif category == 'fx':
            ticker_list = [ticker for ticker in tickers.fxs if not pd.isnull(ticker)]
            code_list = [f'{code}=X' for code in ticker_list]
        elif category == 'index':
            ticker_list = [ticker for ticker in tickers.indexs if not pd.isnull(ticker)]
            code_list = [f'^{code}' for code in ticker_list]
        elif category == 'commodity':
            ticker_list = [ticker for ticker in tickers.comodity if not pd.isnull(ticker)]
            code_list = [f'{code}=F' for code in ticker_list]
            
        downloader = StockDataDownloader(tickers, std_period, fpass)
        result_message = downloader.get_stock_data(periods, category, intervals, code_list, ticker_list, front_code, end_code)
    result_message = downloader.get_stooq_data(intervals)
    result_message = downloader.get_economic_data(tickers, fpass)
    result_message = downloader.get_n225_data(fpass)
    result_message = downloader.update_margin_restriction()
    send_line(result_message)