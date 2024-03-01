import subprocess
import math
import time
import datetime
import random
import os
import sys
import pandas as pd
import numpy as np
import xlwings as xw
import pyautogui as py
import openpyxl
import requests
fpass = r'C:\Users\****\Documents\docker-python\topix500' # データフォルダパス

def send_line(message): # 売買結果をLINEに送信
    line_notify_token = 'eKBO3BPM1UHjvPi7G9SQkbdiQn8FBeefX9V4GCCoECj'
    line_notify_api = 'https://notify-api.line.me/api/notify'
    payload = {'message': message}
    headers = {'Authorization': 'Bearer ' + line_notify_token}
    requests.post(line_notify_api, data=payload, headers=headers)

def clear_contents(): # excel注文データをクリア
    xw.Range('B12:C31, E12:G31, I12:M31, P12:S31, Y12:AB31').clear_contents()
def clear_contents_stop_order(): # excel損切り注文データをクリア
    xw.Range('B84:C103, E84:G103, I84:M103, P84:S103, Y84:AB103').clear_contents()

def send_cancel_order(message): # 取消注文(逆指値注文を取消し)
    df = pd.read_csv(rf'{fpass}\order\stop_today_vif.csv', encoding='cp932')
    if len(df) > 0:
        # wb = xw.Book(r'C:\Users\****\Desktop\投資\trigger_order_samplesheet.xlsm')
        sheet = xw.sheets['一括発注_国内株']
        sheet.activate()
        
        tmp = pd.DataFrame(xw.Range('B61:M80').value, columns=xw.Range('B60:M60').value) # 信用建玉リスト from RSS
        tmp = tmp[tmp['銘柄コード'].isin([int(i) for i in tmp['銘柄コード'] if isinstance(i, float)])] # 銘柄コードが数値以外の行を除外
        if len(tmp['銘柄コード']) == 0: # tmp['発注数量'] > 0の時 ＝ 逆指値または返済注文済? ←　要確認
            message += '建玉がありません\n'
            return message

        df = df[df['銘柄コード'].isin(tmp['銘柄コード'])]
        
        for c in df['order_number'].tolist():
            try:
                df.loc[df['order_number'] == c, 'cancel_order_id'] = int(df[df['order_number'] == c]['発注ID'].values[0]) + 1
            except:
                message += '注文番号がない建玉があります\n'
                
        clear_contents()
        # 取消注文データ入力
        xw.Range('B12', transpose=True).value = [3] * len(df) # 注文種類 3=取消
        xw.Range('C12', transpose=True).value = df['cancel_order_id'].tolist() # 発注ID(取消注文用)
        xw.Range('AB12', transpose=True).value = df['order_number'].tolist() # 注文番号(逆指値時)
        
        xw.Range('D12', transpose=True).value = [1] * len(df) # 発注トリガー
        time.sleep(5)
        xw.Range('D12', transpose=True).value = [0] * len(df) # 発注トリガー
        df.to_csv(rf'{fpass}\order\cancel_stop_today_vif.csv', index=False, encoding='cp932')
        message += '取消注文を送信しました\n'
    return message

# 逆指値条件価格を呼値の単位に合わせる
def mround(value):
    if value <= 10000:
        return round(value)
    elif value <= 30000:
        unit = 5
    else:
        unit = 10
    round_value = value - (value % unit) + unit
    return round_value

def send_close_order(message): # 決済注文(買建・売建決済)
    df = pd.read_csv(rf'{fpass}\order\order_today_vif.csv', parse_dates=['Date'])
    # stop = pd.read_csv(rf'{fpass}\order\stop_today_vif.csv', encoding='cp932') # 逆指値付通常注文用
    
    if len(df) > 0:
        # wb = xw.Book(r'C:\Users\****\Desktop\投資\trigger_order_samplesheet.xlsm')
        sheet = xw.sheets['一括発注_国内株']
        sheet.activate()
        sdaka_flag = 0 # 前日終値より15%以上値動きがある銘柄数
        syasu_flag = 0 # 同、-15%以上値動きがある銘柄数
        
        tmp = pd.DataFrame(xw.Range('B61:M80').value, columns=xw.Range('B60:M60').value) # 信用建玉リスト from RSS
        tmp = tmp[tmp['銘柄コード'].isin([int(i) for i in tmp['銘柄コード'] if isinstance(i, float)])] # 銘柄コードが数値以外の行を除外
        if len(tmp['銘柄コード']) == 0:
            message += '建玉がありません'
            return message
        for c, b, t, i in zip(tmp['銘柄コード'], tmp['売買'], tmp['建値'], range(len(tmp))):
            code_count = tmp[tmp['銘柄コード'] == c]['銘柄コード'].count()
            if b == '買建':
                if code_count == 1:
                    tmp.loc[tmp['銘柄コード'] == c, 'close_price'] = mround(tmp[tmp['銘柄コード'] == c]['時価'].values[0] * 1.02)
                    tmp.loc[tmp['銘柄コード'] == c, 'close_order_id'] = c * 10 + 3
                    # tmp.loc[tmp['銘柄コード'] == c, 'stop_price'] = stop[stop['銘柄コード'] == c]['stop_price'].values[0]
                    # tmp.loc[tmp['銘柄コード'] == c, 'stop_type'] = stop[stop['銘柄コード'] == c]['stop_type'].values[0]
                else:
                    tmp.loc[((tmp['銘柄コード'] == c) & (tmp['建値'] == t)), 'close_price'] = mround(tmp[((tmp['銘柄コード'] == c) & (tmp['建値'] == t))]['時価'].values[0] * 1.02)
                    tmp.loc[((tmp['銘柄コード'] == c) & (tmp['建値'] == t)), 'close_order_id'] = c * 100 + 31 + i
                    # tmp.loc[((tmp['銘柄コード'] == c) & (tmp['建値'] == t)), 'stop_price'] = stop[((stop['銘柄コード'] == c) & (stop['建値'] == t))]['stop_price'].values[0]
                    # tmp.loc[((tmp['銘柄コード'] == c) & (tmp['建値'] == t)), 'stop_type'] = stop[((stop['銘柄コード'] == c) & (stop['建値'] == t))]['stop_type'].values[0]
                    
                tmp.loc[tmp['銘柄コード'] == c, 'close_order'] = 1 # 1=売埋
                tmp.loc[tmp['銘柄コード'] == c, 'margin_type'] = df[df['code'] == c]['margin_type'].values[0]
                if tmp[tmp['銘柄コード'] == c]['前日比率'].values[0] > 10:
                    sdaka_flag += 1
                    
            elif b == '売建':
                if code_count == 1:
                    tmp.loc[tmp['銘柄コード'] == c, 'close_price'] = mround(tmp[tmp['銘柄コード'] == c]['時価'].values[0] * 0.98)
                    tmp.loc[tmp['銘柄コード'] == c, 'close_order_id'] = c * 10 + 3
                    # tmp.loc[tmp['銘柄コード'] == c, 'stop_price'] = stop[stop['銘柄コード'] == c]['stop_price'].values[0]
                    # tmp.loc[tmp['銘柄コード'] == c, 'stop_type'] = stop[stop['銘柄コード'] == c]['stop_type'].values[0]

                else:
                    tmp.loc[((tmp['銘柄コード'] == c) & (tmp['建値'] == t)), 'close_price'] = mround(tmp[((tmp['銘柄コード'] == c) & (tmp['建値'] == t))]['時価'].values[0] * 0.98)
                    tmp.loc[((tmp['銘柄コード'] == c) & (tmp['建値'] == t)), 'close_order_id'] = c * 100 + 31 + i
                    # tmp.loc[((tmp['銘柄コード'] == c) & (tmp['建値'] == t)), 'stop_price'] = stop[((stop['銘柄コード'] == c) & (stop['建値'] == t))]['stop_price'].values[0]
                    # tmp.loc[((tmp['銘柄コード'] == c) & (tmp['建値'] == t)), 'stop_type'] = stop[((stop['銘柄コード'] == c) & (stop['建値'] == t))]['stop_type'].values[0]
                    
                tmp.loc[tmp['銘柄コード'] == c, 'close_order'] = 3 # 3=買埋
                tmp.loc[tmp['銘柄コード'] == c, 'margin_type'] = df[df['code'] == c]['margin_type'].values[0]
                if tmp[tmp['銘柄コード'] == c]['前日比率'].values[0] < -10:
                    syasu_flag += 1
                    
        tmp.loc[tmp['建市場'] == '東証', '建市場'] = 1
        tmp.loc[tmp['建市場'] == 'JNX', '建市場'] = 4
        tmp.loc[tmp['建市場'] == 'Chi-X', '建市場'] = 6

        clear_contents()
        # 決済注文データ入力
        xw.Range('B12', transpose=True).value = [2] * len(tmp) # 注文種類 1=信用、2=返済
        xw.Range('C12', transpose=True).value = tmp['close_order_id'].tolist() # 発注ID
        xw.Range('E12', transpose=True).value = tmp['銘柄コード'].tolist() # 銘柄コード
        xw.Range('F12', transpose=True).value = tmp['close_order'].tolist() # 売買区分 3=買埋、1=売埋
        xw.Range('G12', transpose=True).value = [0] * len(tmp) # 注文区分 0=通常、1=指値付通常、2=逆指値
        xw.Range('I12', transpose=True).value = tmp['margin_type'].tolist() # 信用区分 1=制度信用、2=一般信用(無期限)、4=いちにち信用
        xw.Range('J12', transpose=True).value = tmp['建玉数量'].tolist() # 注文数量
        xw.Range('K12', transpose=True).value = [0] * len(tmp) # 価格区分 0=成行、1=指値
        # xw.Range('L12', transpose=True).value = tmp['close_price'].tolist() # 注文価格
        xw.Range('M12', transpose=True).value = [4] * len(tmp) # 執行条件 1=本日中、3=寄付、4=引け、6=大引不成
        xw.Range('Y12', transpose=True).value = tmp['建日'].tolist() # 建日
        xw.Range('Z12', transpose=True).value = tmp['建値'].tolist() # 建単価
        xw.Range('AA12', transpose=True).value = tmp['建市場'].tolist() # 建市場
        
        # xw.Range('P12', transpose=True).value = tmp['stop_price'].tolist() # 逆指値条件価格
        # xw.Range('Q12', transpose=True).value = tmp['stop_type'].tolist() # 逆指値条件区分
        # xw.Range('R12', transpose=True).value = [1] * len(tmp) # 逆指値価格区分 0=成行、1=指値
        # xw.Range('S12', transpose=True).value = tmp['stop_price'].tolist() # 逆指値価格
            
        xw.Range('D12', transpose=True).value = [1] * len(tmp) # 発注トリガー
        time.sleep(5)
        xw.Range('D12', transpose=True).value = [0] * len(tmp) # 発注トリガー解除

        if os.path.exists(rf'{fpass}\order\order_history.csv'):
            df = pd.read_csv(rf'{fpass}\order\order_history.csv', encoding='cp932')
            df = pd.concat([df, tmp], join='outer')
            df.to_csv(rf'{fpass}\order\order_history.csv', index=False, encoding='cp932')
        else:
            tmp.to_csv(rf'{fpass}\order\order_history.csv', index=False, encoding='cp932')
        message += f'決済注文を{len(tmp)}銘柄送信しました\n'
        
        if sdaka_flag > 0:
            message += f'前日より10%以上値上がりしている銘柄が{sdaka_flag}社あります\n'
        if syasu_flag > 0:
            message += f'前日より10%以上値下がりしている銘柄が{syasu_flag}社あります'
    return message


def start_rss(): # 証券会社発注アプリ起動
    os.chdir(r'C:\Users\****\AppData\Local\****\Bin')
    global market_speed
    market_speed_path = r'C:\Users\****\AppData\Local\****\Bin\****.exe' # 証券会社の発注アプリ
    market_speed = subprocess.Popen(market_speed_path)
    #market_speed.wait()
    time.sleep(60) #バージョンアップの時間を考慮
    py.click(1280,800) #画面中央クリック
    time.sleep(1)
    py.typewrite('****')  #パスワード　ログインIDは記録させておくため入力しない
    time.sleep(2)
    py.press('Enter')
    time.sleep(15)

def start_excel(xl): # excel起動、RSS接続
    # py.hotkey("win","m")
    xl = xw.App(visible=True, add_book=False)
    time.sleep(3)
    wb = xl.books.open(r'C:\Users\****\Desktop\投資\trigger_order_samplesheet.xlsm')
    time.sleep(1)
    wb.app.activate(steal_focus=True)
    time.sleep(3)
    # excelのアドインを読込み
    addin_path=r'C:\Users\****\AppData\Local\****\Download\rss\****_64bit.xll'
    wb.app.api.RegisterXLL(addin_path)
    # py.click(1280,800) #画面中央をクリック
    time.sleep(10)
    sheet = xw.sheets['一括発注_国内株']
    sheet.activate()
    clear_contents() # 過去の入力データクリア (通常注文用)
    time.sleep(1)
    clear_contents_stop_order() # 過去の入力データクリア(逆指値注文用)
    xw.Range('B123:G159, B61:M80').clear_contents() # 過去の注文リストと建玉一覧クリア
    
    time.sleep(1)
    py.press("alt") # 以下、RSSに接続 
    time.sleep(1)
    py.press("y")
    py.press("2")
    time.sleep(1)
    py.press("y")
    py.press("1") 
    time.sleep(10)
    wb.app.activate(steal_focus=True)
    py.press("alt") # 以下、発注許可on
    time.sleep(1)
    py.press("y")
    py.press("2")
    time.sleep(1)
    py.press("y")
    py.press("2")
    time.sleep(3)
    py.press("Enter")
    app = xw.apps.active
    return xl

# 各国マーケットカレンダー読込み
n225 = pd.read_csv(rf'{fpass}\1d\N225.csv', parse_dates=['Date']).iloc[-1]['Date']
sp500 = pd.read_csv(rf'{fpass}\1d\GSPC.csv', parse_dates=['Date']).iloc[-1]['Date']
dax = pd.read_csv(rf'{fpass}\1d\GDAXI.csv', parse_dates=['Date']).iloc[-1]['Date']
order_today_date = pd.read_csv(rf'{fpass}\order\order_today_vif.csv', parse_dates=['Date']).iloc[-1]['Date']

# 注文に必要なデータが全て揃っているか確認
if (datetime.date.today() in pd.read_csv(rf'{fpass}\calendar_2023.csv', parse_dates=['Date'])['Date'].tolist()) & (n225 == sp500 == dax == order_today_date):
    pass
elif (datetime.date.today() not in pd.read_csv(rf'{fpass}\calendar_2023.csv', parse_dates=['Date'])['Date'].dt.date.values):
    send_line(message='本日は休場日です' )
    sys.exit()
elif ((n225 != sp500) | (n225 != dax) | (n225 != order_today_date)):
    send_line(message='学習データに欠損があります' )
    sys.exit()

# RSS, excel起動
market_speed = None
xl = None
start_rss()
xl = start_excel(xl)

time.sleep(random.randint(1, 30))
message = ''

try:
    message = send_cancel_order(message)
    time.sleep(random.randint(10, 60))
except:
    message += '取消注文時にエラーが発生しました'
    send_line(message)
    sys.exit()
try:
    message = send_close_order(message)
    send_line(message)
    time.sleep(60)
except:
    message += '返済注文時にエラーが発生しました'
    send_line(message)
    sys.exit()

# 発注アプリ＆excel終了
wb = xw.Book(r'C:\Users\****\Desktop\投資\trigger_order_samplesheet.xlsm')
wb.activate()
time.sleep(3)

wb.save()
time.sleep(3)
wb.close()
time.sleep(3)
xl.quit()
time.sleep(3)

market_speed.kill()