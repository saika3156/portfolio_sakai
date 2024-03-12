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
    line_notify_token = '****'
    line_notify_api = 'https://notify-api.line.me/api/notify'
    payload = {'message': message}
    headers = {'Authorization': 'Bearer ' + line_notify_token}
    requests.post(line_notify_api, data=payload, headers=headers)

def clear_contents(): # excel注文データをクリア
    xw.Range('B12:C31, E12:G31, I12:M31, P12:S31, Y12:AB31').clear_contents()
def clear_contents_stop_order(): # excel損切り注文データをクリア
    xw.Range('B84:C103, E84:G103, I84:M103, P84:S103, Y84:AB103').clear_contents()

def clear_order_id_list(): # excel過去の注文データクリア
    sheet = xw.sheets['一括発注_国内株']
    sheet.activate()
    
    time.sleep(1) # 以下、RSS切断
    py.press("alt")
    time.sleep(1)
    py.press("y")
    py.press("2")
    time.sleep(1)
    py.press("y")
    py.press("1") 
    time.sleep(5)
    
    xw.Range('B123:G159').clear_contents() # 当日の新規注文リスト削除
    
    time.sleep(1) # 以下、RSS接続
    py.press("alt")
    time.sleep(1)
    py.press("y")
    py.press("2")
    time.sleep(1)
    py.press("y")
    py.press("1")
    time.sleep(5)
    py.press("alt") # 以下、発注許可on
    time.sleep(1)
    py.press("y")
    py.press("2")
    time.sleep(1)
    py.press("y")
    py.press("2")
    time.sleep(3)
    py.press("Enter")
    
# 買建て、売建て注文送信
def send_order(message, base_amount=500000): # base_amount=発注単位(円)
    df = pd.read_csv(rf'{fpass}\order\order_today_vif.csv') # 当日の売買銘柄を読込み
    if len(df) > 0:
        # wb = xw.Book(r'C:\Users\****\Desktop\投資\trigger_order_samplesheet.xlsm')
        sheet = xw.sheets['一括発注_国内株']
        sheet.activate()

        for c, o in zip(df['code'], df['order']): # 買建、売建、それぞれの発注データを作成
            if o == 3: # 3=買い
                xw.Range('A110').value = c
                time.sleep(3)
                if xw.Range('G110').value == 1:
                    df.loc[df['code'] == c, 'margin_type'] = 4 # いちにち信用_買建 可
                elif (xw.Range('G110').value == 0) & (xw.Range('E110').value == 1):
                    df.loc[df['code'] == c, 'margin_type'] = 1 # 制度信用_買建 可

                latest_price = pd.read_csv(rf'{fpass}\1d\{c}.csv', parse_dates=['Date']).iloc[-1]['Close']
                moq = latest_price * xw.Range('B116').value
                if moq >= base_amount * 0.9:
                    df.loc[df['code'] == c, 'order_unit'] = xw.Range('B116').value
                    df.loc[df['code'] == c, 'order_amount'] = moq
                elif moq < base_amount * 0.9:
                    df.loc[df['code'] == c, 'order_unit'] = round(base_amount / moq) * xw.Range('B116').value # math.ceil() ← 切り上げに使用
                    df.loc[df['code'] == c, 'order_amount'] = round(base_amount / moq) * xw.Range('B116').value * latest_price # math.ceil()
            elif o == 1: # 1=売り
                xw.Range('A110').value = c
                time.sleep(3)
                if (xw.Range('H110').value == 1) & (xw.Range('B110').value > 0): # いちにち信用_売建 可 & 売建可能数 > 0
                    df.loc[df['code'] == c, 'margin_type'] = 4
                elif (xw.Range('F110').value == 1): # 制度信用_売建 可
                    df.loc[df['code'] == c, 'margin_type'] = 1
                # elif (xw.Range('H110').value == 0) & (xw.Range('F110').value == 0) & (xw.Range('L110').value == 1) & (xw.Range('B110').value > 0): # 一般信用_無期限_売建 可
                #     df.loc[df['code'] == c, 'margin_type'] = 2
                else:
                    df.loc[df['code'] == c, 'margin_type'] = np.nan
                
                latest_price = pd.read_csv(rf'{fpass}\1d\{c}.csv', parse_dates=['Date']).iloc[-1]['Close']
                moq = latest_price * xw.Range('B116').value
                if moq >= base_amount * 0.9: # 発注基準額 < 最低発注額の場合、最低発注額を採用する
                    df.loc[df['code'] == c, 'order_unit'] = xw.Range('B116').value
                    df.loc[df['code'] == c, 'order_amount'] = moq
                elif moq < base_amount * 0.9:
                    df.loc[df['code'] == c, 'order_unit'] = round(base_amount / moq) * xw.Range('B116').value # math.ceil()
                    df.loc[df['code'] == c, 'order_amount'] = round(base_amount / moq) * xw.Range('B116').value * latest_price # math.ceil()
                 # いちにち信用売かつ、売建可能数<発注予定数 の場合、制度信用に変更する。
                if (df[df['code'] == c]['margin_type'].values[0] == 4) & (df[df['code'] == c]['order_unit'].values[0] > xw.Range('B110').value):
                    if xw.Range('F110').value == 1:
                        df.loc[df['code'] == c, 'margin_type'] = 1
                    else:
                        df.loc[df['code'] == c, 'order_unit'] = xw.Range('B110').value # 制度信用が不可の場合は売建可能数を注文数量とする
                    
        clear_contents()
        # excelに注文データを入力
        xw.Range('B12', transpose=True).value = [1] * len(df) # 注文種類 1=信用、2=返済
        xw.Range('C12', transpose=True).value = [i * 10 for i in df['code'].astype(int)] # 発注ID
        xw.Range('E12', transpose=True).value = df['code'].tolist() # 銘柄コード
        xw.Range('F12', transpose=True).value = df['order'].tolist() # 売買区分 3=買、1=売
        xw.Range('G12', transpose=True).value = [0] * len(df) # 注文区分 0=通常、1=指値付通常、2=逆指値
        xw.Range('I12', transpose=True).value = df['margin_type'].tolist() # 信用区分 1=制度信用、4=いちにち信用
        xw.Range('J12', transpose=True).value = df['order_unit'].tolist() # 注文数量
        xw.Range('K12', transpose=True).value = [0] * len(df) # 価格区分 0=成行、1=指値
        xw.Range('M12', transpose=True).value = [3] * len(df) # 執行条件 1=本日中、3=寄付、4=引け、6=大引不成
        
        df['stop_flag'] = 0 # 逆指値注文済フラグ

        xw.Range('D12', transpose=True).value = [1] * len(df) # 発注トリガー 1=発注、0=待機
        time.sleep(5)
        xw.Range('D12', transpose=True).value = [0] * len(df) # 発注トリガー 1=発注、0=待機
        time.sleep(5)
        
        tmp = pd.DataFrame(xw.Range('B123:G159').value, columns=xw.Range('B122:G122').value)
        tmp = tmp[tmp['発注結果'].isin([i for i in tmp['発注結果'] if isinstance(i, str)])]
        for code in df['code']:
            if '発注済み' in tmp[tmp['発注ID'] == (code * 10)]['発注結果'].tolist(): # 発注成功=1,発注エラー=2
                df.loc[df['code'] == code, 'ordered_flag'] = 1
            else:
                df.loc[df['code'] == code, 'stop_flag'] = 2
                df.loc[df['code'] == code, 'ordered_flag'] = 1
        df.to_csv(rf'{fpass}\order\order_today_vif.csv', index=False)
        
        if (df['ordered_flag'].count() == df['code'].count()) & (df[df['stop_flag'] != 0]['stop_flag'].count() == 0):
            message += '新規注文は全て発注済です\n'
        else:
            message += f"未完了の注文が{df[df['stop_flag'] != 0]['code'].count()}件、code{df[df['stop_flag'] != 0]['code'].tolist()}あります\n"
            
        message += f"買い:{df[(df['order'] == 3) & (df['stop_flag'] == 0)]['order_amount'].count()}銘柄,{round(df[(df['order'] == 3) & (df['stop_flag'] == 0)]['order_amount'].sum() / 10000)}万円\n"
        message += f"売り:{df[(df['order'] == 1) & (df['stop_flag'] == 0)]['order_amount'].count()}銘柄,{round(df[(df['order'] == 1) & (df['stop_flag'] == 0)]['order_amount'].sum() / 10000)}万円\n"
    else:
        message += '注文用データがありません'
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

# 逆指値注文(損切り)
def send_stop_order(message, limit=0.005): # 値動きが0.5%を超えたら損切り
    # wb = xw.Book(r'C:\Users\****\Desktop\投資\trigger_order_samplesheet.xlsm')
    sheet = xw.sheets['一括発注_国内株']
    sheet.activate()
    clear_contents_stop_order()
    
    df = pd.read_csv(rf'{fpass}\order\order_today_vif.csv', parse_dates=['Date'])
    pending_time = 0
    
    if df[df['stop_flag'] == 0]['code'].count() == 0:
        message += '全て逆指値注文済みです'
        return message
    while df[df['stop_flag'] == 0]['code'].count() > 0: # stop_flagが全て1になるまでループ
        df = pd.read_csv(rf'{fpass}\order\order_today_vif.csv', parse_dates=['Date'])
        stop_today = pd.read_csv(rf'{fpass}\order\stop_today_vif.csv', encoding='cp932')
    
        tmp = pd.DataFrame(xw.Range('B61:M80').value, columns=xw.Range('B60:M60').value) # 信用建玉リスト from RSS
        tmp = tmp[tmp['銘柄コード'].isin([int(i) for i in tmp['銘柄コード'] if isinstance(i, float)])] # 銘柄コードが数値以外の行を除外

        codes = [i for i in df[df['stop_flag'] == 0]['code'] if i in tmp['銘柄コード'].astype(int).tolist()] # stop_flag = Nan かつ 建玉があるコードのみ抽出
        if len(codes) > 0: # stop_flag = Nan かつ 建玉一覧に上がっているコードがあるか判定
            for c in codes:
                code_count = tmp[tmp['銘柄コード'] == c]['銘柄コード'].count()
                if code_count == 1:
                    tmp.loc[tmp['銘柄コード'] == c, '発注ID'] = c * 10 + 1
                else: # 同一銘柄で複数の建玉が存在する場合(建値が異なる場合)
                    for t, i in zip(tmp[tmp['銘柄コード'] == c]['建値'], range(code_count)):
                        tmp.loc[((tmp['銘柄コード'] == c) & (tmp['建値'] == t)), '発注ID'] = c * 100 + 11 + i # 銘柄コード * 100 + 11 + 連番 0～

                if tmp.loc[tmp['銘柄コード'] == c, '売買'].values[0] == '買建':
                    tmp.loc[tmp['銘柄コード'] == c, 'stop_price'] = mround(tmp[tmp['銘柄コード'] == c]['建値'].values[0] * (1 - limit))
                    tmp.loc[tmp['銘柄コード'] == c, 'stop_order'] = 1 # 1=売埋
                    tmp.loc[tmp['銘柄コード'] == c, 'stop_type'] = 2 # 逆指値価格区分 2=以下
                    tmp.loc[tmp['銘柄コード'] == c, 'margin_type'] = df[df['code'] == c]['margin_type'].values[0]

                elif tmp.loc[tmp['銘柄コード'] == c, '売買'].values[0] == '売建':
                    tmp.loc[tmp['銘柄コード'] == c, 'stop_price'] = mround(tmp[tmp['銘柄コード'] == c]['建値'].values[0] * (1 + limit))
                    tmp.loc[tmp['銘柄コード'] == c, 'stop_order'] = 3 # 3=買埋
                    tmp.loc[tmp['銘柄コード'] == c, 'stop_type'] = 1 # 逆指値価格区分 1=以上
                    tmp.loc[tmp['銘柄コード'] == c, 'margin_type'] = df[df['code'] == c]['margin_type'].values[0]

            tmp.loc[tmp['建市場'] == '東証', '建市場'] = 1
            tmp.loc[tmp['建市場'] == 'JNX', '建市場'] = 4
            tmp.loc[tmp['建市場'] == 'Chi-X', '建市場'] = 6

            tmp_copy = tmp.copy()
            tmp = tmp[tmp['銘柄コード'].isin(codes)]
            
            clear_contents_stop_order()
            # excelに逆指値注文データを入力
            xw.Range('B84', transpose=True).value = [2] * len(tmp) # 注文種類 1=信用、2=返済
            xw.Range('C84', transpose=True).value = tmp['発注ID'].tolist() # 発注ID
            xw.Range('E84', transpose=True).value = tmp['銘柄コード'].tolist() # 銘柄コード
            xw.Range('F84', transpose=True).value = tmp['stop_order'].tolist() # 売買区分 3=買、1=売
            xw.Range('G84', transpose=True).value = [2] * len(tmp) # 注文区分 0=通常、1=指値付通常、2=逆指値
            xw.Range('I84', transpose=True).value = tmp['margin_type'].tolist() # 信用区分 1=制度信用、2=一般信用(無期限)、4=いちにち信用
            xw.Range('J84', transpose=True).value = tmp['建玉数量'].tolist() # 注文数量
            xw.Range('M84', transpose=True).value = [1] * len(tmp) # 執行条件 1=本日中、6=大引不成
            xw.Range('Y84', transpose=True).value = tmp['建日'].tolist() # 建日
            xw.Range('Z84', transpose=True).value = tmp['建値'].tolist() # 建単価
            xw.Range('AA84', transpose=True).value = tmp['建市場'].tolist() # 建市場
            xw.Range('P84', transpose=True).value = tmp['stop_price'].tolist() # 逆指値条件価格
            xw.Range('Q84', transpose=True).value = tmp['stop_type'].tolist() # 逆指値条件区分
            xw.Range('R84', transpose=True).value = [0] * len(tmp) # 逆指値価格区分 0=成行、1=指値

            xw.Range('D84', transpose=True).value = [1] * len(tmp) # 発注トリガー 1=発注、0=待機
            time.sleep(2)
            xw.Range('D84', transpose=True).value = [0] * len(tmp) # 発注トリガー 1=発注、0=待機
            
            if len(stop_today) == 0:
                tmp_copy.to_csv(rf'{fpass}\order\stop_today_vif.csv', index=False, encoding='cp932')
            else: # 逆指値注文の実行が本日2回目以降の場合、既に注文済みのデータと今回の注文分を結合
                stop_today = pd.concat([stop_today, tmp], join='outer')
                stop_today.to_csv(rf'{fpass}\order\stop_today_vif.csv', index=False, encoding='cp932')
            for c in codes:
                df.loc[df['code'] == c, 'stop_flag'] = 1
            df.to_csv(rf'{fpass}\order\order_today_vif.csv', index=False)
        if pending_time == 330: # 8:59:30開始
            send_line(f"5分経過して約定していない注文が{df[df['stop_flag'] == 0]['code'].count()}件、{df[df['stop_flag'] == 0]['code'].tolist()}")
        if pending_time > 430: # 一定時間経過しても注文が約定しない銘柄がある場合はbreak
            break
        time.sleep(2)
        pending_time += 2
    
    time.sleep(3)
    tmp = pd.DataFrame(xw.Range('B123:G167').value, columns=xw.Range('B122:G122').value) # 注文一覧 from RSS
    tmp = tmp[tmp['発注ID'].isin([i for i in tmp['発注ID'] if isinstance(i, float)])]
    stop_today = pd.read_csv(rf'{fpass}\order\stop_today_vif.csv', encoding='cp932')
    for i in stop_today['発注ID'].tolist():
        try:
            stop_today.loc[stop_today['発注ID'] == i, 'order_number'] = int(tmp[tmp['発注ID'] == i]['注文番号'].values[0])
        except:
            message += f'注文番号を取得できない発注ID:{i}\n'
    stop_today.to_csv(rf'{fpass}\order\stop_today_vif.csv', index=False, encoding='cp932')
    message += '逆指値注文を送信しました'
    return message

# 取消注文(逆指値注文を取消し)
def send_cancel_order(message):
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

# 決済注文(買建・売建決済)
def send_close_order(message):
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

if __name__ == '__main__':
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

    # RSS, EXCEL起動
    market_speed = None
    xl = None
    start_rss()
    xl = start_excel(xl)

    time.sleep(random.randint(1, 30))
    message = ''
    message = send_order(message, base_amount=500000) # 発注
    send_line(message)

    # 買建て、売建て注文送信
    try:
        clear_order_id_list() # 注文一覧削除(逆指値注文時に注文番号を確実に取得する必要があるため、新規信用注文一覧を削除しておく)
    except:
        send_line(message='注文一覧削除でエラーが発生しました')

    while datetime.datetime.now().time() < datetime.time(8, 59, 30): # 8時59分になったら逆指値注文(それまでは待機)
        time.sleep(1)
    message = ''
    message = send_stop_order(message, limit=0.005) # 逆指値注文(損切り注文)
    send_line(message)

    while datetime.datetime.now().time() < datetime.time(14, 50, 0): # 14時50分になったら決済注文(それまでは待機)
        time.sleep(60)
        
    # 決済注文送信
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