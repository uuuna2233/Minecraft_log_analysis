# ! pip install gspread gspread-dataframe oauth2client gspread-formatting

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials as sac
from  gspread_formatting import CellFormat
import re
import numpy as np
from datetime import date

today = str(date.today())

def connect_google():
    # Set api key
    auth_json = 'YOUR API KEY.json'
    # Set scopes
    gs_scopes = ['https://spreadsheets.google.com/feeds']
    
    # 以金鑰及操作範圍設定憑證資料
    cr = sac.from_json_keyfile_name(auth_json, gs_scopes)
    # 取得操作憑證
    gc = gspread.authorize(cr)
    
    # Opening a Spreadsheet by entire spreadsheet’s url
    gsheet = gc.open_by_url('YOUR GOOGLE SFEET URL')
    # Creating a Worksheet，若想定義表格範圍，可加入 rows="", cols="" 參數
    try:
        worksheet = gsheet.add_worksheet(title=f"{today}")
    except:
        pass
    # Selecting a Worksheet by title
    wks = gsheet.worksheet(f"{today}")
    
    return wks

def log_game():
    ''' 玩家生存戰況 '''
    
    # 根據 log 產生規則，以正則提取玩家戰績以進行後續計算
    row = []
    for logdata in log:
        if 'was slain by' in logdata and 'message' not in logdata:
            time = "".join(re.findall(r'\[\d+:\d+:\d+\]', logdata))
            time = [today +" "+ time[1:-1]]
            person = list(re.findall(r"([^: ][\/*\.*\ *\&*\w]*) was slain by (\w*)", logdata)[0])
            data = time + person
            row.append(data)
        else:
            continue
    df = pd.DataFrame(row, columns=['time','loss','win'])
    order = ['time','win','loss']
    df = df[order]
    
    # NPC 列表
    MonsterList = ['Fisherman','Shepherd','Villager','Blaze', 'Creeper', 'Drowned', 'Elder Guardian', 'Endermite', 'Evoker', 'Ghast', 'Guardian', 'Hoglin', 'Husk', 'Magma Cube', 'Phantom', 'Piglin Brute', 'Pillager', 'Ravager', 'Shulker', 'Silverfish', 'Skeleton', 'Slime', 'Stray', 'Vex', 'Vindicator', 'Warden', 'Witch', 'Wither Skeleton', 'Zoglin', 'Zombie', 'Zombie Villager', 'Enderman', 'Piglin', 'Spider', 'Cave Spider', 'Zombified Piglin', 'Ender Dragon', 'Wither','Iron Golem']
    
    # 服主列表
    ServerOwnerList = ['Hoyiqiang_TW', 'mignon0923', 'lavender']
    
    
    # 排除與怪物戰鬥和服主產生的擊殺或死亡
    for i in range(1,3):
        df.iloc[:,i] = df.iloc[:,i].replace(MonsterList, np.nan).replace(ServerOwnerList, np.nan)
    df.dropna(inplace=True)
    
    # pandas 的 value_counts 函數對 Series 的值進行計數且排序，結果返回 Series
    loss = df["loss"].value_counts()
    win = df["win"].value_counts()
    
    # 合併兩個 Series 並 rename 欄位名稱為 Id, win_count, loss_count
    df2 = win.to_frame(name = "win_count").reset_index().rename(columns={'index': 'Id'})
    df3 = loss.to_frame(name = "loss_count").reset_index().rename(columns={'index': 'Id'})
    df4 = pd.merge(df2, df3, how='outer', on=["Id","Id"]).fillna(0)

    # 將 count欄位的型態轉為整數形式
    df4['win_count'] = df4['win_count'].astype('int64')
    df4['loss_count'] = df4['loss_count'].astype('int64')

    # 寫入 Google Sheet
    # df 為包含時間段的流水紀錄，df4 為統整每位玩家輸贏次數
    set_with_dataframe(wks, df)
    set_with_dataframe(wks, df4, row=1, col=5)

def log_advancement():
    ''' 玩家成就達成 '''
    
    # 根據 log 產生規則，以正則提取玩家成就達成的紀錄
    row = []
    for logdata in log:
        if 'has made the advancement' in logdata:
            time = "".join(re.findall(r'\[\d+:\d+:\d+\]', logdata))
            time = [today +" "+ time[1:-1]]
            person = list(re.findall(r"([^: ][\/*\.*\ *\&*\w]*) has made the advancement \[(.*)\]", logdata)[0])
            data = time + person
            row.append(data)
        else:
            continue
    df = pd.DataFrame(row, columns=['time','id','advancement'])

    # 寫入 Google Sheet
    set_with_dataframe(wks, df, row=1, col=9)

def log_server():
    ''' 伺服器過載紀錄 '''
    
    # 根據 log 產生規則，以正則提取伺服器過載事件
    row = []
    for logdata in log:
        if 'Is the server overloaded?' in logdata:
            time = "".join(re.findall(r'\[\d+:\d+:\d+\]', logdata))
            time = [today +" "+ time[1:-1]]
            person = list(re.findall(r"Is the server overloaded\? Running (.*)ms or (.*) ticks", logdata)[0])
            data = time + person
            row.append(data)
        else:
            continue
    df = pd.DataFrame(row,columns=['time','ms','ticks'])

    # 寫入 Google Sheet
    set_with_dataframe(wks, df, row=1, col=13)
    
if __name__ == "__main__":

    # Connect google sheet
    wks = connect_google()
   
    # 美化格式，uses DEFAULT_FORMATTER，置中、底色、粗體、固定頂欄
    fmt = CellFormat(horizontalAlignment='CENTER')
    fmt2 = CellFormat(backgroundColor=color(0.67, 0.81, 0.85),
    textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), horizontalAlignment='CENTER')
    
    format_cell_ranges(wks, [('A:Z', fmt), ('A1:C1', fmt2), ('E1:G1', fmt2), ('I1:K1', fmt2), ('M1:O1', fmt2)])
    set_frozen(wks, rows=1)
    
    # Open File
    f_name = 'minecraft.log'
    with open(f_name,"r",encoding='utf-8') as file:
        log = file.readlines()
        
    log_game()
    log_advancement()
    log_server()


