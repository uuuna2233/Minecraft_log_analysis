import re
import pandas as pd
import datetime
from minecraft_log import connect_google
from gspread_dataframe import set_with_dataframe
from gspread_formatting import *

def log_utw(ufw, log):
    ''' 處理 utw_log '''
    
    Year = datetime.strftime(datetime.today(),'%Y')
    
    time = []
    Ip = []
    for logdata in ufw:
        # 轉換時間格式，使其與賽事 log 一致
        date = datetime.strptime(Year + logdata[0:14],'%Y%b %d %H:%M:%S')
        time.append(date)
        
        # 提取登入 ip 位址與 port 口
        ip = re.findall(r'(?:[0-9]{1,3}\.){3}[0-9]{1,3}', logdata)[0]
        if re.findall(r'SPT=(\d+)', logdata) != []:
            port = re.findall(r'SPT=(\d+)', logdata)[0]
        else:
            port = 'null'
        Ip.append(ip + ':' + port)
    result = {"time":time, "Ip":Ip}
    df = pd.DataFrame(result)
    
    
    ''' 處理 minecraft_log '''
    
    row = []    
    for logdata in log:
        # 提取玩家自訂 id 和 系統分配 id
        if 'UUID of player' in logdata:
            person = list(re.findall(r"UUID of player (.*) is (.*)", logdata)[0])
        # 提取玩家登入 ip
        elif 'logged in with entity id' in logdata:
            ipaddr = list(re.findall(r"\[\/(.*)\] logged in with entity", logdata))
            data = person + ipaddr
            row.append(data)
        else:
            continue
    
    # 合併兩個 Dataframe，以 ip 對照玩家 id
    df1 = pd.DataFrame(row, columns=['player_id','server_id','ip_addr'])
    df2 = pd.merge(df, df1, left_on="Ip", right_on="ip_addr").drop('ip_addr',axis=1)
    
    # 寫入 Google Sheet
    set_with_dataframe(wks, df2, row=1, col=17)

def log_joined(log):
    ''' 計算同時在線人數 '''
        
    row = []
    count = 0
    for logdata in log:
        
        # 加入遊戲
        if 'joined the game' in logdata:
            time = [logdata[:11] + logdata[12:20]]
            
            if count >= 1:
                d1 = datetime.datetime.strptime(time[0][:10], '%Y-%m-%d')
                d2 = datetime.datetime.strptime(row[-1][0][:10], '%Y-%m-%d')
                
                # 若日期不是同一天，重新計算在線人數
                if (d1-d2).days >= 1:
                    count = 0
                    
            count += 1
            row.append(time + [str(count)])
        
        # 退出遊戲
        elif 'left the game' in logdata:
            time = [logdata[:11] + logdata[12:20]]
            
            if count >= 1:
                d1 = datetime.datetime.strptime(time[0][:10], '%Y-%m-%d')
                d2 = datetime.datetime.strptime(row[-1][0][:10], '%Y-%m-%d')
                
                # 若日期不是同一天，重新計算在線人數
                if (d1-d2).days >= 1:
                    count = 0
                
            count -= 1
            row.append(time + [str(count)])
        else:
            continue
        
    df3 = pd.DataFrame(row, columns=['time','count'])

    # 寫入 Google Sheet
    set_with_dataframe(wks, df3, row=1, col=22)
    
if __name__ == "__main__":

    # Connect google sheet
    wks = connect_google()

    # 美化格式，uses DEFAULT_FORMATTER，置中、底色、粗體、固定頂欄
    fmt = CellFormat(horizontalAlignment='CENTER')
    fmt2 = CellFormat(backgroundColor=color(0.89, 0.79, 0.85),
    textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), horizontalAlignment='CENTER')
    
    format_cell_ranges(wks, [('A:Z', fmt), ('Q1:T1', fmt2), ('V1:W1', fmt2)])
    set_frozen(wks, rows=1)
    
    # Open File
    f_name = 'ufw.log'
    with open(f_name,'r', encoding='utf-8') as file:
        ufw = file.readlines()
    f_name = 'minecraft.log'    
    with open(f_name,"r", encoding='utf-8') as file:
        log = file.readlines()
        
    log_utw(ufw, log)
    log_joined(log)

