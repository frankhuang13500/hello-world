import sqlite3
import pandas as pd
import datetime as dt

STOCKDATA_SQL_FA       = "../StockInfo/stock_data_FA.sqlite3"
STOCKDATA_SQL_DAY_LAST = "../StockInfo/stock_data_last.sqlite3"
STOCKDATA_SQL_DAY_OLD = "../StockInfo/stock_data_old.sqlite3"
STOCKDATA_SQL_FRA       = "../StockInfo/stock_data_FRA.sqlite3"
"""
STOCKDATA_SQL_FA       = "D:\PythonPrj\jupyter\Financial\StockInfo\stock_data_FA.sqlite3"
STOCKDATA_SQL_DAY_LAST = "D:\PythonPrj\jupyter\Financial\StockInfo\stock_data_last.sqlite3"
STOCKDATA_SQL_DAY_OLD  = "D:\PythonPrj\jupyter\Financial\StockInfo\stock_data_old.sqlite3"
STOCKDATA_SQL_FRA       = "D:\PythonPrj\jupyter\Financial\StockInfo\stock_data_FRA.sqlite3"
"""

# 從stock_data.sqlite3取得每日財報, 由於資料量龐大未使用whare時存取會變得很久, where可指定年份及股票代碼
# ex: get_sql_daily_report(end=datetime.date(2020, 11, 6), day=300, target=' * ', sqlwhere='where 證券代號 =' + '2330')
# 證券代號,成交股數,成交筆數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,最後揭示買價,最後揭示買量,最後揭示賣價,最後揭示賣量,本益比
# dropstr=['最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量']
def get_sql_daily_report(target: str = ' * ', sqlwhere: str = '', dropstr: list = []):
    print(sqlwhere)
    
    table="daily_price"
    sqltrt = 'select {target} from {table} {sqlwhere} '.format(table=table,target=target, sqlwhere=sqlwhere)
    conn = sqlite3.connect(STOCKDATA_SQL_DAY_LAST)
    data1 = pd.read_sql(sqltrt, conn, index_col=['date'])
    conn.close()
    conn = sqlite3.connect(STOCKDATA_SQL_DAY_OLD)
    data2 = pd.read_sql(sqltrt , conn, index_col=['date'])
    conn.close()
    frames = [data1, data2]
    data = pd.concat(frames)
    if dropstr != []:
        data.drop(dropstr, axis=1, inplace=True)
    data.index = pd.to_datetime(data.index, format='%Y-%m-%d')
    data = data.sort_index()  # 最好加這一行，否則可能無法執行data.loc[start : end]
    #data = data.rename(columns={"開盤價": "open", "最高價": "high", "最低價": "low", "收盤價": "close", "成交股數": "volume"})
    return data


def get_sql_TwFinanceRatio(market_type, purpose, target: str = ' * ', sqlwhere: str = '', dropstr: list = []):
    # Profit analysis, Profit analysis oct, financial analysis, financial analysis oct
    print(sqlwhere)
    if market_type == 'sii': #'上市'
        if purpose =='營益分析':
            table = "'Profit analysis'"
        else: #'財務結構分析'
            table = "'financial analysis'"
    else: #'上櫃'
        if purpose =='營益分析':
            table = "'Profit analysis oct'"
        else: #'財務結構分析'
            table = "'financial analysis oct'"

    sqltrt = 'select {target} from {table} {sqlwhere} '.format(table=table, target=target, sqlwhere=sqlwhere)
    #print("SQL STR:"+sqltrt)
    conn = sqlite3.connect(STOCKDATA_SQL_FA)
    data = pd.read_sql(sqltrt, conn)
    conn.close()
    if dropstr != []:
        data.drop(dropstr, axis=1, inplace=True)
    return data

def get_sql_TwRevenue(market_type, target: str = ' * ', sqlwhere: str = '', dropstr: list = []):
    # Revenue sii, Revenue oct
    print(sqlwhere)
    if market_type == 'sii': #'上市'
        table = "'Revenue sii'"
    else: #'上櫃'
        table = "'Revenue oct'"

    sqltrt = 'select {target} from {table} {sqlwhere} '.format(table=table, target=target, sqlwhere=sqlwhere)
    #print("SQL STR:"+sqltrt)
    conn = sqlite3.connect(STOCKDATA_SQL_FA)
    data = pd.read_sql(sqltrt, conn)
    conn.close()
    if dropstr != []:
        data.drop(dropstr, axis=1, inplace=True)
    return data

# OCT daily price 只有財報出來後的隔天資料,目的是twstock_sql中計算財報出來後的本益比
def get_sql_octDaily(target: str = ' * ', sqlwhere: str = '', dropstr: list = []):
    # Revenue sii, Revenue oct
    print(sqlwhere)
    table = "'daily_price_oct'"

    sqltrt = 'select {target} from {table} {sqlwhere} '.format(table=table, target=target, sqlwhere=sqlwhere)
    #print("SQL STR:"+sqltrt)
    conn = sqlite3.connect(STOCKDATA_SQL_FA)
    data = pd.read_sql(sqltrt, conn)
    conn.close()
    if dropstr != []:
        data.drop(dropstr, axis=1, inplace=True)
    return data

# 公司股利
def get_sql_twDividend(market_type, target: str = ' * ', sqlwhere: str = '', dropstr: list = []):
    
    #未實作
    return

# 減資換發新股
def get_sql_twCptRdc(market_type, target: str = ' * ', sqlwhere: str = '', dropstr: list = []):
    
    #未實作
    return
    
    
    
