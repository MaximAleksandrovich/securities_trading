import pandas as pd
import sqlite3
from constants import db, symbols

conn = sqlite3.connect(db)
cursor = conn.cursor()
c.execute('SELECT MAX(WDayIndex) FROM Prices')
index_max = c.fetchone()[0]
c.execute('SELECT MIN(WDayIndex) FROM PricesWithWDR')
index_min = c.fetchone()[0]
print(index_min)
result_df = pd.DataFrame()
for index in range(500, 800):
    query = f'select wd.Symbol, wd.WD40_rank, wwd.Date, p.Date as DateOut , wwd.Close as PriceIn, ' \
            f'p.Close as PriceOut, iif ((p.Close - wwd.Close)>0, ' \
            f'(p.Close - wwd.Close*1.08)/(wwd.Close*0.08),-1.05) as Income ' \
            f'from (SELECT * FROM PricesWithWDR where WD40_rank in (99, 100, 101, 102) and WDayIndex ={index}) as wd ' \
            f'join (SELECT * FROM PricesWithWDR where WD40_rank not in (99, 100, 101, 102) ' \
            f'and WDayIndex ={index + 1}) as wdd on wdd.Symbol=wd.Symbol ' \
            f'join (SELECT * FROM PricesWithWDR where WDayIndex ={index + 2}) as wwd on wwd.Symbol=wd.Symbol ' \
            f'left join (select Symbol, Close, WDayIndex, Date from Prices where WDayIndex={index + 21}) ' \
            f'as p on p.Symbol=wd.Symbol'
    df = pd.read_sql_query(query, conn)
    if df.empty:
        pass
    else:
        print(df)
        result_df = pd.concat([df, result_df])
        print(index)
if result_df.empty:
    print("В заданном периоде задаваемые условия не встречались")
else:
    print(result_df)
    average_Income = result_df['Income'].mean()
    print("Среднее значение столбца Income:", average_Income)
