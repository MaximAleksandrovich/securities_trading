import pandas as pd
import sqlite3
from constants import db

NDays = 60
# Подключаемся к базе данных SQLite
conn = sqlite3.connect(db)
c = conn.cursor()
c.execute('SELECT MAX(WDayIndex) FROM Prices')
index_max = c.fetchone()[0]
c.execute('SELECT MIN(WDayIndex) FROM PricesWithWDR')
index_min = c.fetchone()[0]
pd.set_option('display.max_rows', None)
result_df = pd.DataFrame()
for index in range(1200, 1300):
    query = f'select wd.Symbol,  wwd.Date,  wd.WD40, wd.WDayIndex,  ' \
            f'op.Close as PriceOut, wwd.Close as PriceIn, wd.WD40_rank, (op.Close-wwd.Close)/wwd.Close as Income ' \
            f'from (SELECT * FROM PricesWithWDR where WD40>0 and WD40_rank>5 and WDayIndex ={index}) as wd ' \
            f'join (SELECT * FROM PricesWithWDR where WD40>0 ' \
            f'and WDayIndex ={index - 40}) as wdd on wdd.Symbol=wd.Symbol ' \
            f'join (SELECT * FROM PricesWithWDR where WD40>0 ' \
            f'and WDayIndex ={index - 80}) as ddd on ddd.Symbol=wd.Symbol ' \
            f'join (SELECT * FROM PricesWithWDR where WD40>0 ' \
            f'and WDayIndex ={index - 120}) as ab on ab.Symbol=wd.Symbol ' \
            f'join (SELECT * FROM PricesWithWDR where WD40>0 ' \
            f'and WDayIndex ={index - 20}) as ac on ac.Symbol=wd.Symbol ' \
            f'join (SELECT * FROM PricesWithWDR where WDayIndex ={index + 1}) as wwd on wwd.Symbol=wd.Symbol ' \
            f'join (SELECT * FROM PricesWithWDR where WDayIndex ={index + NDays}) as op on op.Symbol=wd.Symbol'
    df = pd.read_sql_query(query, conn)
    result_df = pd.concat([df, result_df])

print(result_df)
average_income = result_df['Income'].mean() * 100
print("Среднее значение столбца Income:", average_income)
