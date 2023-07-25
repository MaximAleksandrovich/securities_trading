import pandas as pd
import sqlite3
from constants import db

NDays = 66
conn = sqlite3.connect(db)
c = conn.cursor()
c.execute('SELECT MAX(WDayIndex) FROM Prices')
index_max = c.fetchone()[0]
c.execute('SELECT MIN(WDayIndex) FROM PricesWithWDR')
index_min = c.fetchone()[0]
print(index_min)
pd.set_option('display.max_rows', None)
result_df = pd.DataFrame()
for index in range(1300, 1400):
    query = f'select wd.Symbol, wwd.Date, wd.WD40_rank, wd.WDayIndex, ' \
            f'wwd.Close*1.1 as PriceOut, wwd.Close as PriceIn ' \
            f'from (SELECT * FROM PricesWithWDR where WD40_rank in (99, 100, 101, 102) ' \
            f'and WDayIndex ={index}) as wd ' \
            f'join (SELECT * FROM PricesWithWDR where WD40_rank not in (99, 100, 101, 102) ' \
            f'and WDayIndex ={index - 1}) as wdd on wdd.Symbol=wd.Symbol ' \
            f'join (SELECT * FROM PricesWithWDR where WDayIndex ={index + 1}) as wwd on wwd.Symbol=wd.Symbol '
    df = pd.read_sql_query(query, conn)
    if df.empty:
        pass
    else:
        print(df)
        c.execute(f"SELECT MIN(WDayIndex) FROM Prices where WDayIndex>{index} "
                  f"and Symbol='{df['Symbol'][0]}' and Close>{df['PriceOut'][0]} ")
        df['WDayIndexOut'] = c.fetchone()[0]
        if df['WDayIndexOut'][0] is None:
            if index + NDays+1 > 1643:
                df['WDayIndexOut'] = 1643
            else:
                df['WDayIndexOut'] = index + NDays+1
            c.execute(f"SELECT Close FROM Prices where WDayIndex={df['WDayIndexOut'][0]} "
                      f"and Symbol='{df['Symbol'][0]}'")
            df['Income'] = (c.fetchone()[0] - df['PriceIn']) / df['PriceIn']
        else:
            if df['WDayIndexOut'][0] > index + NDays+1:
                df['WDayIndexOut'] = index + NDays+1
                c.execute(f"SELECT Close FROM Prices where WDayIndex={df['WDayIndexOut'][0]} "
                          f"and Symbol='{df['Symbol'][0]}'")
                df['Income'] = (c.fetchone()[0] - df['PriceIn']) / df['PriceIn']
            else:
                df['Income'] = 0.1
        df['Period'] = df['WDayIndexOut'] - df['WDayIndex']
        result_df = pd.concat([df, result_df])
        print(index)
        print(df)

print(result_df)
average_period = result_df['Period'].mean()
print("Среднее значение столбца Period:", average_period)
count_less_than_25 = (result_df['Period'] < 25).sum()
print("Количество значений меньше 25 в столбце Period:", count_less_than_25)
count_less_than_25 = (result_df['Period'] > NDays).sum()
print(f"Количество значений больше {NDays} в столбце Period:", count_less_than_25)
mean_period_less_than_100 = result_df.loc[result_df['Period'] < NDays, 'Period'].mean()
print(f"Среднее значение столбца Period (для значений < {NDays}):", mean_period_less_than_100)
maximum_period_less_than_100 = result_df.loc[result_df['Period'] < NDays, 'Period'].max()
print(f"Максимальное значение столбца Period (для значений < {NDays}):", maximum_period_less_than_100)
mean_Income = result_df.loc[result_df['Period'] > NDays, 'Income'].mean()
print("Среднее значение Убытков:", mean_Income)
mean_Income = result_df.loc[result_df['Period'] > NDays]
print("Убытки:", mean_Income)
# mean_Income = result_df.loc[result_df['Period'] > NDays, 'Income']
# print("Убытки2:", mean_Income)
