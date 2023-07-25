import yfinance as yf
import pandas as pd
import sqlite3
import datetime
from constants import db, symbols

# Подключение к базе данных SQLite
conn = sqlite3.connect(db)  # Пропишите путь к своей базе данных
c = conn.cursor()
c.execute('SELECT MAX(Date) FROM Prices')
date = c.fetchone()[0]
c.execute(f'DELETE FROM Prices WHERE Date="{date}"')
conn.commit()
c.execute(f'DELETE FROM PricesWithWDR WHERE Date="{date}"')
conn.commit()

# Загружаем историю торгов по выбранным тикетам
for symbol in symbols:
    data = yf.Ticker(symbol).history(start=date)
    data = data.reset_index()
    data['Date'] = pd.to_datetime(data['Date']).dt.date
    data['WDayIndex'] = None
    data.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)
    data.insert(0, 'Symbol', symbol)  # добавляем столбец Symbol
    data.to_sql('Prices', conn, if_exists='append', index=False)
    print(data)

# Добавление индекса рабочего дня в таблицу цен
rows = c.execute('SELECT * FROM Prices ORDER BY Symbol, Date').fetchall()
column_names = [i[1] for i in c.execute('PRAGMA table_info("Prices")').fetchall()]
table = []
for row in rows:
    table.append({column_names[i]: row[i] for i in range(len(row))})
Dates = c.execute('Select Distinct Date FROM Prices ORDER BY Date').fetchall()
index = list(i for i in range(len(Dates)))
index_table = {str(Dates[i])[2:-3]: index[i] for i in range(len(Dates))}
for row in table[::-1]:
    row['WDayIndex'] = index_table[row['Date']] + 1
to_db = [(i['Symbol'], i['Date'], i['Open'], i['High'], i['Low'], i['Close'], i['Volume'], i['WDayIndex']) for i in
         table]
c.execute('DELETE from Prices')  # Удаление таблицы без индекса рабочего дня
sqlite_insert_query = 'INSERT INTO Prices (Symbol, Date, Open, High, Low, Close, Volume, WDayIndex) ' \
                      'VALUES (?, ?, ?, ?, ?, ?, ?,?)'  # Вставка данных с индексом рабочего дня
c.executemany(sqlite_insert_query, to_db)
conn.commit()

# расчет WD40
c.execute('SELECT MAX(WDayIndex) FROM Prices')
index = c.fetchone()[0]
c.execute('SELECT MAX(WDayIndex) FROM PricesWithWDR ')
index_min = c.fetchone()[0]
result_df = pd.DataFrame()
for index in range(index_min + 1, index + 1):
    print('index=', index)
    query = f'select p.Symbol,p.Date, p.Close, p.Volume, p.WDayIndex, (p.Volume/p.Close*(p.Close-c2.Close)' \
            f'+c2.Volume/c2.Close*(c2.Close-c3.Close)+' \
            f'c3.Volume/c3.Close*(c3.Close-c4.Close)+' \
            f'c4.Volume/c4.Close*(c4.Close-c5.Close)+' \
            f'c5.Volume/c5.Close*(c5.Close-c6.Close)+' \
            f'c6.Volume/c6.Close*(c6.Close-c7.Close)+' \
            f'c7.Volume/c7.Close*(c7.Close-c8.Close)+' \
            f'c8.Volume/c8.Close*(c8.Close-c9.Close)+' \
            f'c9.Volume/c9.Close*(c9.Close-c10.Close)+' \
            f'c10.Volume/c10.Close*(c10.Close-c11.Close)+' \
            f'c11.Volume/c11.Close*(c11.Close-c12.Close)+' \
            f'c12.Volume/c12.Close*(c12.Close-c13.Close)+' \
            f'c13.Volume/c13.Close*(c13.Close-c14.Close)+' \
            f'c14.Volume/c14.Close*(c14.Close-c15.Close)+' \
            f'c15.Volume/c15.Close*(c15.Close-c16.Close)+' \
            f'c16.Volume/c16.Close*(c16.Close-c17.Close)+' \
            f'c17.Volume/c17.Close*(c17.Close-c18.Close)+' \
            f'c18.Volume/c18.Close*(c18.Close-c19.Close)+' \
            f'c19.Volume/c19.Close*(c19.Close-c20.Close)+' \
            f'c20.Volume/c20.Close*(c20.Close-c21.Close)+' \
            f'c21.Volume/c21.Close*(c21.Close-c22.Close)+' \
            f'c22.Volume/c22.Close*(c22.Close-c23.Close)+' \
            f'c23.Volume/c23.Close*(c23.Close-c24.Close)+' \
            f'c24.Volume/c24.Close*(c24.Close-c25.Close)+' \
            f'c25.Volume/c25.Close*(c25.Close-c26.Close)+' \
            f'c26.Volume/c26.Close*(c26.Close-c27.Close)+' \
            f'c27.Volume/c27.Close*(c27.Close-c28.Close)+' \
            f'c28.Volume/c28.Close*(c28.Close-c29.Close)+' \
            f'c29.Volume/c29.Close*(c29.Close-c30.Close)+' \
            f'c30.Volume/c30.Close*(c30.Close-c31.Close)+' \
            f'c31.Volume/c31.Close*(c31.Close-c32.Close)+' \
            f'c32.Volume/c32.Close*(c32.Close-c33.Close)+' \
            f'c33.Volume/c33.Close*(c33.Close-c34.Close)+' \
            f'c34.Volume/c34.Close*(c34.Close-c35.Close)+' \
            f'c35.Volume/c35.Close*(c35.Close-c36.Close)+' \
            f'c36.Volume/c36.Close*(c36.Close-c37.Close)+' \
            f'c37.Volume/c37.Close*(c37.Close-c38.Close)+' \
            f'c38.Volume/c38.Close*(c38.Close-c39.Close)+' \
            f'c39.Volume/c39.Close*(c39.Close-c40.Close)+' \
            f'c40.Volume/c40.Close*(c40.Close-c41.Close))/v.Vol as WD40 ' \
            f'from (select * from Prices where WDayIndex={index}) as p ' \
            f'join (select * from Prices where WDayIndex={index - 1}) as c2 on c2.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 2}) as c3 on c3.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 3}) as c4 on c4.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 4}) as c5 on c5.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 5}) as c6 on c6.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 6}) as c7 on c7.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 7}) as c8 on c8.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 8}) as c9 on c9.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 9}) as c10 on c10.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 10}) as c11 on c11.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 11}) as c12 on c12.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 12}) as c13 on c13.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 13}) as c14 on c14.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 14}) as c15 on c15.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 15}) as c16 on c16.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 16}) as c17 on c17.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 17}) as c18 on c18.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 18}) as c19 on c19.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 19}) as c20 on c20.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 20}) as c21 on c21.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 21}) as c22 on c22.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 22}) as c23 on c23.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 23}) as c24 on c24.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 24}) as c25 on c25.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 25}) as c26 on c26.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 26}) as c27 on c27.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 27}) as c28 on c28.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 28}) as c29 on c29.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 29}) as c30 on c30.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 30}) as c31 on c31.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 31}) as c32 on c32.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 32}) as c33 on c33.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 33}) as c34 on c34.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 34}) as c35 on c35.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 35}) as c36 on c36.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 36}) as c37 on c37.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 37}) as c38 on c38.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 38}) as c39 on c39.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 39}) as c40 on c40.Symbol = p.Symbol ' \
            f'join (select * from Prices where WDayIndex={index - 40}) as c41 on c41.Symbol = p.Symbol ' \
            f'join (select Symbol, avg(Volume) as Vol from Prices where WDayIndex>={index - 80} ' \
            f'and WDayIndex<={index} group by Symbol) as v on v.Symbol = p.Symbol'
    df = pd.read_sql_query(query, conn)
    result_df = pd.concat([df, result_df])
result_df['WD40_rank'] = result_df.groupby('Date')['WD40'].rank('first', ascending=False)
result_df.to_sql('PricesWithWDR', conn, if_exists='append')
print(result_df)

conn.close()
