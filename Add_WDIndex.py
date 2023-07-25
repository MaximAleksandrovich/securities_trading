import sqlite3
import pathlib
from pathlib import Path
from datetime import datetime
from constants import db

conn = sqlite3.connect(db)
cursor = conn.cursor()

rows = c.execute('SELECT * FROM Prices ORDER BY Symbol, Date').fetchall()
column_names = [i[1] for i in c.execute('PRAGMA table_info("Prices")').fetchall()]
table = []
for row in rows:
    table.append({column_names[i]: row[i] for i in range(len(row))})
Dates = c.execute('Select Distinct Date FROM Prices ORDER BY Date').fetchall()
index = list(i for i in range(len(Dates)))
print(index)
index_table = {str(Dates[i])[2:-3]: index[i] for i in range(len(Dates))}
print(index_table)
for row in table[::-1]:
    row['WDayIndex'] = index_table[row['Date']] + 1
to_db = [
    (i['Symbol'], i['Date'], i['Open'], i['High'], i['Low'], i['Close'], i['Adj_Close'], i['Volume'], i['WDayIndex'])
    for i in table]
c.execute('DELETE from Prices')
print('Удаление выполнено')
sqlite_insert_query = 'INSERT INTO Prices (Symbol, Date, Open, High, Low, Close, Adj_Close, Volume, WDayIndex) ' \
                      'VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)'
c.executemany(sqlite_insert_query, to_db)
print('Обновленные данные добавлены!')
conn.commit()
conn.close()
