# Загрузка истории изменения цен в SQLite

import csv
import io
import sqlite3
import requests
import pathlib
from pathlib import Path
from time import sleep
from constants import db, symbols, Headers

# Подключаемся к базе данных SQLite
conn = sqlite3.connect(db)
cursor = conn.cursor()
period = 'period1=1483228800&period2=1689552145'  # Период анализа (на сайте yahoo finance)


def exclude_first_line(csv_reader):
    next(csv_reader, None)  # пропуск заголовка
    for data_string in csv_reader:
        yield data_string


def download_csv(source):  # Загрузка CSV-файла
    response = requests.get(source, headers=Headers, params=None)
    csv_data = response.text
    csv_reader = csv.reader(io.StringIO(csv_data), delimiter=',')
    for datarow in exclude_first_line(csv_reader):
        if not datarow: continue  # Пропустить пустые строки
        if len(row) < 7: continue  # Проверка, что строка содержит минимум 7 значений
        (Symbol, Date, Open, High, Low, Close, Volume, Wday_index) = (symbol,
                                                                      row[0], row[1], row[2], row[3], row[4], row[6],
                                                                      None)
        c.execute("INSERT INTO Prices VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                  (Symbol, Date, Open, High, Low, Close, Volume, Wday_index))  # Вставка данных в таблицу Prices
    conn.commit()  # Сохранение изменений и закрытие соединения


# Создание таблицы
create_table_query = "CREATE TABLE IF NOT EXISTS Prices (Symbol TEXT, Date DATE, Open DECIMAL, High DECIMAL, " \
                     "Low DECIMAL, Close DECIMAL, Volume INTEGER, WDayIndex INTEGER)"
c.execute(create_table_query)
conn.commit()

# Загрузка CSV-файла
for symbol in symbols:
    url = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?{period}&interval=1d&events' \
          f'=history&includeAdjustedClose=true'
    download_csv(url)
    print(symbol)
    sleep(0.5)

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
to_db = [
    (i['Symbol'], i['Date'], i['Open'], i['High'], i['Low'], i['Close'], i['Adj_Close'], i['Volume'], i['WDayIndex'])
    for i in table]
c.execute('DELETE from Prices')  # Удаление таблицы без индекса рабочего дня
sqlite_insert_query = 'INSERT INTO Prices (Symbol, Date, Open, High, Low, Close, Adj_Close, Volume, WDayIndex) ' \
                      'VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)'  # Вставка данных с индексом рабочего дня
c.executemany(sqlite_insert_query, to_db)
conn.commit()
conn.close()
