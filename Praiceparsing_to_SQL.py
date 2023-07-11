# Загрузка истории изменения цен в SQLite

import csv
import io
import sqlite3
import requests
import pathlib
from pathlib import Path
from time import sleep

# Переменные, указываемые пользователем:
path = Path(pathlib.Path.home(), 'Documents', 'SQLite', 'Securities.db') # Путь к папке с базой данных
period='period1=1483228800&period2=1688515200' # Период анализа
# period='period1=1688083200&period2=1688515200'
# Перечень компаний для анализа
symbols= ('AMD', 'ADBE', 'ABNB', 'ALGN', 'AMZN', 'AMGN', 'AEP', 'ADI', 'ANSS', 'AAPL', 'AMAT', 'ASML',
          'TEAM', 'ADSK', 'ATVI', 'ADP', 'AZN', 'AVGO', 'BIDU', 'BIIB', 'BMRN', 'CDNS', 'CHTR', 'CPRT',
          'CRWD', 'CTAS', 'CSCO', 'CMCSA', 'COST', 'CSX', 'CTSH', 'DDOG', 'DOCU', 'DXCM', 'DLTR', 'EA',
          'EBAY', 'EXC', 'FAST', 'META', 'FISV', 'FTNT', 'GILD', 'GOOGL', 'HON', 'ILMN', 'INTC', 'INTU',
          'ISRG', 'MRVL', 'IDXX', 'JD', 'KDP', 'KLAC', 'KHC', 'LRCX', 'LCID', 'LULU', 'MELI', 'MAR', 'MTCH',
          'MCHP', 'MDLZ', 'MRNA', 'MNST', 'MSFT', 'MU', 'NFLX', 'NTES', 'NVDA', 'NXPI', 'OKTA', 'ODFL',
          'ORLY', 'PCAR', 'PANW', 'PAYX', 'PDD', 'PYPL', 'PEP', 'QCOM', 'REGN', 'ROST', 'SIRI', 'SGEN', 'SPLK',
          'SWKS', 'SBUX', 'SNPS', 'TSLA', 'TXN', 'TMUS', 'VRSN', 'VRSK', 'VRTX', 'WBA', 'WDAY', 'XEL', 'ZM', 'ZS')

def exclude_first_line(csv_reader):
    next(csv_reader, None)  # пропуск заголовка
    for row in csv_reader:
        yield row

def download_csv(url): # Загрузка CSV-файла
    response = requests.get(url, headers=Headers, params=None)
    csv_data = response.text
    csv_reader = csv.reader(io.StringIO(csv_data), delimiter=',')
    for row in exclude_first_line(csv_reader):
        if not row: continue  # Пропустить пустые строки
        if len(row) < 7: continue # Проверка, что строка содержит минимум 7 значений
        (Symbol, Date, Open, High, Low, Close, Adj_close, Volume, Wday_index) = (symbol,
                        row[0],row[1], row[2], row[3], row[4], row[5], row[6], None)
        c.execute("INSERT INTO Prices VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (Symbol, Date, Open, High, Low, Close, Adj_close, Volume, Wday_index))  # Вставка данных в таблицу Prices
    sqlite_connection.commit() # Сохранение изменений и закрытие соединения

# Указание юзер-агента
Headers={'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                       ' Chrome/103.0.0.0 Safari/537.36','accept':'*/*'}
# Установка соединения с базой данных SQLite
dburi = 'file:{}?mode=rw'.format(path)
sqlite_connection = sqlite3.connect(dburi, uri=True)
c = sqlite_connection.cursor()

# Создание таблицы
create_table_query = "CREATE TABLE IF NOT EXISTS Prices (Symbol TEXT, Date DATE, Open DECIMAL, High DECIMAL, " \
                     "Low DECIMAL, Close DECIMAL, Adj_Close DECIMAL, Volume INTEGER, WDayIndex INTEGER)"
c.execute(create_table_query)
sqlite_connection.commit()

# Загрузка CSV-файла
for symbol in symbols:
    url = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?{period}&interval=1d&events' \
          f'=history&includeAdjustedClose=true'
    csv_data = download_csv(url)
    print(symbol)
    sleep(0.5)

# Добавление индекса рабочего дня в таблицу цен
rows = c.execute('SELECT * FROM Prices ORDER BY Symbol, Date').fetchall()
column_names = [i[1] for i in c.execute('PRAGMA table_info("Prices")').fetchall()]
table = []
for row in rows:
  table.append({column_names[i]: row[i] for i in range(len(row))})
Dates=c.execute('Select Distinct Date FROM Prices ORDER BY Date').fetchall()
index = list (i for i in range (len(Dates)))
index_table = {str(Dates [i])[2:-3]: index[i] for i in range(len(Dates)) }
for row in table[::-1]:
  row['WDayIndex']=index_table [row['Date']]+1
to_db = [(i['Symbol'], i['Date'], i['Open'], i['High'], i['Low'], i['Close'], i['Adj_Close'], i['Volume'], i['WDayIndex']) for i in table]
c.execute('DELETE from Prices') # Удаление таблицы без индекса рабочего дня
sqlite_insert_query = 'INSERT INTO Prices (Symbol, Date, Open, High, Low, Close, Adj_Close, Volume, WDayIndex) ' \
                        'VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)' # Вставка данных с индексом рабочего дня
c.executemany(sqlite_insert_query, to_db)
sqlite_connection.commit()
sqlite_connection.close()