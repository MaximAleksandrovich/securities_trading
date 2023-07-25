import yfinance as yf
import pandas as pd
import datetime
import sqlite3
from constants import db, symbols

# Подключение к базе данных SQLite
conn = sqlite3.connect(db)
cursor = conn.cursor()

info = yf.Ticker('AMD').info
print(info.keys())
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
result = pd.DataFrame()
today = datetime.date.today()
print('текущая дата', today)

for symbol in symbols:
    df = pd.DataFrame([{'Symbol': symbol, 'trailingPE': None,
                        'forwardPE': yf.Ticker(symbol).info['forwardPE'],
                        'earningsQGrowth': None,
                        'earningsGrowth': None,
                        'revenueGrowth': None,
                        'industry': yf.Ticker(symbol).info['industry'],
                        'sector': yf.Ticker(symbol).info['sector'],
                        'parsingDate': today}])
    try:
        df.at[0, 'trailingPE'] = yf.Ticker(symbol).info['trailingPE']
    except KeyError:
        pass
    try:
        df.at[0, 'earningsQGrowth'] = yf.Ticker(symbol).info['earningsQuarterlyGrowth']
    except KeyError:
        pass
    try:
        df.at[0, 'earningsGrowth'] = yf.Ticker(symbol).info['earningsGrowth']
    except KeyError:
        pass
    try:
        df.at[0, 'revenueGrowth'] = yf.Ticker(symbol).info['revenueGrowth']
    except KeyError:
        pass
    print(symbol)
    result = pd.concat([result, df], ignore_index=True)

result = result.sort_values('forwardPE')
result.to_sql('Fundamentals', conn, if_exists='append')
print(result)
