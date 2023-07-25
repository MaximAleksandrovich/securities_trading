import pandas as pd
import sqlite3
import pathlib
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from constants import db

conn = sqlite3.connect(db)
cursor = conn.cursor()
days = 150  # Количество дней, доходность за которые необходимо сравнить
# Получаем индекс отчетного дня
cursor.execute('SELECT MAX(WDayIndex) FROM Prices')
index = cursor.fetchone()[0]
# Загружаем данные на конец отчетного дня
query = f'SELECT Symbol, Date, Close FROM Prices where WDayIndex = "{index-days}"'
result_df = pd.read_sql_query(query, conn)
# Загружаем данные за каждый день анализируемого периода
for day in range(index-days, index+1):
    query = f'SELECT Symbol, Close FROM Prices where WDayIndex = "{day}"'
    df = pd.read_sql_query(query, conn)

    # Переименовываем столбец чтобы его название не пересекалось с имеющимся столбцом
    df.columns = ['Symbol', f'{day}Rise']

    # Присоединяем таблицу за новый день
    result_df = result_df.merge(df, on=['Symbol'], how='inner')

    # Рассчитываем доход за соответствующее количество дней
    result_df[f'{day}Rise'] = result_df[f'{day}Rise'] / result_df['Close'] - 1

print('result_df\n', result_df)
# Сохраняем анализ в CSV-файл
result_df.to_csv('result.csv', index=False)

# Строим график прироста стоимости самых удачных и самых неудачных акций

# создаем список показателей
rise_columns = [f'{day}Rise' for day in range(index-days, index+1)]

# выбираем 5 символов с максимальным показателем {index}Rise
top_symbols = result_df.nlargest(5, f'{index}Rise')['Symbol'].tolist()

# # выбираем 10 символов с показателем {index}Rise c 30-го по 40-й
# top_symbols = result_df.nlargest(40, f'{index}Rise')['Symbol'].tolist()[-10:]

# выбираем 3 символа с минимальным показателем {index}Rise
bottom_symbols = result_df.nsmallest(3, f'{index}Rise')['Symbol'].tolist()

# создаем график
fig, ax = plt.subplots()

# добавляем данные для каждого символа из списка top_symbols
for symbol in top_symbols:
    symbol_data = result_df[result_df['Symbol'] == symbol][rise_columns].values[0]
    ax.plot(rise_columns, symbol_data, label=symbol)

# добавляем данные для каждого символа из списка bottom_symbols
for symbol in bottom_symbols:
    symbol_data = result_df[result_df['Symbol'] == symbol][rise_columns].values[0]
    ax.plot(rise_columns, symbol_data, label=symbol)

# добавляем легенду и подписи осей
ax.legend(ncol=10, loc='upper center', bbox_to_anchor=(0.5, 1.15))
ax.set_xlabel('Rise')
ax.set_ylabel('Value')

# отображаем график
plt.show()
