# securities_trading
Набор python скриптов, который позволяет:
1. Создать базу данных SQLite по результатам торгов на бирже Nasdaq/Nyse (укажите интересующие Вас тикеры и путь к Вашей базе данных в файле [constants.py](https://github.com/MaximAleksandrovich/securities_trading/blob/main/constants.py). Запустите скрипт [Priceparsing_to_SQL.py](https://github.com/MaximAleksandrovich/securities_trading/blob/main/Priceparsing_to_SQL.py))
2. Делать апдейт базы (необходимо запустить скрипт [Update.py](https://github.com/MaximAleksandrovich/securities_trading/blob/main/Priceparsing_to_Update.py))
3. Выводить график изменения стоимости акций за последние N торговых сессий (для этого необходимо в скрипте Stock_screen.py в переменной days
   указать количество торговых сессий, за которое Вы хотите посмотреть динамику изменения акций, и запустить Stock_screen.py). По умолчанию
   на график выводится 5 тикеров, которые показали лучший результат, и 3 тикера с худшими результатами. Количество лучших тикеров можно
   изменить, корректируя переменную top_symbols. Количество худших тикеров можно изменить, корректируя переменную bottom_symbols. Можно
   снять комментарии со строк между top_symbols и bottom_symbols и тогда на графике отразятся, например, тикеры из 3-го десятка по убыванию
   результата.
5. Проигрывать различные стратегии. В папке Strategies сохранены варианты стратегий покупки/продажи акций исходя из изменения показателя WD40
   и WD40_rank. Показатели WD40 и WD40_rank придуманы автором данного репозитория, добавлены в модель оценки корреляции изменения стоимости
   акций от различных показателей и показали минимальную, но бОльшую по сравнению с остальными показателями, корреляцию с изменением
   стоимости акций. Для любого периода в прошлом можно подобрать такую стратегию, которая обеспечила бы хороший финансовый результат в этом
   периоде, но при изменении периода, стратегия оказывается, как правило, не эффективной.
6. Получать список компаний, ранжированный по убыванию показателя 'forwardPE' (для этого необходимо запустить скрипт Forward_PE.py, список тикеров
   будет соответствовать указанном в файле Constants.py). В списке указываются тикеры, прогнозы роста прибыли и выручки, сектора, к которым
   относятся тикеры. Показатель 'forwardPE' это текущая стоимость акций, деленная на прогнозную прибыль. Возможно, этот показатель - лучшая
   отправная точка для поиска недооцененных на рынке акций.
