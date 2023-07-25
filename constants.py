# Пропишите путь к своей базе данных
db = 'C:/Путь/к/вашей/папке/base.db'

# выберите тикеты
symbols = ('AMD', 'ADBE', 'ABNB', 'ALGN', 'AMZN', 'AMGN', 'AEP', 'ADI', 'ANSS', 'AAPL', 'AMAT', 'ASML', 'AAL',
           'TEAM', 'ADSK', 'ATVI', 'ADP', 'AZN', 'AVGO', 'BIDU', 'BIIB', 'BMRN', 'CDNS', 'CHTR', 'CPRT', 'PATH',
           'CRWD', 'CTAS', 'CSCO', 'CMCSA', 'COST', 'CSX', 'CTSH', 'DDOG', 'DOCU', 'DXCM', 'DLTR', 'EA',
           'EBAY', 'EXC', 'FAST', 'META', 'FI', 'FTNT', 'GILD', 'GOOGL', 'HON', 'ILMN', 'INTC', 'INTU',
           'ISRG', 'MRVL', 'IDXX', 'JD', 'KDP', 'KLAC', 'KHC', 'LRCX', 'LCID', 'LULU', 'MELI', 'MAR', 'MTCH',
           'MCHP', 'MDLZ', 'MRNA', 'MNST', 'MSFT', 'MU', 'NFLX', 'NTES', 'NVDA', 'NXPI', 'OKTA', 'ODFL',
           'ORLY', 'PCAR', 'PANW', 'PAYX', 'PDD', 'PYPL', 'PEP', 'QCOM', 'REGN', 'ROST', 'SIRI', 'SGEN', 'SPLK',
           'SWKS', 'SBUX', 'SNPS', 'TSLA', 'TXN', 'TMUS', 'VRSN', 'VRSK', 'VRTX', 'WBA', 'WDAY', 'XEL', 'ZM', 'ZS')

# Укажите своего или наиболее популярного юзер-агента
Headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                         ' Chrome/103.0.0.0 Safari/537.36', 'accept': '*/*'}