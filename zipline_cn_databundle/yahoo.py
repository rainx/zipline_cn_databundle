from zipline.data.bundles import register, yahoo_equities
from ytd.downloader.StockDownloader import StockDownloader
from ytd.compat import unicode
from time import sleep
import re

"""
For ingest chinese history day bar from Yahoo

From both Shenzhen And Shanghai Stock Exchange
"""

# from .all_stocks import get_all_stocks


#def get_all_yahoo_stock_names():
#    all_stocks = get_all_stocks()
#
#    return [full_code(code) for code in all_stocks.index]

#def full_code(code):
#    if int(code[0]) >= 6:
#        return "%s.ss" % code
#    else:
#        return '%s.sz' % code

def register_cn_bundle_from_yahoo(name):
    """
    register a new bundle of stocks from chinese market from yahoo
    :param name: the name of bundle
    :return: register result
    """
    symbol_list = get_cn_stocks_from_yahoo_lookup()

    return register(
        name,
        yahoo_equities(dict(list(zip(symbol_list, symbol_list)))),
    )

# ref https://github.com/Benny-/Yahoo-ticker-symbol-downloader/blob/master/YahooTickerDownloader.py
def get_cn_stocks_from_yahoo_lookup():
    downloader = StockDownloader()
    loop = 0

    stocks = []

    while not downloader.isDone():
        symbols = downloader.nextRequest(insecure=True)
        print("Got " + str(len(symbols)) + " downloaded " + downloader.type + " symbols:")
        if (len(symbols) > 2):
            try:
                print(" " + unicode(symbols[0]))
                print(" " + unicode(symbols[1]))
                print("  ect...")
            except:
                print(" Could not display some ticker symbols due to char encoding")
        downloader.printProgress()
        loop = loop + 1

        if not downloader.isDone():
            sleep(2)  # So we don't overload the server.

    for symbol in downloader.getCollectedSymbols():
        ticker = symbol.ticker
        """
        Chinese Ticker Should looks like 000001.SZ (Shenzhen Market) or 600001.SS(ShanghaiMarket)
        """
        if re.match(r'\d{6}\.(SS|SZ)', ticker.upper()):
            symbols.append(ticker)

    return symbols

if __name__ == '__main__':
    symbols = get_cn_stocks_from_yahoo_lookup()
    print('Writing To Files: ')
    with open('/tmp/cn_symbols.txt', 'w') as f:
        f.write("\n".join(symbols))
    print('Done!')