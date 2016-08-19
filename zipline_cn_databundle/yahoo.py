from zipline.data.bundles import register, yahoo_equities
import requests
import os
from pandas_datareader.data import DataReader

"""
For ingest chinese history day bar from Yahoo

From both Shenzhen And Shanghai Stock Exchange
"""

from .all_stocks import get_all_stocks, get_cache_dir


def get_all_yahoo_stock_names(cache=True):

    if cache==False:
        print('Get All Stock List.....')
    all_stocks = get_all_stocks(cache=cache)

    return [full_code(code) for code in all_stocks.index]

def full_code(code):
    if int(code[0]) >= 6:
        return "%s.SS" % code
    else:
        return '%s.SZ' % code

def register_cn_bundle_from_yahoo(name, cache=True):
    """
    register a new bundle of stocks from chinese market from yahoo
    :param name: the name of bundle
    :return: register result
    """
    symbol_list = get_filtered_symbols(cache)

    return register(
        name,
        yahoo_equities(dict(list(zip(symbol_list, symbol_list)))),
    )

def check_code(code):
    """
    XXX: used check_data_reader instead
    :param code:
    :return:
    """
    checkurl = r'http://finance.yahoo.com/_finance_doubledown/api/resource/searchassist;gossipConfig=%7B%22url%22%3A%7B%22host%22%3A%22s.yimg.com%22%2C%22path%22%3A%22%2Fxb%2Fv6%2Ffinance%2Fautocomplete%22%2C%22query%22%3A%7B%22appid%22%3A%22yahoo.com%22%2C%22nresults%22%3A10%2C%22output%22%3A%22yjsonp%22%2C%22region%22%3A%22US%22%2C%22lang%22%3A%22en-US%22%7D%2C%22protocol%22%3A%22https%22%7D%2C%22isJSONP%22%3Atrue%2C%22queryKey%22%3A%22query%22%2C%22resultAccessor%22%3A%22ResultSet.Result%22%2C%22suggestionTitleAccessor%22%3A%22symbol%22%2C%22suggestionMeta%22%3A%5B%22symbol%22%2C%22name%22%2C%22exch%22%2C%22type%22%2C%22exchDisp%22%2C%22typeDisp%22%5D%7D;searchTerm={{CODE}}?bkt=3E0%2507canary&dev_info=0&device=desktop&intl=us&lang=en-US&partner=none&region=US&site=finance&tz=America%2FLos_Angeles&ver=0.101.302&returnMeta=true'
    referer = 'http://finance.yahoo.com/'

    url = checkurl.replace('{{CODE}}', code)

    response = requests.get(url, headers={
        'Referer'       : 'http://finance.yahoo.com/',
        'User-Agent'    : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
    })
    try:
        data = response.json()
        items = data['data']['items']

        if len(items) > 0:
            for item in items:
                if item['symbol'].upper() == code.upper():
                    print('.')
                    return True

    except Exception as e:
        print(str(e))
        return False

    print('x')
    return False

def check_data_reader(code):
    try:
        DataReader(code, 'yahoo')
    except:
        print('%s ok!' % code)
        return False
    print('%s not ok!' % code)
    return True

def get_filtered_symbols(cache=True):
    cache_dir = get_cache_dir()
    file_path = os.path.join(cache_dir, 'symbols.txt')

    if cache and os.path.isfile(file_path):
        with open(file_path, 'r') as f:
            content = f.read()
            if content:
                return content.split("\n")

    symbols = get_all_yahoo_stock_names(cache)

    if cache==False:
        print('Check availablity from Yahoo...')
    filtered_symbols = list(filter(check_data_reader, symbols))
    print('cache output to %s' % file_path)
    with open(file_path, 'w') as f:
        f.write("\n".join(filtered_symbols))
    print('done!')
    return filtered_symbols

def zipline_cn_databundle_update():
    print('Start to fetch data and update cache')
    get_filtered_symbols(cache=False)