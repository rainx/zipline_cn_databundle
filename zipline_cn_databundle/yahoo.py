from zipline.data.bundles import register, yahoo_equities

"""
For ingest chinese history day bar from Yahoo

From both Shenzhen And Shanghai Stock Exchange
"""

from .all_stocks import get_all_stocks


def get_all_yahoo_stock_names():
    all_stocks = get_all_stocks()

    return [full_code(code) for code in all_stocks.index]

def full_code(code):
    if int(code[0]) >= 6:
        return "%s.ss" % code
    else:
        return '%s.sz' % code

def register_cn_bundle_from_yahoo(name):
    """
    register a new bundle of stocks from chinese market from yahoo
    :param name: the name of bundle
    :return: register result
    """
    return register(
        name,
        yahoo_equities(get_all_yahoo_stock_names()),
    )