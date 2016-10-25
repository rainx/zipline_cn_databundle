from squant.data.stock import file_parser
import os
import datetime

"""
Squant is a private library that parse the data from our private source

That read data from binary files

via RainX<i@rainx.cn>

ipython 3 only
"""


# 需要设定沪深文件目录
CQCX_SH = os.environ.get("CQCX_SH")
CQCX_SZ = os.environ.get("CQCX_SZ")

if not CQCX_SH or not CQCX_SZ:
    raise Exception("need set cqcx file on CQCX_SH CQCX_SZ")

if not os.path.isfile(CQCX_SH) \
    or not os.path.isfile(CQCX_SZ):
    raise Exception("setting CQCX_SH, CQCX_SZ path is not correct")

CQCX_LIST = (CQCX_SH, CQCX_SZ)

def load_splits_and_dividends():
    """
    获取所有除权出息的信息, 根据zipline平台的特点,忽略配股信息
    :return:
    """

    splits = {}
    dividends = {}

    for CQCX in CQCX_LIST:
        cqcx_data = file_parser.get_cqcx(CQCX.encode("utf-8"))
        for row in cqcx_data:
            code = str(row['stock']).zfill(6)
            # sgVal 送股数，每1000股送股数
            if row['sgVal'] != 0:
                if code not in splits.keys():
                    splits[code] = []
                splits[code].append({
                    'effective_date' : int_to_date(row['date']),
                    'ratio' : 1000 / (1000 + row['sgVal']),
                })

            if row['pxVal'] != 0:
                if code not in dividends.keys():
                    dividends[code] = []
                dividends[code].append({
                    'amount' : row['pxVal'] / 1000,
                    'ex_date' : int_to_date(row['date']),
                })


    return splits, dividends


def int_to_date(d):
    d = str(d)
    return datetime.date(int(d[:4]), int(d[4:6]), int(d[6:]))

if __name__ == '__main__':

    import tushare as ts
    import pandas as pd

    ts_symbols = ts.get_stock_basics()


    symbols = []


    # 获取股票数据
    i = 0
    total = len(ts_symbols)
    for index, row in ts_symbols.iterrows():
        i = i +1
        if i > 10:
            break

        srow = {}
        srow['t'] = 1
        srow['symbol'] = index
        srow['asset_name'] = row['name']
        symbols.append(srow)

    df_symbols = pd.DataFrame(data=symbols).sort_values('symbol')
    symbol_map = pd.DataFrame.copy(df_symbols.symbol)

    raw_splits, raw_dividends = load_splits_and_dividends()

    splits = []
    dividends = []
    for sid, code in symbol_map.iteritems():
        if code in raw_splits:
            split = pd.DataFrame(data=raw_splits[code])
            split['sid'] = sid
            split.index = split['effective_date'] = pd.DatetimeIndex(split['effective_date'])
            splits.append(split)
        if code in raw_dividends:
            dividend = pd.DataFrame(data = raw_dividends[code])
            dividend['sid'] = sid
            dividend['record_date'] = dividend['declared_date'] = dividend['pay_date'] = pd.NaT
            dividend.index = dividend['ex_date'] = pd.DatetimeIndex(dividend['ex_date'])
            dividends.append(dividend)

    print(pd.concat(splits, ignore_index=True))
    print(pd.concat(dividends, ignore_index=True))
