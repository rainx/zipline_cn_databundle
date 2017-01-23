from squant.data.stock import file_parser
from squant.zipline.datasource import get_symbol_list
import os
import datetime
from .tdx.reader import TdxReader, TdxFileNotFoundException
import pandas as pd

"""
Squant is a private library that parse the data from our private source

That read data from binary files

via RainX<i@rainx.cn>

ipython 3 only
"""


# 需要设定沪深文件目录
CQCX_SH = os.environ.get("CQCX_SH")
CQCX_SZ = os.environ.get("CQCX_SZ")

TDX_DIR = os.environ.get("TDX_DIR")

if not CQCX_SH or not CQCX_SZ:
    raise Exception("need set cqcx file on CQCX_SH CQCX_SZ")

if not os.path.isfile(CQCX_SH) \
    or not os.path.isfile(CQCX_SZ):
    raise Exception("setting CQCX_SH, CQCX_SZ path is not correct")


if not TDX_DIR:
    raise Exception("Please Setting TDX data dir")

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


def zipline_splits_and_dividends(symbol_map):
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
    return splits, dividends

def int_to_date(d):
    d = str(d)
    return datetime.date(int(d[:4]), int(d[4:6]), int(d[6:]))


def squant_bundle(environ,
                   asset_db_writer,
                   minute_bar_writer,
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start_session,
                   end_session,
                   cache,
                   show_progress,
                   output_dir):

    tdx_reader = TdxReader(TDX_DIR)

    symbol_df = get_symbol_list()
    # 只保留未停牌的
    symbol_df = symbol_df[symbol_df['status'] == False]

    # 由于meta,split,dividend 和 行情数据源不同,所以有可能会不同,所以我们这里统一根据

    symbol_map = symbol_df.simplesymbol

    # 更新日期信息
    def update_start_and_end_date(s):
        start_date = start_session.replace(tzinfo=None)
        end_date = end_session.replace(tzinfo=None)
        if s.start_date < start_date:
            s.start_date = start_date
        if s.end_date == pd.Timestamp('1900-01-01') or s.end_date is pd.NaT:
            s.end_date = end_date
        return s
    symbol_df = symbol_df.apply(func=update_start_and_end_date, axis=1)


    # 写入基础信息
    asset_db_writer.write(symbol_df)
    # 写入数据文件
    daily_bar_writer.write(get_hist_data(symbol_df, symbol_map, tdx_reader, start_session, end_session, calendar),
                           show_progress=show_progress)
    # split and diviends
    splits, dividends = zipline_splits_and_dividends(symbol_map)

    # hack for tdx data , for tdx source for shenzhen market, we can not get data before 1991-12-23
    splits_df = pd.concat(splits, ignore_index=True)
    dividends_df = pd.concat(dividends, ignore_index=True)

    splits_df= splits_df.loc[splits_df['effective_date'] > start_session]
    dividends_df = dividends_df.loc[dividends_df['ex_date'] > start_session]
    adjustment_writer.write(
        splits=splits_df,
        dividends=dividends_df,
    )

def get_hist_data(symbol_df, symbol_map, tdx_reader, start_session, end_session, calendar):
    for sid, index in symbol_map.iteritems():
        exchagne = ''
        if symbol_df.loc[sid]['exchange'] == 'SZSE':
            exchagne = 'sz'
        elif symbol_df.loc[sid]['exchange'] == 'SSE':
            exchagne = 'sh'

        try:
            history = tdx_reader.get_df(index, exchagne)

            #print('max-min for %s is %s : %s', (index, history.index[0], history.index[-1]))
            # 去除没有报价信息的内容
            if history.index[0] > pd.Timestamp((end_session.date())):
                continue
        except TdxFileNotFoundException as e:
            #print('symbol %s file no found, ignore it ' % index)
            continue
        # history.to_pickle('/tmp/debug.pickle')

        #reindex
        sessions = calendar.sessions_in_range(start_session, end_session)

        history = history.reindex(
            sessions.tz_localize(None),
            copy=False,
        ).fillna(0.0)

        yield sid, history.sort_index()
    pass

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
