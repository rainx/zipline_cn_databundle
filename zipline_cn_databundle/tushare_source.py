import click
import tushare as ts
import pandas as pd
import os
from .squant_source import load_splits_and_dividends, zipline_splits_and_dividends

"""
从tushare获取股票信息

其中....

需要读取通联数据, 请设置 环境变量

ZIPLINE_TL_TOKEN

保存通联数据的token

"""

"""
ZIPLINE_TL_TOKEN = os.environ.get('ZIPLINE_TL_TOKEN')

if not ZIPLINE_TL_TOKEN:
    raise Exception("no datayes token in envirionment ZIPLINE_TL_TOKEN,  we need this token to fetch ajustments data")

ts.set_token('ZIPLINE_TL_TOKEN')
"""

def tushare_bundle(environ,
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

    metadata, histories, symbol_map = get_basic_info()
    # 写入股票基础信息
    asset_db_writer.write(metadata)
    # 准备写入dailybar
    daily_bar_writer.write(get_hist_data(symbol_map, histories, start_session, end_session), show_progress=show_progress)
    # 送股,分红数据, 从squant 获取
    splits, dividends = zipline_splits_and_dividends(symbol_map)
    adjustment_writer.write(
        splits=pd.concat(splits, ignore_index=True),
        dividends=pd.concat(dividends, ignore_index=True),
    )

def get_basic_info(show_progress=True):
    # 先获取列表
    if show_progress:
        click.echo("获取股票基础信息")
    ts_symbols = ts.get_stock_basics()
    if show_progress:
        click.echo("写入股票列表")

    symbols = []

    histories = {}

    # 获取股票数据
    i = 0
    total = len(ts_symbols)
    for index, row in ts_symbols.iterrows():
        i = i +1
        if i > 10:
            break

        srow = {}
        # 获取历史报价信息
        click.echo("正在获取代码%s(%s)的历史行情信息 (%d/%d)" % (index, row['name'], i, total))
        histories[index] = ts.get_hist_data(index)
        srow['start_date'] = histories[index].index[-1]
        srow['end_date'] = histories[index].index[0]
        srow['symbol'] = index
        srow['asset_name'] = row['name']
        symbols.append(srow)

    df_symbols = pd.DataFrame(data=symbols).sort_values('symbol')
    symbol_map = pd.DataFrame.copy(df_symbols.symbol)

    # fix the symbol exchange info
    df = df_symbols.apply(func=convert_symbol_series, axis=1)


    return df, histories, symbol_map


def symbol_to_exchange(symbol):
    isymbol = int(symbol)
    if (isymbol>=600000):
        return symbol + ".SS", "SSE"
    else:
        return symbol + ".SZ", "SZSE"

def convert_symbol_series(s):
    symbol, e = symbol_to_exchange(s['symbol'])
    s['symbol'] = symbol
    s['exchange'] = e
    return s

def get_hist_data(symbol_map, histories, start_session, end_session):
    for sid, index in symbol_map.iteritems():
        history = histories[index]

        """
        writer needs format with
        [index], open, close, high, low, volume

        so we do not need to change the format from tushare

        but we need resort it
        """
        yield sid, history.sort_index()

if __name__ == '__main__':
    df_symbols, histories, symbol_map = get_basic_info()
    print(df_symbols)

    """
    for h,df in histories.items():
        print(df)
    """
