"""
Get all stock name
"""

import requests
from io import StringIO
import pandas as pd
import os
import shutil
import sys

ALL_STOCKS_URL = 'http://218.244.146.57/static/all.csv'


def get_cache_dir():
    home_path = os.path.expanduser('~')
    cache_dir = os.path.join(home_path, '.zipline_cn_databundle')

    if not os.path.isdir(cache_dir):
        os.makedirs(cache_dir)
    return cache_dir

def get_cache_path():
    cache_dir = get_cache_dir()
    return os.path.join(cache_dir, 'all_stocks.csv')

def get_all_stocks(cache=True):
    """
    used idea from tushare
    :return: data frame of stock list
    """
    cache_path = get_cache_path()

    #use cache
    if cache and os.path.isfile(cache_path):
        df = pd.read_csv(cache_path,  dtype={'code': 'object'})
        df = df.set_index('code')
        return df

    response = requests.get(ALL_STOCKS_URL)
    text = response.content
    text = text.decode('GBK')
    text = text.replace('--', '')
    df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
    df = df.set_index('code')
    # write to cache
    with open(cache_path, 'wb') as f:
        f.write(text.encode('utf-8'))

    return df



if __name__ == '__main__':
    print(get_all_stocks(True))