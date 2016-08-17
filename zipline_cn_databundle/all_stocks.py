"""
Get all stock name
"""

import requests
from io import StringIO
import pandas as pd

ALL_STOCKS_URL = 'http://218.244.146.57/static/all.csv'


def get_all_stocks():
    """
    used idea from tushare
    :return: data frame of stock list
    """
    response = requests.get(ALL_STOCKS_URL)
    text = response.content
    text = text.decode('GBK')
    text = text.replace('--', '')
    df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
    df = df.set_index('code')
    return df

if __name__ == '__main__':
    print(get_all_stocks())