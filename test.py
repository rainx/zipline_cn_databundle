from zipline_cn_databundle import register_cn_bundle_from_yahoo, get_all_stocks, get_all_yahoo_stock_names, check_code
from pprint import  pprint



if __name__ == '__main__':
    symbols = get_all_yahoo_stock_names()
    filtered_symbols = list(filter(check_code, symbols))
    print('output to symbols.txt')
    with open('symbols.txt', 'w') as f:
        f.write("\n".join(filtered_symbols))
    print('done!')



