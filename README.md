Equtity Data bundle of chinese exchange market for Zipline
=======


Installation
-----

```
pip install zipline-cn-databundle
```
or

```
python setup.py install
```

Usage
----

### Register in zipline

modify zipline config located in ~/.zipline/extension.py

and add

```
from zipline_cn_databundle import register_cn_bundle_from_yahoo

register_cn_bundle_from_yahoo('cn_exchange_yahoo')
```

the first time you used databundle, we will download and check availablity from yahoo.
we and the second time you use this api ,we will use cached symbol list directly,
if you prefer update the data list or do not use the cached version, you could use:

```
 register_cn_bundle_from_yahoo('cn_exchange_yahoo', cache=False)
```
instead

and if you wanna fetch and cache the info before use it, you can run command in console:

`zipline-cn-databundle-update`

```
> zipline-cn-databundle-update
Start to fetch data and update cache
Get All Stock List.....
Check availablity from Yahoo...
300533.SZ ok!
...
....
```

### Others

#### Get All Stocks info in Chinese Market

```

get_all_stocks()

```

returns infomations in pandas format

```
         name industry area       pe  outstanding      totals  totalAssets  \
code
601997    N贵银       银行   贵州     9.11     50000.00   229859.19  28492688.00
000972   中基健康       食品   新疆     0.00     77128.35    77128.35    229892.13
601011    宝泰隆     焦炭加工  黑龙江  1240.98    130874.56   136750.00    794420.81
....
```