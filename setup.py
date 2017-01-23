from setuptools import setup, find_packages

setup(
    name = "zipline-cn-databundle",
    version = "0.5",
    author= 'RainX',
    description='ingest zipline databundle source for chinese market',
    packages = find_packages(),
    install_requires=[
        'pandas-datareader',
    ],
    entry_points={
        'console_scripts': [
            'zipline-cn-databundle-update=zipline_cn_databundle:zipline_cn_databundle_update',
            'gen-all-index-benchmark-data=zipline_cn_databundle.index_list.__init__:gen_data',
        ]
    }
)

