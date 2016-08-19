from setuptools import setup, find_packages

setup(
    name = "zipline-cn-databundle",
    version = "0.1",
    packages = find_packages(),
    install_requires=[
        'pandas-datareader',
    ],
    entry_points={
        'console_scripts': [
            'zipline-cn-databundle-update=zipline_cn_databundle:zipline_cn_databundle_update',
        ]
    }
)

