from setuptools import setup, find_packages

setup(
    name = "zipline-cn-databundle",
    version = "0.1",
    packages = find_packages(),
    requires=[
        'Yahoo-ticker-downloader'
    ]
)

