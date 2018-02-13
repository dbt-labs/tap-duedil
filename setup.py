#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-duedil",
    version="0.1.0",
    description="Singer.io tap for extracting data from the Yotpo API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_duedil"],
    install_requires=[
        "singer-python==5.0.4",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-duedil=tap_duedil:main
    """,
    packages=["tap_duedil"],
    package_data = {
        "schemas": ["tap_duedil/schemas/*.json"]
    },
    include_package_data=True,
)
