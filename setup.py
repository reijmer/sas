import os

from setuptools import find_packages, setup

__version__ = 0.1

setup(
    name="sas",
    version=__version__,
    install_requires=[
        'click',
        'pandas',
        'numpy',
        'termtables',
        'dataframe_sql'
    ],
    author = 'Bart Reijmer',
    author_email = 'bart.reijmer@roche.com',
    packages=find_packages(),
    entry_points={"console_scripts": ["sas=sas.reader:main"]},
)
