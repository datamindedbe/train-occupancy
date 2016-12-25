#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='train-occupancy',
    version='0.1',
    description='Predict train occupancy and train delays',
    long_description='Predict train occupancy and train delays',
    author='Kris Peeters',
    author_email="kris@dataminded.be",
    license='',
    url='https://github.com/datamindedbe/train-occupancy',
    package_data={},
    include_package_data=True,
    scripts= [],
    packages=find_packages(),
    install_requires=[
        "pip>=9.0",
        "numpy",
        "pandas",
        "scipy",
        "sklearn",
        "sqlalchemy",
        "psycopg2",
        "requests"
    ],
    extras_require={
    },
    tests_require=[
        "nose",
        "mock>=1.3.0",
        "testing.common.database",
        "testing.postgresql",
    ],
    zip_safe=False
)
