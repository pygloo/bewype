#!/usr/bin/env python
# (C) Copyright 2010 Bewype <http://www.bewype.org>

# setuptools import
from setuptools import setup, find_packages

VERSION = '0.1'

setup(
    name='bewype',
    version=VERSION,
    description='Bewype tools',
    author='florent.pigout@gmail.com',
    author_email='florent.pigout@gmail.com',
    url='',
    download_url='',
    license='MIT',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages(),
    install_requires=[
        'ConfigObject',
        'MySQL-Python',
        'pysqlite'
    ],
    namespace_packages=[
        'bewype',
        ],
    package_data={'bewype.config.static': ['*/*.ini']},
    scripts=[],
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    entry_points="""
    """,
    keywords = [],
    test_suite = 'nose.collector',
)

