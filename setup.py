#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
file: setup.py
description: setuptools for Velox
author: Luke de Oliveira (lukedeo@vaitech.io)
"""

import os
from setuptools import setup
from setuptools import find_packages


from setuptools import setup, find_packages

VERSION = '0.4.0'

setup(
    name='Velox',
    version=VERSION,
    description=('Batteries-included tooling for handling promotion, '
                 'versioning, and zero-downtime requirments of Machine '
                 'Learning models in production.'),
    author='Luke de Oliveira',
    author_email='lukedeo@vaitech.io',
    url='https://github.com/vaitech/Velox',
    download_url='https://github.com/vaitech/Velox/archive/{}.tar.gz'.format(
        VERSION),
    license='Apache 2.0',
    install_requires=['apscheduler', 'semantic_version', 'dill',
                      'six', 'futures;python_version=="2.7"',
                      'future;python_version=="2.7"'],
    packages=find_packages(),
    keywords=('Machine-Learning TensorFlow Deployment Versioning Keras '
              'AWS Deep Learning'),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6'
    ],
    extras_require={
        'aws': ['boto3'],
        'tests': ['numpy', 'pytest', 'pytest-cov', 'pytest-pep8',
                  'pytest-xdist', 'python-coveralls', 'moto', 'keras[h5py]',
                  'backports.tempfile', 'scikit-learn', 'mock',
                  'tensorflow<1.5'],
        'docs': ['bs4', 'strif', 'pdoc']
    }
)
