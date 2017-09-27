#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
file: setup.py
description: setuptools for Velox
author: Luke de Oliveira (lukedeo@manifold.ai)
"""

import os
from setuptools import setup
from setuptools import find_packages


setup(
    name='Velox',
    version='0.1.1',
    description=('Batteries-included tooling for handling promotion, '
                 'versioning, and zero-downtime requirments of Machine '
                 'Learning models in production.'),
    author='Luke de Oliveira',
    author_email='lukedeo@ldo.io',
    url='https://github.com/lukedeo/Velox',
    download_url='https://github.com/lukedeo/Velox/archive/0.1.1.tar.gz',
    license='Apache 2.0',
    install_requires=['apscheduler', 'boto3', 'semantic_version',
                      'futures', 'future', 'six'],
    packages=find_packages(),
    keywords=('Machine-Learning TensorFlow Deployment Versioning Keras '
              'AWS Deep Learning'),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7'
    ],
    extras_require={
        'aws': ['boto3'],
        'tests': ['numpy', 'pytest', 'pytest-cov', 'pytest-pep8',
                  'pytest-xdist', 'python-coveralls', 'moto', 'keras[h5py]',
                  'backports.tempfile', 'scikit-learn', 'mock', 'tensorflow'],
        'docs': ['bs4', 'strif', 'pdoc']
    }
)
