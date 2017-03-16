#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
file: setup.py
description: setuptools for Velox
author: Luke de Oliveira (lukedeo@ldo.io)
"""

import os
from setuptools import setup
from setuptools import find_packages


setup(
    name='Velox',
    version='0.1.0',
    description=('Batteries-included tooling for handling promotion, '
                 'versioning, and zero-downtime requirments of Machine '
                 'Learning models'),
    author='Luke de Oliveira',
    author_email='lukedeo@ldo.io',
    url='https://github.com/lukedeo/Velox',
    license='Apache 2.0',
    install_requires=['apscheduler', 'boto3', 'semantic_version', 'futures'],
    packages=find_packages(),
    keywords=' '.join(('Machine Learning', 'TensorFlow',
                       'Deployment', 'Versioning', 'Keras', 'AWS')),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7'
    ]
)
