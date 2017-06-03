#!/usr/bin/env python
# -*- coding: utf-8 -*-

u"""

<img src="img/velox-logo.png" width=37% align="right" />

# Welcome to Velox!

Deploying and managing live machine learning models is difficult. It involves a mix of handling model versioning, hot-swapping new versions and determining version constraint satisfaction on-the-fly, and managing binary file movement either on a networked or local file system or with a cloud storage system like S3. Velox can handle this for you with a simple base class enforcing opinionated methods of handling the above problems.

Velox provides two main utilities:

* Velox abstracts the messiness of consistent naming schemes and handling saving and loading requirements for a filesystem and for other forms of storage.
* Velox allows the ability to do a model / blob hotswap in-place for a new binary version.

## Requirements
---

Velox currently only supports Python 2.7, but **we would love contributions towards Python 3 support** 😁

The main requirements are `apscheduler` for scheduling hot-swaps, `semantic_version` for version sanity, and the `futures` Python 2.7 backport. If you want to be able to work with S3, you'll need `boto3` (and a valid and properly set up AWS account).

To run the tests, you'll need the brilliant `moto` library, the `backports.tempfile` library for Python 2.7 compatibility, and `Keras` and `sckit-learn`.

For logging, simply grab the Velox logger by the `velox` handle.

## `VeloxObject` Abstract Base Class
---

Functionality is exposed using the `VeloxObject` abstract base class (ABC). A subclass of a `velox.obj.VeloxObject` needs to implement three things in order for the library to know how to manage it.

* Your class must be defined with a `velox.obj.register_model` decorator around it.
* Your class must implement a `_save` object method that takes as input a file object and does whatever is needed to save the object.
* Your class must implement a `_load` class method (with the `@classmethod` decorator) that takes as input a file object and reconstructs and returns an instance of your class.

This allows you to abstract away much of the messiness in bookkeeping.

Here is an example using [`gensim`](https://github.com/RaRe-Technologies/gensim) to build a topic model and keep track of all the necessary ETL-type objects that follow:

<!--begin_code-->
    #!python
    @register_model(
        registered_name='foobar',
        version='0.1.0-alpha',
        version_constraints='>=0.1.0,<0.2.0'
    )
    class ChurnModel(VeloxObject):
        def __init__(self, submodel):
            super(VeloxObject, self).__init__()
            self._submodel = submodel

        def _save(self, fileobject):
            pickle.dump(self, fileobject)

        @classmethod
        def _load(cls, fileobject):
            return pickle.load(fileobject)

        def predict(self, X):
            return self._submodel.predict(X)
<!--end_code-->

"""
import logging

# supress "No handlers could be found for logger XXXXX" error
logging.getLogger('velox').addHandler(logging.NullHandler())

__version__ = '0.1.0'

from .obj import VeloxObject, register_model

import filesystem
import exceptions
import tools
import obj
import wrapper

__all__ = ['filesystem', 'exceptions', 'tools', 'obj', 'wrapper']
