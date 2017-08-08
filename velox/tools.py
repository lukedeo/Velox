#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
## `velox.tools`

The `velox.tools` submodule provides general support utilities to the Velox 
package.
"""

from __future__ import unicode_literals

import datetime
from hashlib import md5
import six
from threading import Thread

from concurrent.futures import Future

from builtins import bytes


def sha(s):
    """
    get a simple, potentially value-inconsistent SHA of a python object.
    """
    m = md5()
    if isinstance(s, six.string_types):
        m.update(bytes(s, 'utf-8'))
    else:
        m.update(s.__repr__())
    return m.hexdigest()


def timestamp():
    """ 
    Returns a string of the form YYYYMMDDHHmmSSCCCCCC, where 

    * `YYYY` is the current year
    * `MM` is the current month, with zero padding to the left
    * `DD` is the current day, zero padded on the left
    * `HH`is the current hour, zero padded on the left
    * `mm`is the current minute, zero padded on the left
    * `CCCCCC`is the current microsecond as a decimal number, 
        zero padded on the left
    """
    return datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')


class abstractclassmethod(classmethod):

    # this hack allows us to enforce the ABC implementation of a classmethod
    __isabstractmethod__ = True

    def __init__(self, callable):
        callable.__isabstractmethod__ = True
        super(abstractclassmethod, self).__init__(callable)


def call_with_future(fn, future, args, kwargs):
    try:
        result = fn(*args, **kwargs)
        future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)


def threaded(fn):
    """
    A simple decorator that allows a function to be called with its return 
    value given as a `Future` object.

    Example:
    --------

        #!python
        @threaded
        def fn(x):
            return x ** 2

        a = fn(6)
        assert a.result() == 36
    """

    def wrapper(*args, **kwargs):
        future = Future()
        Thread(target=call_with_future,
               args=(fn, future, args, kwargs)).start()
        return future
    return wrapper

__all__ = ['threaded', 'timestamp']
