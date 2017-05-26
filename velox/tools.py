#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
file: tools.py
description: useful tools for velox
author: Luke de Oliveira (lukedeo@vaitech.io)
"""

from contextlib import contextmanager
import datetime
from errno import EEXIST
from hashlib import md5
import logging
from os import makedirs
import os
import tempfile
from threading import Thread

from concurrent.futures import Future

logger = logging.getLogger(__name__)


def sha(s):
    """
    get a simple, potentially value-inconsistent SHA of a python object.
    """
    m = md5()
    if isinstance(s, basestring):
        m.update(s)
    else:
        m.update(s.__repr__())
    return m.hexdigest()


def timestamp():
    """ 
    Returns a string of the form YYYYMMDDHHMMSS, where 
    HH is in 24hr time for easy sorting
    """
    return datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")


class abstractstatic(staticmethod):

    # we want to enforce an implementation of a static method using the ABC
    # pattern. This hack allows you to enforce registration of a non-abstract
    # version of a function

    __slots__ = ()

    def __init__(self, function):
        super(abstractstatic, self).__init__(function)
        function.__isabstractmethod__ = True
    __isabstractmethod__ = True


class abstractclassmethod(classmethod):

    # this hack allows us to enforce the ABC implementation of a classmethod
    __isabstractmethod__ = True

    def __init__(self, callable):
        callable.__isabstractmethod__ = True
        super(abstractclassmethod, self).__init__(callable)


def enforce_return_type(fn):
    """ Wrapper to ensure that a classmethod has a consistent return type with 
    the cls argument.

    Args:
    -----
        fn: classmethod to wrap

    Returns:
    --------
        the output of fn(cls, *args, **kwargs)

    Raises:
    -------
        asserts that isinstance(fn(cls, *args, **kwargs), cls) is true.
    """

    from functools import wraps

    @wraps(fn)
    def _typesafe_ret_type(cls, *args, **kw):
        o = fn(cls, *args, **kw)
        if not isinstance(o, cls):
            raise TypeError("Return type doesn't match specified "
                            "{}, found {} instead".format(cls, type(o)))
        return o

    _typesafe_ret_type.__doc__ = fn.__doc__
    return _typesafe_ret_type


def is_enforced_func(f):
    """
    Boolean test for whether or not a class method has been wrapped 
    with @enforce_return_type
    """
    return f.__code__.co_name == '_typesafe_ret_type'


def call_with_future(fn, future, args, kwargs):
    try:
        result = fn(*args, **kwargs)
        future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)


def threaded(fn):
    def wrapper(*args, **kwargs):
        future = Future()
        Thread(target=call_with_future,
               args=(fn, future, args, kwargs)).start()
        return future
    return wrapper


def zero_reload_downtime(fn):
    from functools import wraps

    @wraps(fn)
    def _respect_reload(cls, *args, **kw):
        if cls._needs_increment:
            logger.info('model version increment needed')
            cls._increment()
        else:
            return fn(cls, *args, **kw)

    _respect_reload.__doc__ = fn.__doc__
    return _respect_reload
