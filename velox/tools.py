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
import importlib
from concurrent.futures import Future

from builtins import bytes

VELOX_NEW_FILE_SIGNATURE = '||vx||'
VELOX_NEW_FILE_META_LENGTH = 100
VELOX_NEW_FILE_PAD_CHAR = '%'

VELOX_NEW_FILE_SIGNATURE_LENGTH = len(VELOX_NEW_FILE_SIGNATURE)
VELOX_NEW_FILE_FORMAT_STRING = (
    '{:' + VELOX_NEW_FILE_PAD_CHAR + '>' + str(VELOX_NEW_FILE_META_LENGTH) +
    '}' + VELOX_NEW_FILE_SIGNATURE
)
VELOX_NEW_FILE_EXTRAS_LENGTH = len(VELOX_NEW_FILE_FORMAT_STRING.format(''))


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


def fullname(o):
    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__
    return module + '.' + o.__class__.__name__


# def import_from_qualified_name(name):
#     components = name.split('.')
#     mod = __import__(components[0])
#     for comp in components[1:]:
#         mod = getattr(mod, comp)
#     return mod


def import_from_qualified_name(name):
    components = name.split('.')
    module, classname = components[:-1], components[-1]
    module = '.'.join(module)
    return getattr(importlib.import_module(module), classname)


def obtain_padding_bytes(obj, asbytes=True):
    qualname = fullname(obj)
    if len(qualname) > VELOX_NEW_FILE_META_LENGTH:
        raise ValueError('Qualified name {} is too long for use in Velox.'
                         .format(qualname))

    pad_bytes = VELOX_NEW_FILE_FORMAT_STRING.format(fullname(obj))

    if asbytes:
        return pad_bytes.encode()
    return pad_bytes


def obtain_qualified_name(meta_string):
    # No need to return the file signature
    if isinstance(meta_string, bytes):
        meta_string = meta_string.decode()
    class_info_string = meta_string[:-VELOX_NEW_FILE_SIGNATURE_LENGTH]
    return class_info_string.replace(VELOX_NEW_FILE_PAD_CHAR, '')


def get_file_meta(filehandle, truncate=False):
    """Get the metadata string from a velox file that has a type hint."""
    # Go to the end of the file
    filehandle.seek(0, 2)

    # We'll use the size to backtrack a few chars
    size = filehandle.tell()
    if size <= VELOX_NEW_FILE_SIGNATURE_LENGTH:
        return None
    filehandle.seek(size - VELOX_NEW_FILE_SIGNATURE_LENGTH)
    file_signature = filehandle.read()

    if isinstance(file_signature, bytes):
        file_signature = file_signature.decode()

    # If we're dealing with the old file format, we cannot find the qualname
    if file_signature != VELOX_NEW_FILE_SIGNATURE:
        return None

    # Go to the beginning of the file meta section
    filehandle.seek(size - VELOX_NEW_FILE_EXTRAS_LENGTH)

    meta_string = filehandle.read()

    if truncate:
        filehandle.seek(size - VELOX_NEW_FILE_EXTRAS_LENGTH)
        filehandle.truncate()

    filehandle.seek(0)

    return meta_string


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
