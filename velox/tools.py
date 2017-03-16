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

import boto3
from concurrent.futures import Future

SESSION = boto3.Session()

logger = logging.getLogger(__name__)


def safe_mkdir(path):
    '''
    Safe mkdir (i.e., don't create if already exists, 
    and no violation of race conditions)
    '''
    try:
        logger.debug('safely making (or skipping) directory: {}'.format(path))
        makedirs(path)
    except OSError as exception:
        if exception.errno != EEXIST:
            raise exception


def is_s3_path(pth):
    return pth.startswith('s3://')


def parse_s3(pth):
    if not pth.startswith('s3://'):
        raise ValueError('{} is not a valid s3 path.'.format(pth))
    pth = pth.replace('s3://', '')
    split = pth.split(os.sep)
    return split[0], os.sep.join(split[1:])


@contextmanager
def get_aware_filepath(path, mode='wb', session=None):
    """ context handler for dealing with local fs and remote (s3 only...)

    Args:
    -----
        path (str): path to object you wish to write. Can be 
            either /path/to/desired/file.fmt, or s3://myBucketName/this/is/a.key

        mode: one of {rb, wb}

        session (None, boto3.Session): can pass in a custom boto3 session 
        if need be

    Example:
    --------

        with get_aware_filepath('s3://bucket/file.txt', 'wb') as f:
            f.write('foobar')

        with get_aware_filepath('s3://bucket/file.txt', 'rb') as f:
            print f.read() 

        # foobar
    """

    if mode not in {'rb', 'wb'}:
        raise ValueError('mode must be one of {rb, wb}')

    if not is_s3_path(path):
        logger.debug('opening file = {} on local fs'.format(path))

        with open(path, mode) as f:
            yield f

        logger.debug('successfully closed session with file = {}'.format(path))

    else:

        if session is None:
            session = SESSION

        S3 = session.resource('s3')

        fd, temp_fp = tempfile.mkstemp(
            suffix='.tmpfile', prefix='s3_tmp', text=False)

        bucket, key = parse_s3(path)

        logger.debug('detected bucket = {}, key = {}, mode = {}'.format(
            bucket, key, mode))

        if mode == 'rb':
            logger.debug('initiating download to tempfile')
            S3.Bucket(bucket).download_file(key, temp_fp)
            logger.debug('download to tempfile successful')

        with open(temp_fp, mode) as f:
            logger.debug('yielding {} with mode {}'.format(temp_fp, mode))
            yield f
            logger.debug('closing {}'.format(temp_fp))

        if mode == 'wb':
            logger.debug('uploading {} to bucket = {} with key = {}'.format(
                temp_fp, bucket, key))
            S3.Bucket(bucket).upload_file(temp_fp, key)

        logger.debug('removing temporary allocations')
        os.close(fd)
        os.remove(temp_fp)

        logger.debug('cleaned up, releasing')


def sha(s):
    """
    get a potentially value-inconsistent SHA of a python object.
    """
    m = md5()
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
        Thread(target=call_with_future, args=(fn, future, args, kwargs)).start()
        return future
    return wrapper


def zero_reload_downtime(fn):
    from functools import wraps

    @wraps(fn)
    def _respect_reload(cls, *args, **kw):
        if not cls._increment_underway:
            return fn(cls, *args, **kw)
        elif cls._needs_increment:
            logger.info('model version increment needed')
            cls._increment()
        logger.info('model version increment still under preparation')
        return fn(cls, *args, **kw)

    _respect_reload.__doc__ = fn.__doc__
    return _respect_reload
