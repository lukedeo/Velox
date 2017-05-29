#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
file: filesystem.py
description: useful tools for dealing with the filesystem for velox
author: Luke de Oliveira (lukedeo@manifold.ai)
"""

from contextlib import contextmanager
import datetime
from errno import EEXIST
import fnmatch
from glob import glob
from hashlib import md5
import logging
import os
from tempfile import mkstemp
from threading import Thread

from concurrent.futures import Future


logger = logging.getLogger(__name__)


def find_matching_files(prefix, specifier):
    if not is_s3_path(prefix):
        logger.debug('Searching on filesystem')
        filelist = sorted(glob(os.path.join(prefix, specifier)))
    else:
        import boto3
        S3 = boto3.Session().resource('s3')

        bucket, key = parse_s3(prefix)

        logger.debug('searching in bucket s3://{} with '
                     'pfx key = {}'.format(bucket, key))

        filelist = sorted([
            obj.key for obj in S3.Bucket(bucket).objects.filter(Prefix=key)
            if fnmatch.fnmatch(os.path.split(obj.key)[-1], specifier)
        ])
    return filelist[::-1]


def stitch_filename(prefix, filename):
    if is_s3_path(prefix):
        if prefix.endswith('/'):
            prefix = prefix[:-1]

        if not filename.startswith('/'):
            filename = '/' + filename

        return prefix + filename

    return os.path.join(prefix, filename)


def ensure_exists(prefix):

    if not is_s3_path(prefix):
        logger.debug('Safely ensuring {} exists.'.format(prefix))
        safe_mkdir(prefix)
    else:
        import boto3

        logger.info('Prefix {} will be on S3'.format(prefix))

        bucket, key = parse_s3(prefix)

        logger.info('S3 bucket = {}'.format(bucket))
        logger.info('S3 key = {}'.format(key))

        SESSION = boto3.Session()

        S3 = SESSION.resource('s3')

        if bucket in {_.name for _ in S3.buckets.iterator()}:
            logger.debug('bucket already exists')
        else:
            logger.warn('bucket does not exist. Creating it...')
            S3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={
                    'LocationConstraint': SESSION.region_name
                }
            )


def safe_mkdir(path):
    '''
    Safe mkdir (i.e., don't create if already exists, 
    and no violation of race conditions)
    '''
    try:
        logger.debug('safely making (or skipping) directory: {}'.format(path))
        os.makedirs(path)
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
    # this gets back bucket, key
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
            import boto3
            session = boto3.Session()

        S3 = session.resource('s3')

        fd, temp_fp = mkstemp(suffix='.tmpfile', prefix='s3_tmp', text=False)

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
