#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
## `velox.filesystem`

The `velox.filesystem` submodule provides support utilities related to 
managing files (and by extension, S3) to the Velox ecosystem.
"""

from contextlib import contextmanager
import errno
import fnmatch
from glob import glob
import logging
import os
import shutil
from tempfile import mkstemp, mkdtemp

from .tools import get_file_meta, obtain_qualified_name

logger = logging.getLogger(__name__)


def _is_non_zero_file(filepath):
    """Check for a file of zero size, with some safety w/ race conditions."""
    try:
        return os.path.getsize(filepath) > 0
    except OSError:
        return False


def find_matching_files(prefix, specifier):
    """
    Searches for files matching the `specifier` at the `prefix` location 
    (which can be on S3).
    """
    if not is_s3_path(prefix):
        logger.debug('Searching on filesystem')
        filelist = sorted(glob(os.path.join(
            os.path.abspath(prefix),
            specifier
        )))

        # make sure we don't have any files that are zero sized
        filelist = [fp for fp in filelist if _is_non_zero_file(fp)]
    else:
        import boto3
        S3 = boto3.Session().resource('s3')

        bucket, key = parse_s3(prefix)

        logger.debug('searching in bucket s3://{} with '
                     'pfx key = {}'.format(bucket, key))

        objs_iterator = S3.Bucket(bucket).objects.filter(Prefix=key)

        filelist = sorted([
            os.path.basename(obj.key) for obj in objs_iterator
            if (fnmatch.fnmatch(os.path.split(obj.key)[-1], specifier) and
                obj.size > 0)  # make sure we don't have zero byte files
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
    """
    Safely ensures that the specified `prefix` exists. If `prefix` would point
    to a location on a reachable file system, it will safely create the 
    necessary directory path respecting race conditions. If `prefix` would 
    point to S3, it creates the bucket, if one doesn't already exist.
    """

    if not is_s3_path(prefix):
        logger.debug('Safely ensuring {} exists.'.format(prefix))
        safe_mkdir(prefix)
    else:
        import boto3

        logger.info('Prefix {} will be on S3'.format(prefix))

        bucket, key = parse_s3(prefix)

        logger.debug('S3 bucket = {}'.format(bucket))
        logger.debug('S3 key = {}'.format(key))

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
    """
    Safe mkdir (i.e., don't create if already exists, 
    and no violation of race conditions).
    """
    try:
        logger.debug('safely making (or skipping) directory: {}'.format(path))
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise exception


def is_s3_path(pth):
    """Checks if a passed path is an S3 path."""
    return pth.startswith('s3://')


def parse_s3(pth):
    """
    Args:
    -----
    * `pth (str)`: an S3 path, written in the form `s3://bucket/foo.bar`

    Returns:
    --------

    `Tuple[str]`: A `(bucket, key)` pair for the file.

    Raises:
    -------
    * `ValueError` if the passed path is not a valid s3 path
    """
    if not pth.startswith('s3://'):
        raise ValueError('{} is not a valid s3 path.'.format(pth))
    pth = pth.replace('s3://', '')
    split = pth.split(os.sep)
    # this gets back bucket, key
    return split[0], os.sep.join(split[1:])


# TODO(@lukedeo): Ensure that if things go wrong, we clean up all velox
# metadata
@contextmanager
def get_aware_filepath(path, mode='r', session=None, yield_type_hint=False,
                       delete_on_close=True):
    """ context handler for dealing with local fs and remote (S3 only...)

    Args:
    -----
    * `path (str)`: path to object you wish to write. Can be 
            either `/path/to/desired/file.fmt`, or 
            `s3://myBucketName/this/is/a.key`

    * `mode (str)`: one of {rb, wb, r, w}

    * `session (None | boto3.Session)`: can pass in a custom boto3 session 
        if need be

    * `yield_type_hint (bool)`: Whether or not to yield any type hints from the 
        velox metadata. If `True`, then will yield a tuple.

    * `delete_on_close (bool)`: Whether or not to delete any temporary files 
        (only used if `path` is an S3 path.)

    Example:
    --------

        #!python
        with get_aware_filepath('s3://bucket/file.txt', 'wb') as f:
            f.write('foobar')

        with get_aware_filepath('s3://bucket/file.txt', 'rb') as f:
            assert f.read() == 'foobar'

    Raises:
    -------

    * `ValueError` if `mode` is not one of the allowed modes.


    """

    if mode not in {'rb', 'wb', 'r', 'w'}:
        raise ValueError('mode must be one of {rb, wb, r, w}')

    binary = 'b' if 'b' in mode else ''
    read_operation = 'r' in mode

    if not is_s3_path(path):
        logger.debug('opening file = {} on local fs'.format(path))

        metadata = None
        # If we are reading from a file, we want to extract and truncate the
        # bytes related to the velox type hint before yielding the file so that
        # whatever operation the user specified is not compromised by extra
        # bytes. We then want to add the tail bytes back on to the file after
        # finishing
        # if yield_type_hint:
        if read_operation:
            logger.debug('peforming safe copy to avoid race conditions.')
            tmpdir = mkdtemp(suffix='tmpdir', prefix='typehint')
            _, filename = os.path.split(path)
            tmp_path = os.path.join(tmpdir, filename)
            shutil.copyfile(src=path, dst=tmp_path)
            path = tmp_path
            logger.debug('will now operate on newly allocated file: {}'
                         .format(path))
            with open(path, 'r{}+'.format(binary)) as pre_opened_file:
                # extract out the file ending with the type hint, and
                metadata = get_file_meta(pre_opened_file, truncate=True)
                if metadata is not None:
                    logger.debug('found velox metadata in file signature')

        with open(path, mode) as f:
            if not yield_type_hint:
                yield f
            else:
                clsname = None
                if metadata is not None:
                    clsname = obtain_qualified_name(metadata)
                    logger.debug(
                        'found type hint: {} - yielding as part pf payload'
                        .format(clsname)
                    )

                yield (f, clsname)

        if yield_type_hint:
            logger.debug('removing temporary allocations under {}'
                         .format(tmpdir))
            shutil.rmtree(tmpdir)
            logger.debug('cleaned up, releasing')
        logger.debug('successfully closed session with file = {}'.format(path))
    else:
        if session is None:
            import boto3
            session = boto3.Session()

        S3 = session.resource('s3')

        # fd, temp_fp = mkstemp(suffix='.tmpfile', prefix='s3_tmp', text=False)
        temp_dir = mkdtemp(suffix='tmpfile', prefix='s3_tmp')

        bucket, key = parse_s3(path)

        logger.debug('detected bucket = {}, key = {}, mode = {}'.format(
            bucket, key, mode))

        _, filename = os.path.split(key)
        temp_fp = os.path.join(temp_dir, filename)

        if read_operation:
            logger.debug('initiating download to tempfile')
            S3.Bucket(bucket).download_file(key, temp_fp)
            logger.debug('download to tempfile successful')

            with open(temp_fp, 'r{}+'.format(binary)) as pre_opened_file:
                # extract out the file ending with the type hint, and
                metadata = get_file_meta(pre_opened_file, truncate=True)
                if metadata is not None:
                    logger.info('found velox metadata in file signature')

        with open(temp_fp, mode) as f:
            logger.debug('yielding {} with mode {}'.format(temp_fp, mode))
            if not yield_type_hint:
                yield f
            else:
                clsname = None
                if metadata is not None:
                    clsname = obtain_qualified_name(metadata)
                    logger.debug(
                        'found type hint: {} - yielding as part pf payload'
                        .format(clsname)
                    )
                yield (f, clsname)
            logger.debug('closing {}'.format(temp_fp))

        if not read_operation:
            logger.debug('uploading {} to bucket = {} with key = {}'.format(
                temp_fp, bucket, key))
            S3.Bucket(bucket).upload_file(temp_fp, key)

        if delete_on_close:
            logger.debug('removing temporary allocations')
            shutil.rmtree(temp_dir)
            logger.debug('cleaned up, releasing')

__all__ = ['get_aware_filepath', 'ensure_exists']
