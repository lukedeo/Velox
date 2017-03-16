#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
file: object.py
description: definitions for serializable Velox objects
author: Luke de Oliveira (lukedeo@ldo.io)
"""

from abc import ABCMeta, abstractmethod
import cPickle as pickle
import fnmatch
from glob import glob
import logging
import os
import tempfile

from apscheduler.schedulers.background import BackgroundScheduler
import boto3
from semantic_version import Version as SemVer, Spec as Specification

from .tools import (abstractclassmethod, timestamp, safe_mkdir,
                    enforce_return_type, is_s3_path, parse_s3, threaded)

logger = logging.getLogger(__name__)

DEFAULT_PREFIX = os.environ.get('VELOX_ROOT')
SESSION = boto3.Session()
S3 = SESSION.resource('s3')

DEFAULT_PROTOCOL = ''

if DEFAULT_PREFIX is None:
    DEFAULT_PREFIX = os.path.join(os.environ.get('HOME'), '.heimdall', 'models')
    logger.info('environment variable VELOX_ROOT not set. '
                'Falling back to {}'.format(DEFAULT_PREFIX))


if not is_s3_path(DEFAULT_PREFIX):
    logger.info('Default prefix will be built on local file system')

    logger.debug('Safely ensuring {} exists.'.format(DEFAULT_PREFIX))
    safe_mkdir(DEFAULT_PREFIX)
else:
    logger.info('Default prefix will be on S3')
    bucket, key = parse_s3(DEFAULT_PREFIX)
    logger.info('S3 bucket = {}'.format(bucket))
    logger.info('S3 key = {}'.format(key))

    DEFAULT_PREFIX = '/'.join((bucket, key))

    DEFAULT_PROTOCOL = 's3://'

    if bucket in (_.name for _ in S3.buckets.iterator()):
        logger.debug('bucket already exists')
    else:
        logger.warn('bucket does not exist. Creating it...')
        S3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={
                'LocationConstraint': SESSION.region_name
            }
        )


class VeloxCreationError(Exception):
    """
    Raised in the event of a class instantiation that does not follow
    protocol.
    """
    pass


class VeloxConstraintError(Exception):
    """
    Raised in the event of no matches for loading a model
    """
    pass


class ManagedObject(object):
    __metaclass__ = ABCMeta

    # we dont want duplication of model names!
    _registered_object_names = []

    def __init__(self):
        self.__incr_underway = False
        self.__replacement = None
        self._scheduler = BackgroundScheduler()
        self._job_pointer = None

    def __del__(self):
        if self._scheduler.state:
            self._scheduler.shutdown()
        if self._increment_underway:
            if not self.__replacement.done():
                self.__replacement.cancel()

    def __getstate__(self):
        # capture what is normally pickled
        state = self.__dict__.copy()
        del state['_scheduler']
        return state

    def __setstate__(self, newstate):
        newstate['_scheduler'] = BackgroundScheduler()
        self.__dict__.update(newstate)

    @property
    def _needs_increment(self):
        if self.__replacement is None:
            return False
        return self.__replacement.done()

    @property
    def _increment_underway(self):
        return self.__incr_underway

    @classmethod
    def clear_registered_names(cls):
        cls._registered_object_names = []

    @abstractmethod
    def _save(self, fileobject):
        raise NotImplementedError(
            'Relying on super-class serialization not allowed')

    @abstractclassmethod
    def _load(cls, fileobject):
        raise NotImplementedError(
            'Relying on super-class de-serialization not allowed')

    def save(self, prefix=None):
        outpath = self.savepath(prefix=prefix)
        logger.debug('assigned unique filepath: {}'.format(outpath))
        with get_aware_filepath(outpath, 'wb') as fileobject:
            self._save(fileobject)
        return outpath

    @classmethod
    def load(cls, prefix=None, specifier=None):
        filepath = cls.loadpath(prefix=prefix, specifier=specifier)
        logger.debug('retrieving from filepath: {}'.format(filepath))
        with get_aware_filepath(filepath, 'rb') as fileobject:
            return cls._load(fileobject)

    def _increment(self):
        replacement = self.__replacement.result()
        current_scheduler = self._scheduler
        current_job_pointer = self._job_pointer
        self.__dict__.update(replacement.__dict__)

        self._scheduler = current_scheduler
        self._job_pointer = current_job_pointer

        self.__incr_underway = False
        self.__replacement = None

    @threaded
    def __load_async(self, prefix, specifier):
        return self.__class__.load(prefix, specifier)

    def __reload(self, prefix, specifier):
        self.__incr_underway = True
        self.__replacement = self.__load_async(prefix, specifier)

    def reload(self, prefix=None, specifier=None, scheduled=False, **interval_trigger_args):

        if scheduled:
            if not self._scheduler.state:
                self._scheduler.start()
            if self._job_pointer is not None:
                raise ValueError('Found already-running job: '
                                 '{}'.format(self._job_pointer.id))
            logger.debug('scheduling with config: '
                         '{}'.format(interval_trigger_args))

            self._job_pointer = self._scheduler.add_job(
                func=self.__reload,
                args=(prefix, specifier),
                trigger='interval',
                max_instances=1,
                **interval_trigger_args
            )

            logger.debug('launched job {}: {}'.format(
                self._job_pointer.id, self._job_pointer))

        else:
            logger.debug('initializing unscheduled async reload')
            self.__reload(prefix, specifier)

    def cancel_scheduled_reload(self):
        if self._job_pointer is not None:
            logger.info('removing job: {}'.format(self._job_pointer.id))
            self._job_pointer.remove()
            self._job_pointer = None
        else:
            logger.warning('no active reload jobs found')

    def _register_name(self, name):
        self.__registered_name = name

    @property
    def registered_name(self):
        try:
            return self.__registered_name
        except NameError:
            raise VeloxCreationError('Usage of unregistered model')

    def formatted_filename(self):
        return '{timestamp}_{name}.plx'.format(
            timestamp=timestamp(), name=self.registered_name)

    def savepath(self, prefix=None):
        if prefix is None:
            logger.debug('No prefix specified. Falling back to '
                         'default at: {}'.format(DEFAULT_PREFIX))

            return DEFAULT_PROTOCOL + os.path.join(
                DEFAULT_PREFIX, self.formatted_filename()
            )

        logger.debug('Prefix specified at: {}'.format(prefix))

        return os.path.join(prefix, self.formatted_filename())

    @classmethod
    def loadpath(cls, prefix=None, specifier=None, handle_download=True):

        # will be {s3://}path/to/thing

        prefix = DEFAULT_PROTOCOL + \
            (DEFAULT_PREFIX if prefix is None else prefix)

        v = SemVer(cls.__registered_name.split('_')[-1][1:])
        sortkey = cls.__registered_name.replace(str(v), '*')

        specifier = ('*{}'.format(specifier) if specifier is not None else '') + \
            '*_{sortkey}.plx'.format(
                sortkey=sortkey)

        logger.info('Searching for matching file in {} with specifier {}'.format(
            prefix, specifier))

        if not is_s3_path(prefix):
            logger.debug('globbing on local filesystem')

            filelist = sorted(glob(os.path.join(prefix, specifier)))[::-1]
        else:
            bucket, key = parse_s3(prefix)

            logger.debug('searching in bucket s3://{} with '
                         'pfx key = {}'.format(bucket, key))

            filelist = sorted([
                obj.key for obj in S3.Bucket(bucket).objects.filter(Prefix=key)
                if fnmatch.fnmatch(os.path.split(obj.key)[-1], specifier)
            ])[::-1]

        if not filelist:
            raise VeloxConstraintError(
                'No files matching pattern {specifier} '
                'found in {prefix}'.format(specifier=specifier, prefix=prefix)
            )

        if cls._version_spec is not None:
            logger.debug('matching version requirements: '
                         '{}'.format(cls._version_spec))

            version_identifiers = map(get_semver, filelist)
            best_match = cls._version_spec.select(version_identifiers)
            if best_match is None:
                raise VeloxConstraintError(
                    'No files matching version requirements {} were '
                    'found'.format(cls._version_spec)
                )

            filelist = [
                fp for fp, v in zip(filelist, version_identifiers)
                if v == best_match
            ]

        if len(filelist) > 1:
            logger.warn('Found {} files matching. Selecting most '
                        'recent by filename timestamp'.format(len(filelist)))

        logger.info('will load from {}'.format(filelist[0]))

        if is_s3_path(prefix):
            if handle_download:
                _, temp_fp = tempfile.mkstemp(
                    suffix='.plx', prefix='s3_tmp', text=False)
                logger.debug('tranfering from '
                             's3://{bucket}/{key} => {dest}'.format(
                                 bucket=bucket, key=filelist[0], dest=temp_fp
                             ))
                S3.Bucket(bucket).download_file(filelist[0], temp_fp)
                return temp_fp
            return 's3://{bucket}/{key}'.format(bucket=bucket, key=filelist[0])

        return filelist[0]


class register_model(object):

    def __init__(self, registered_name, version='0.1.0-alpha',
                 version_constraints=None):
        try:
            registered_name = '{}_v{}'.format(registered_name, SemVer(version))
        except ValueError:
            raise ValueError('Invalid SemVer string: {}'.format(version))

        if registered_name in ManagedObject._registered_object_names:
            raise VeloxCreationError(
                'Already a registered class named {}'.format(registered_name))
        ManagedObject._registered_object_names.append(registered_name)
        self.registered_name = registered_name

        if version_constraints is not None:
            if isinstance(version_constraints, basestring):
                self.version_specification = Specification(version_constraints)
            else:
                self.version_specification = Specification(*version_constraints)
        else:
            self.version_specification = None

    def __call__(self, cls):
        if hasattr(cls, ' ManagedObject__registered_name'):
            raise VeloxCreationError('Class already registered!')
        setattr(cls, ' ManagedObject__registered_name',
                self.registered_name)

        setattr(cls, '_version_spec',
                self.version_specification)

        return cls


def get_prefix(filepath):
    return os.path.split(filepath)[0]


def _get_naming_info(filepath):
    return os.path.splitext(os.path.split(filepath)[-1])[0].split('_')


def get_registration_name(filepath):
    return _get_naming_info(filepath)[1]


def get_specifier(filepath):
    return _get_naming_info(filepath)[0]


def get_semver(filepath):
    return SemVer(_get_naming_info(filepath)[2][1:])


def available_models():
    return ManagedObject._registered_object_name
