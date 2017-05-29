#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
file: object.py
description: definitions for serializable Velox objects
author: Luke de Oliveira (lukedeo@ldo.io)
"""

from abc import ABCMeta, abstractmethod
import inspect
import logging
import os

from apscheduler.schedulers.background import BackgroundScheduler
from semantic_version import Version as SemVer, Spec as Specification


from .exceptions import VeloxCreationError, VeloxConstraintError

from .filesystem import (find_matching_files, ensure_exists, stitch_filename,
                         get_aware_filepath)

from .tools import (abstractclassmethod, timestamp, threaded, sha,
                    zero_reload_downtime)

logger = logging.getLogger(__name__)


def _default_prefix():
    vroot = os.environ.get('VELOX_ROOT')
    if vroot is None:
        vroot = os.path.abspath('.')
        logger.warning('falling back to {}, as no directory specified in '
                       'VELOX_ROOT'.format(vroot))
    return vroot


class VeloxObject(object):
    __metaclass__ = ABCMeta

    # we dont want duplication of model names!
    _registered_object_names = []

    def __init__(self):
        if not hasattr(self, '_registered_spec'):
            raise VeloxCreationError(
                'Managed Velox object instantiation failed due to missing '
                'registration decorator. The definition of all VeloxObject '
                'subclasses must be surrounded by the `register_model` '
                'decorator. Please consult the documentation for more details.'
            )
        self.__incr_underway = False
        self.__replacement = None
        self._scheduler = BackgroundScheduler()
        self._job_pointer = None
        self._current_sha = None

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
        del state['_current_sha']
        return state

    def __setstate__(self, newstate):
        newstate['_scheduler'] = BackgroundScheduler()
        newstate['_current_sha'] = None
        self.__dict__.update(newstate)

    @property
    def current_sha(self):
        return self._current_sha

    @current_sha.setter
    def current_sha(self, value):
        if self._current_sha is not None:
            raise ValueError('Cannot set a sha unless previous sha was None')
        self._current_sha = value

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
        raise NotImplementedError('super-class serialization not allowed')

    @abstractclassmethod
    def _load(cls, fileobject):
        raise NotImplementedError('super-class de-serialization not allowed')

    def save(self, prefix=None):
        outpath = self.savepath(prefix=prefix)
        logger.debug('assigned unique filepath: {}'.format(outpath))

        with get_aware_filepath(outpath, 'wb') as fileobject:
            self._save(fileobject)

        return outpath

    @classmethod
    def load(cls, prefix=None, specifier=None, skip_sha=None):

        filepath = cls.loadpath(prefix=prefix, specifier=specifier)
        filesha = sha(get_filename(filepath))

        if skip_sha == filesha:
            raise VeloxConstraintError('found sha: {} when sha was explicitly '
                                       'blacklisted'.format(skip_sha))

        logger.debug('retrieving from filepath: {}'.format(filepath))

        with get_aware_filepath(filepath, 'rb') as fileobject:
            obj = cls._load(fileobject)
            if not issubclass(type(obj), VeloxObject):
                raise TypeError('loaded object of type {} must inherit from '
                                'VeloxObject'.format(cls))
            obj.current_sha = filesha

        return obj

    def _increment(self):
        replacement = self.__replacement.result()

        if self._current_sha != replacement.current_sha:
            logger.debug('will aspire to new version')
            # logger.debug('current sha: {}'.format(self._current_sha))
            logger.debug('current sha: {}'.format(self.current_sha))
            logger.debug('    new sha: {}'.format(replacement.current_sha))

            for k, v in replacement.__dict__.iteritems():
                if (k not in {'_scheduler', '_job_pointer'}) and \
                        (not k.startswith('__')):
                    self.__dict__[k] = v
        else:
            logger.debug('found matching sha: {}'.format(self._current_sha))
            logger.debug('will skip increment'.format(self._current_sha))

        self.__incr_underway = False
        self.__replacement = None

    @threaded
    def __load_async(self, prefix, specifier, skip_sha):
        logger.debug('specifying a skip_sha = {}'.format(skip_sha))

        newobj = self.__class__.load(prefix, specifier, skip_sha=skip_sha)
        self.__incr_underway = False
        return newobj

    def __reload(self, prefix, specifier):
        self.__incr_underway = True

        try:
            self.__replacement = self.__load_async(prefix, specifier, None)
            # self.current_sha)
            self._increment()

        except VeloxConstraintError, ve:
            logger.debug('reload skipped. message: {}'.format(ve.args[0]))

    def reload(self, prefix=None, specifier=None, scheduled=False,
               **interval_trigger_args):

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
            logger.debug('launched job {}: {}'
                         .format(self._job_pointer.id, self._job_pointer))

        else:
            logger.debug('initializing unscheduled async reload')
            self.__reload(prefix, specifier)

    def cancel_scheduled_reload(self):
        self._scheduler.remove_all_jobs()
        self._job_pointer = None

    def _register_name(self, name):
        self.__registered_name = name

    @property
    def registered_name(self):
        try:
            return self.__registered_name
        except NameError:
            raise VeloxCreationError('Usage of unregistered model')

    def formatted_filename(self):
        return '{timestamp}_{name}.vx'.format(
            timestamp=timestamp(),
            name=self.registered_name
        )

    def savepath(self, prefix=None):
        if prefix is None:
            prefix = _default_prefix()
            logger.debug('No prefix specified. Falling back to '
                         'default at: {}'.format(prefix))

        ensure_exists(prefix)

        logger.debug('Prefix specified at: {}'.format(prefix))

        return stitch_filename(prefix, self.formatted_filename())

    @classmethod
    def loadpath(cls, prefix=None, specifier=None, handle_download=True):

        # will be {s3://}path/to/thing

        if prefix is None:
            prefix = _default_prefix()
            logger.debug('No prefix specified. Falling back to '
                         'default at: {}'.format(prefix))

        v = SemVer(cls.__registered_name.split('_')[-1][1:])
        sortkey = cls.__registered_name.replace(str(v), '*')

        if specifier is None:
            specifier = '*_{}.vx'.format(sortkey)
        else:
            specifier = '*{}*_{}.vx'.format(specifier, sortkey)

        logger.info('Searching for matching file in {} with specifier {}'
                    .format(prefix, specifier))

        filelist = find_matching_files(prefix, specifier)

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
            logger.debug('found version to aspire to: {}'.format(best_match))

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

        return stitch_filename(prefix, filelist[0])


class register_model(object):

    def __init__(self, registered_name, version='0.1.0-alpha',
                 version_constraints=None):
        try:
            registered_name = '{}_v{}'.format(registered_name, SemVer(version))
        except ValueError:
            raise ValueError('Invalid SemVer string: {}'.format(version))

        if registered_name in VeloxObject._registered_object_names:
            raise VeloxCreationError('Already a registered class named {}'
                                     .format(registered_name))

        VeloxObject._registered_object_names.append(registered_name)
        self.registered_name = registered_name

        if version_constraints is not None:
            if isinstance(version_constraints, basestring):
                self.version_specification = Specification(version_constraints)
            else:
                self.version_specification = Specification(*version_constraints)
        else:
            self.version_specification = None

    def __call__(self, cls):
        if hasattr(cls, '_VeloxObject__registered_name'):
            raise VeloxCreationError('Class already registered!')
        setattr(cls, '_VeloxObject__registered_name',
                self.registered_name)

        setattr(cls, '_version_spec',
                self.version_specification)

        setattr(cls, '_registered_spec', True)

        reserved_attr = {
            'save',
            'reload',
            'cancel_scheduled_reload',
            'formatted_filename',
            'savepath'
        }

        for attr in cls.__dict__:

            ok = (callable(getattr(cls, attr)) and
                  inspect.getargspec(getattr(cls, attr)).args[0] == 'self' and
                  not attr.startswith('_') and
                  attr not in reserved_attr)

            if ok or (attr == '__call__'):

                logger.debug('adding zero-reload downtime for method {}'
                             .format(attr))

                setattr(cls, attr, zero_reload_downtime(getattr(cls, attr)))

        return cls


def get_prefix(filepath):
    return os.path.split(filepath)[0]


def get_filename(filepath):
    return os.path.splitext(os.path.split(filepath)[-1])[0]


def _get_naming_info(filepath):
    return get_filename(filepath).split('_')


def get_registration_name(filepath):
    return _get_naming_info(filepath)[1]


def get_specifier(filepath):
    return _get_naming_info(filepath)[0]


def get_semver(filepath):
    return SemVer(_get_naming_info(filepath)[2][1:])


def available_models():
    return VeloxObject._registered_object_name
