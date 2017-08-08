#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
## `velox.obj`

The `velox.obj` submodule provides all utilities, functionality, and logic around 
the instantiation, maintainence, and lifecycle of all `velox.obj.VeloxObject`
instances.
"""

from abc import ABCMeta, abstractmethod
import inspect
import logging
import os

from apscheduler.schedulers.background import BackgroundScheduler
from semantic_version import Version as SemVer, Spec as Specification
import six


from .exceptions import VeloxCreationError, VeloxConstraintError

from .filesystem import (find_matching_files, ensure_exists, stitch_filename,
                         get_aware_filepath)

from .tools import abstractclassmethod, timestamp, threaded, sha

logger = logging.getLogger(__name__)


def _default_prefix():
    vroot = os.environ.get('VELOX_ROOT')
    if vroot is None:
        vroot = os.path.abspath('.')
        logger.warning('falling back to {}, as no directory specified in '
                       'VELOX_ROOT'.format(vroot))
    return vroot


def _fail_bad_init(fn):
    """
    checks to make sure that an invoked objectmethod is invoked on a class 
    that has had it's superclass __init__() invoked.
    """
    from functools import wraps

    @wraps(fn)
    def _respect_reqs(cls, *args, **kw):
        if not hasattr(cls, '_parent_instantiated'):
            raise VeloxCreationError('Object of type {} instantiated without '
                                     'call to constructor of super-class')
        return fn(cls, *args, **kw)

    _respect_reqs.__doc__ = fn.__doc__
    return _respect_reqs


def _ensure_init(exclude):
    def decorate(cls):
        for attr in cls.__dict__:
            if callable(getattr(cls, attr)) and attr not in exclude:
                setattr(cls, attr, _fail_bad_init(getattr(cls, attr)))
        return cls
    return decorate


@_ensure_init(
    exclude=['__init__', 'load', '_load', '__del__', '__getstate__',
             '__setstate__', 'loadpath', 'clear_registered_names'],
)
class VeloxObject(object):
    """ `velox.obj.VeloxObject` provides a simple consistent way to handle saving,
    loading, versioning, and swapping binary objects.

    The core functionality depends on three main ideas.

    * A prefix, which tells Velox where to save blobs
    * A specifier, which tells Velox to search for particular substrings when
        handling aspiration
    * A version, which lets Velox know how much flexibility there is when
        loading new models

    The prefix is essentially the location where Velox will serialize objects.
    It can be on a filesystem or on s3. It can be set in two ways.

    * The `VELOX_ROOT` environment variable can be set to contain the
        default prefix.
    * During the invocation of `velox.obj.VeloxObject.save` or
        `velox.obj.VeloxObject.load`, a `prefix` keyword argument can be specified
        which allows an override of the default location.

    If the `prefix` keyword argument is not passed, we fall back to `VELOX_ROOT`
    if it is set, otherwise, we fall back to the current working directory.

    A prefix on a reachable filesystem needs nothing besides the path, and a
    prefix on S3 simply requires `s3://bucketName/foo/`.


    As an example, we could define the follwing model, inheriting of course from
    `velox.obj.VeloxObject`.

        #!python
        @register_model(
            registered_name='user_model', 
            version='1.1.4-alpha',
            version_constraints='>=1.0,<3.0'
        )
        class UserModel(VeloxObject):
            def __init__(self, user_model):
                super(UserModel, self).__init__()
                self._user_model = user_model
                self._etl = ETLPipeline()

            def _save(self, fileobject):
                pickle.dump(self, fileobject)

            @classmethod
            def _load(cls, fileobject):
                return pickle.load(fileobject)

            def predict(self, user):
                return self._user_model.predict(
                    self._etl.transform(user)
                )
    """

    __metaclass__ = ABCMeta

    # we dont want duplication of model names!
    _registered_object_names = []

    def __init__(self):
        """
        Base constructor for managed objects. 

        Raises:
        -------

        * `velox.exceptions.VeloxCreationError` if any type that inherits from
            this class fails to invoke this method (i.e., doesn't call 
            `super(NewModelClass, self).__init__()`)

        * `velox.exceptions.VeloxCreationError` if the class is defined without
            a wrapping call from `velox.obj.register_model`.
        """
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
        self._parent_instantiated = True

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
        """
        Defines the current SHA1 of the filename that has most recently been
        loaded. If a file has never been loaded, this will be None.
        """
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
        """
        Unregisters all managed models.
        """
        logger.warning('Removing {} registered models. Proceed with caution.'
                       .format(len(cls._registered_object_names)))

        cls._registered_object_names = []

    @abstractmethod
    def _save(self, fileobject):
        raise NotImplementedError('super-class serialization not allowed')

    @abstractclassmethod
    def _load(cls, fileobject):
        raise NotImplementedError('super-class de-serialization not allowed')

    def save(self, prefix=None):
        """
        Saves the managed object instance using the user-defined method defined 
        in `_save`.

        Args:
        -----

        * `prefix (str)`: the prefix (can be on s3 or on a local filesystem) to 
            save the managed object to. If not passed will default to the
            value of the `VELOX_ROOT` env var if set, else, will fall back to
            the current working directory. 
        """

        outpath = self.savepath(prefix=prefix)
        logger.debug('assigned unique filepath: {}'.format(outpath))

        with get_aware_filepath(outpath, 'wb') as fileobject:
            self._save(fileobject)

        return outpath

    @classmethod
    def load(cls, prefix=None, specifier=None, skip_sha=None):
        """
        Loads a managed object instance using the user-defined method defined 
        in `_load`.

        Args:
        -----

        * `prefix (str)`: the prefix (can be on s3 or on a local filesystem) to
            load a managed object from. If not passed will default to the
            value of the `VELOX_ROOT` env var if set, else, will fall back to
            the current working directory.

        * `specifier (str)`: any substrings in the timestamp (as generated by
        `velox.tools.timestamp`) to explicitly search for.

        * `skip_sha (str)`: define a filename SHA1 to skip over.


        Raises:
        -------

        * `velox.exceptions.VeloxConstraintError` if we try to load from a SHA1 
            for which a skip was requested
        * `TypeError` if the user-defined `_load` function loads an object that 
            does not inherit from `velox.obj.VeloxObject`.

        """

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

            for k, v in replacement.__dict__.items():
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

        except VeloxConstraintError as ve:
            logger.debug('reload skipped. message: {}'.format(ve.args[0]))

    def reload(self, prefix=None, specifier=None, scheduled=False,
               **interval_trigger_args):
        """
        Defines the scheme by which to reload (hot swap) in-place. A scheduled 
        reload can be canceled with a call to 
        `velox.obj.VeloxObject.cancel_scheduled_reload`.

        Args:
        -----

        * `prefix (str)`: the prefix (can be on s3 or on a local filesystem) to
            load a managed object from. If not passed will default to the
            value of the `VELOX_ROOT` env var if set, else, will fall back to
            the current working directory.

        * `specifier (str)`: any substrings in the timestamp (as generated by
        `velox.tools.timestamp`) to explicitly search for.

        * `scheduled (bool)`: whether or not to run this as a scheduled and
            seperate threaded process (`True`) or to simply to an in-place swap
            (`False`). Only `scheduled=True` can guarantee zero-downtime.

        * `interval_trigger_args`: additional arguments to pass the the
            `BackgroundScheduler` object. Most commonly, you can pass something
            like `minutes=2` to schedule a poll to the prefix location every
            two minutes.

        Raises:
        -------

        * `ValueError` if you attempt to schedule a reload task when one is 
            already specified
        """

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
        """
        Cancels a scheduled reload background task started through a call to 
        `velox.obj.VeloxObject.reload`.

        Raises:
        -------

        * `ValueError` if no jobs are available to cancel. 
        """
        if self._job_pointer is None:
            raise ValueError('no available job to cancel.')

        self._scheduler.remove_all_jobs()
        self._job_pointer = None

    def _register_name(self, name):
        self.__registered_name = name

    @property
    def registered_name(self):
        """
        Defines the registered name (i.e., from the `velox.obj.register_model` 
        decorator) of the class.
        """
        try:
            return self.__registered_name
        except NameError:
            raise VeloxCreationError('Usage of unregistered model')

    def formatted_filename(self):
        """
        Returns the formatted filename (without prefix) of the class, in the 
        format `'{timestamp}_{name}.vx'`.
        """
        return '{timestamp}_{name}.vx'.format(
            timestamp=timestamp(),
            name=self.registered_name
        )

    def savepath(self, prefix=None):
        """
        Stitches a prefix and a filename from 
        `velox.obj.VeloxObject.formatted_filename` to form a path to save a model 
        to.

        Args:
        -----

        * `prefix (str)`: the prefix (can be on s3 or on a local filesystem) to 
            save the managed object to. If not passed will default to the
            value of the `VELOX_ROOT` env var if set, else, will fall back to
            the current working directory. 

        Returns:
        --------

        A full filename where a model can be saved to.
        """
        if prefix is None:
            prefix = _default_prefix()
            logger.debug('No prefix specified. Falling back to '
                         'default at: {}'.format(prefix))

        ensure_exists(prefix)

        logger.debug('Prefix specified at: {}'.format(prefix))

        return stitch_filename(prefix, self.formatted_filename())

    @classmethod
    def loadpath(cls, prefix=None, specifier=None):
        """
        Determined the file to load from given the `prefix`, the `specifier`, 
        and any version constraint information from `velox.obj.register_model`. 
        Will always return the most recently created file that satisfies all 
        constraints.

        Args:
        -----

        * `prefix (str)`: the prefix (can be on s3 or on a local filesystem) to 
            load a managed object from. If not passed will default to the
            value of the `VELOX_ROOT` env var if set, else, will fall back to
            the current working directory.

        * `specifier (str)`: any substrings in the timestamp (as generated by 
        `velox.tools.timestamp`) to explicitly search for. 

        Returns:
        --------

        A full filename where a model can be loaded from.


        Raises:
        -------

        * `velox.exceptions.VeloxConstraintError` if no matching candidates 
            are found from the specified prefix subject to specified constraints.
        """

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

            version_identifiers = list(map(get_semver, filelist))
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


def _zero_downtime(fn):
    from functools import wraps

    @wraps(fn)
    def _respect_reload(cls, *args, **kw):
        if cls._needs_increment:
            logger.info('model version increment needed')
            cls._increment()
        # else:
        return fn(cls, *args, **kw)

    _respect_reload.__doc__ = fn.__doc__
    return _respect_reload


class register_model(object):

    """
    Wraps the `velox.obj.VeloxObject` subclass definitions to ensure all 
    necessary ancillary metadata is added. Allows for versioning and constraint
    definition for reloading procedure.

    For example, we could define the follwing model, inheriting of course from
    `velox.obj.VeloxObject`.

        #!python
        @register_model(
            registered_name='price_model', 
            version='0.2.1-rc1',
            version_constraints='>=0.1.0,<0.3.0,!=0.2.0'
        )
        class PriceModel(VeloxObject):
            def __init__(self, price_model):
                super(VeloxObject, self).__init__()
                self._price_model = price_model
                self._etl = ETLPipeline()

            def _save(self, fileobject):
                pickle.dump(self, fileobject)

            @classmethod
            def _load(cls, fileobject):
                return pickle.load(fileobject)

            def predict(self, X):
                return self._price_model.predict(
                    self._etl.transform(X)
                )
    """

    def __init__(self, registered_name, version='0.1.0-alpha',
                 version_constraints=None):
        """ Decorates an object with the required attributes to be managed by 
        Velox. Adds zero-downtime reloading to all non-velox-managed
        functionality.

        Args:
        -----

        * `registered_name (str)`: registration name for the class.
        * `version (str)`: a Sem Ver string specifying this classes version. 
        * `version_constraints (str | list)`: a Sem Ver version constraint 
            string  or list of strings specifying versioning restrictions for 
            loading.

        Raises:
        -------

        * `ValueError` if an invalid SemVer string is passed to either the 
            `version` or `version_constraints` keyword arguments. 

        * (on `__call__` invocation) `velox.exceptions.VeloxCreationError` if 
            `'{registered_name}_v{version}'` is not globally unique.

        * (on `__call__` invocation) `TypeError` if the defined class does not 
            inherit from `velox.obj.VeloxObject`.
        """

        try:
            registered_name = '{}_v{}'.format(registered_name, SemVer(version))
        except ValueError:
            raise ValueError('Invalid SemVer string: {}'.format(version))

        if registered_name in VeloxObject._registered_object_names:
            raise VeloxCreationError('Already a registered class named {}'
                                     .format(registered_name))

        VeloxObject._registered_object_names.append(registered_name)
        self._registered_name = registered_name

        if version_constraints is not None:
            if isinstance(version_constraints, six.string_types):
                self.version_specification = Specification(version_constraints)
            else:
                self.version_specification = Specification(*version_constraints)
        else:
            self.version_specification = None

    def __call__(self, cls):
        if not issubclass(cls, VeloxObject):
            raise TypeError('Type {} must inherit from '
                            'VeloxObject'.format(cls))
        if hasattr(cls, '_VeloxObject__registered_name'):
            raise VeloxCreationError('Class already registered!')
        setattr(cls, '_VeloxObject__registered_name',
                self._registered_name)

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

        logger.info('full: {}'.format(cls.__dict__))

        for attr in cls.__dict__:
            fn = getattr(cls, attr)

            logger.info('considering {}'.format(attr))

            if callable(fn) and inspect.getargspec(fn).args[0] == 'self':
                if not attr.startswith('_') and attr not in reserved_attr:

                    logger.debug('adding zero-reload downtime for method {}'
                                 .format(attr))

                    setattr(cls, attr, _fail_bad_init(_zero_downtime(fn)))

        return cls


def get_prefix(filepath):
    """
    From a `filepath`, will return the `prefix`
    """
    return os.path.split(filepath)[0]


def get_filename(filepath):
    """
    From a `filepath`, will return the filename **without** the `.vx` extension
    """
    return os.path.splitext(os.path.split(filepath)[-1])[0]


def _get_naming_info(filepath):
    return get_filename(filepath).split('_')


def get_registration_name(filepath):
    """
    Get the name passed to the `registered_name` keyword argument of the 
    `velox.obj.register_model` decorator.
    """
    return _get_naming_info(filepath)[1]


def get_specifier(filepath):
    """
    Gets the unique time identifier associated with filename.
    """
    return _get_naming_info(filepath)[0]


def get_semver(filepath):
    """
    Returns a `Version` object with the version of the passed-in filename 
    from the `semantic_version` package.
    """
    return SemVer(_get_naming_info(filepath)[2][1:])


def available_models():
    """
    Get a list of all available models (i.e., those registered with 
    `velox.obj.register_model`)
    """
    return VeloxObject._registered_object_names
