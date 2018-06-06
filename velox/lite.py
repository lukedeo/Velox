#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
## `velox.lite`

The `velox.lite` submodule provides a lightweight version of Velox's binary 
management capabilities.

The core functionality is geared towards continuous deployment environments in
the machine learning world, where consistency and versioning of binary objects
is key to maintain system integrity.

Suppose we build a simple [`scikit-learn`](http://scikit-learn.org/) model that we want to be available somewhere else in a secure, verifyable manner. 

Suppose a data scientist trains the following model.
<!--begin_code-->

    #!python
    import os

    from sklearn.linear_model import SGDClassifier
    from sklearn.datasets import make_blobs
    from velox.lite import save_object

    X, y = make_blobs()

    clf = SGDClassifier()
    clf.fit(X, y)

    save_object(
        obj=clf, 
        name='CustomerModel', 
        prefix='s3://myprodbucket/ml/models', 
        secret=os.environ.get('ML_MODELS_SECRET')
    )
<!--end_code-->

Elsewhere, on a production server, we could easily load this model and
verify it's integrity.

<!--begin_code-->

    #!python
    import os

    from velox.lite import load_object
    try:
        clf = load_object(
            name='CustomerModel', 
            prefix='s3://myprodbucket/ml/models', 
            secret=os.environ.get('ML_MODELS_SECRET')
        )
    except RuntimeError:
        raise RuntimeError('Invalid Secret!')

    # do things with clf...
<!--end_code-->
"""
import dill
import io
import logging

import itsdangerous
from semantic_version import Version as SemVer, Spec as Specification

from . import exceptions
from . import filesystem
from . import tools


DEFAULT_SECRET = 'velox'


logger = logging.getLogger(__name__)


def _get_deserialization_hook(classname):
    if classname == 'dill':
        return dill.load
    else:
        classdef = tools.import_from_qualified_name(classname)
        return classdef._load


def _get_serialization_hook(obj):
    if hasattr(obj, '_save'):
        if not callable(obj._save):
            raise TypeError(
                '_save attribute on object of type {} is not callable.'
                .format(type(obj))
            )
        serialization_hook = obj._save
        deserialization_class = tools.fullname(obj)
    # TODO(lukedeo): create multiple cases here where we can handle custom
    # types like Keras models or PyTorch modules.
    else:
        serialization_hook = lambda buf: dill.dump(obj, buf)
        # We don't need to know the object class here, we just use dill
        deserialization_class = 'dill'

    return serialization_hook, deserialization_class


def _seems_like_nonlite_velox_obj(prefix, name):
    """
    Check and see if the prefix / name combo could match a subclassed velox
    object
    """
    specifier = '*_{}*.vx'.format(name)
    if filesystem.find_matching_files(prefix, specifier):
        return True
    return False


def save_object(obj, name, prefix, versioned=False, secret=None, bump='patch'):
    """
    Velox-managed method to save generic Python objects. Affords the ability
    to version saved objects to a common prefix, as well as to sign binaries
    with a secret.

    * If `obj` has a callable method called `_save`, it will call the method
        to save the object with the signature `obj._save(buf)` where buf is an
        object of type `io.BytesIO`.

    * Else, will use the fantastic `dill` library to save the 
        object generically.

    Saving / loading can proceed by two different standards - the first is a 
    versioned saving / loading scheme, where objects are saved according 
    to `/path/to/prefix/objectname-v1`, and the second is unversioned where 
    objects are saved according to `/path/to/prefix/objectname`. Versions are 
    automatically assigned - they autoincrement whenever a new object is pushed
    to the given prefix.

    Args:
    -----

    * `obj (object)`: Object that is either pickle-able / dill-able or 
        defines a `_save(...)` method to serialize to a `io.BytesIO` object.

    * `name (str)`: The name to save the object under at the `prefix` location.

    * `prefix (str)`: the prefix (can be on S3 or on a local filesystem) to
        save the managed object to. If on S3, it is expected to take the form
        `s3://my-bucket/other/things/`.

    * `versioned (bool)`: Whether or not to save the object according to a 
        versioned scheme.

    * `secret (str)`: A secret (**guard like a password**) to require in 
        the deserialization process.

    * `bump (str)`: One of `{major, minor, patch}`, indicates the semantic
        version bump to save the `obj` with. Consult with the 
        [semantic versioning website](https://semver.org/) for more information.

    Returns:
    --------

    `filename`: the final filename that `obj` is saved into. 


    Raises:
    -------

    * `IOError` if attempting to save an unversioned file that already exists.

    * `ValueError` if a semantic version string cannot be parsed.
    """
    serialization_hook, deserialization_class = _get_serialization_hook(obj)

    # Managed objects can either be versioned or unversioned - in the
    # versioned case, they will always have the name
    # `/my/prefix/myservable-v0.2.3` where vX will be a semver string. If not
    # versioned, then will simply be `/my/prefix/myservable`
    if versioned:
        matching_files = filesystem.find_matching_files(
            prefix=prefix,
            specifier='{}-v*'.format(name)
        )

        # We first try to parse all the substrings defined by the last RHS of a
        # block seperated by a `-v`.
        try:
            matched_versions = [SemVer(f.split('-v')[-1])
                                for f in matching_files]
        except ValueError as err:
            raise ValueError('Error parsing semantic version string: {}'
                             .format(err))

        if matched_versions:
            latest_version = max(matched_versions)
            if bump == 'patch':
                version = latest_version.next_patch()
            elif bump == 'minor':
                version = latest_version.next_minor()
            elif bump == 'major':
                version = latest_version.next_major()
            else:
                raise ValueError('invalid bump: {}'.format(bump))
        else:
            version = SemVer('0.1.0')

        logger.debug('assigning version v{}'.format(version))
        filename = filesystem.stitch_filename(prefix,
                                              '{}-v{}'.format(name, version))

    else:
        filename = filesystem.stitch_filename(prefix, name)
        matching_files = filesystem.find_matching_files(
            prefix=prefix,
            specifier=name
        )
        if matching_files:
            raise IOError('File: {} already exists'.format(filename))

    logger.debug('will use filename: {} for serialization'.format(filename))
    filesystem.ensure_exists(prefix)
    buf = io.BytesIO()
    serialization_hook(buf)
    buf = buf.getvalue()

    data = {
        'data': buf,
        'class': deserialization_class
    }

    serializer = itsdangerous.Serializer(secret or DEFAULT_SECRET,
                                         serializer=dill)
    if secret:
        logger.debug('specified SECRET=<{}>'.format('*' * len(secret)))
    with filesystem.get_aware_filepath(filename, 'wb') as fileobject:
        serializer.dump(data, fileobject)

    return filename


def load_object(name, prefix, versioned=False, version=None, secret=None,
                return_sha=False):
    """
    Velox-managed method to load generic Python objects that have been saved
    via `velox.lite.save_object`. Affords the ability to load versioned
    or unversioned saved objects from a common prefix, as well as to do basic
    signature verification with a secret.

    * If the object type that was saved has a `classmethod` called `_load`,
        it will call the method to load the object with the
        signature `ObjectClass._load(buf)` where buf is an object of
        type `io.BytesIO`. The `ObjectClass` is determined from a small piece
        of data written by Velox in the `velox.lite.save_object` method.

    * Else, will use the `dill` library to load the object.

    Args:
    -----

    * `name (str)`: The name of the object to load.

    * `prefix (str)`: the prefix (can be on S3 or on a local filesystem) to
        load the managed object from. If on S3, it is expected to take the form
        `s3://my-bucket/other/things/`.

    * `versioned (bool)`: Whether or not to load the object according to a 
        versioned scheme.

    * `version (str)`: A specific object semantic version to search with, or a
        semver compatible version specification (such as `<=1.0.2,>0.9.1`).

    * `secret (str)`: A secret (**guard like a password**) to verify 
        permissions in the deserialization process.

    * `return_sha (bool)`: Whether or not to return the sha as part of the 
        payload. If True, returns (obj, sha), else, just returns obj.

    Returns:
    --------

    Returns the loaded object `obj` if `not return_sha`, else, will return a
    tuple of `(obj, sha)`    

    Raises:
    -------

    * `velox.exceptions.VeloxConstraintError` if no matching files to load the
        object from are found.

    * `velox.exceptions.RuntimeError` if `secret` does not match the secret
        that was used to save the object or if a pinned version load is 
        attempted with an unversioned loading scheme.
    """
    if version and not versioned:
        raise RuntimeError('Cannot perform a search against a specific '
                           'version with unversioned loading scheme')
    try:
        if versioned:
            matching_files = filesystem.find_matching_files(
                prefix=prefix,
                specifier='{}-v*'.format(name)
            )
            if not matching_files:
                raise exceptions.VeloxConstraintError(
                    'No matching files at prefix: {} with name: {}. '
                    'Did you mean to load this binary with '
                    'an unversioned scheme?'
                    .format(prefix, name)
                )
            matched_versions = [SemVer(f.split('-v')[-1])
                                for f in matching_files]

            if version:
                best_version = Specification(version).select(matched_versions)
                if not best_version:
                    raise exceptions.VeloxConstraintError(
                        'No matching files at prefix: {} with '
                        'name: {} and version: {}. '
                        .format(prefix, name, version)
                    )
            else:
                best_version = max(matched_versions)
            filename = filesystem.stitch_filename(prefix,
                                                  '{}-v{}'
                                                  .format(name, best_version))
        else:
            filename = filesystem.stitch_filename(prefix, name)
            matching_files = filesystem.find_matching_files(
                prefix=prefix,
                specifier=name
            )
            if not matching_files:
                raise exceptions.VeloxConstraintError(
                    'No matching files at prefix: {} with name: {}. '
                    'Did you mean to load this binary with a versioned scheme?'
                    .format(prefix, name)
                )
    # If someone is trying to use this method to load an object from a
    # non-lite version of velox, let's catch that.
    except exceptions.VeloxConstraintError as err:
        if _seems_like_nonlite_velox_obj(prefix, name):
            logger.info('Found a saved object that resembles a subclassed & '
                        'managed object. Reverting to load_velox_object')
            from .obj import load_velox_object
            return load_velox_object(registered_name=name, prefix=prefix,
                                     version_constraints=version)
        raise err

    logger.debug('will load from filename: {}'.format(filename))
    serializer = itsdangerous.Serializer(secret or DEFAULT_SECRET,
                                         serializer=dill)

    sha = None
    with filesystem.get_aware_filepath(filename, 'rb') as fileobject:
        try:
            data = serializer.load(fileobject)
        except itsdangerous.BadSignature:
            raise RuntimeError(
                'Mismatched secret - deserialization not authorized'
            )
        sha = tools.sha(data['data'])
        deserialization_hook = _get_deserialization_hook(data['class'])
        obj = deserialization_hook(io.BytesIO(data['data']))

    return (obj, sha) if return_sha else obj
