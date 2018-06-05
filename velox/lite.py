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


def save_object(obj, name, prefix, versioned=False, secret=None):
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


    Raises:
    -------

    * `ValueError` if the `name` argument is invalid.

    * `IOError` if attempting to save an unversioned file that already exists.
    """
    if not name.isalnum():
        raise ValueError('name must be alphanumeric, got: {}'.format(name))
    serialization_hook, deserialization_class = _get_serialization_hook(obj)

    # Managed objects can either be versioned or unversioned - in the
    # versioned case, they will always have the name
    # `/my/prefix/myservable-vN` where vN will range from 1 -> N. If not
    # versioned, then will simply be `/my/prefix/myservable`
    if versioned:
        matching_files = filesystem.find_matching_files(
            prefix=prefix,
            specifier='{}-*'.format(name)
        )
        version = len(matching_files) + 1
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
        logger.debug('specified secret={}'.format('*' * len(secret)))
    with filesystem.get_aware_filepath(filename, 'wb') as fileobject:
        serializer.dump(data, fileobject)


def load_object(name, prefix, versioned=False, secret=None):
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

    * `secret (str)`: A secret (**guard like a password**) to verify 
        permissions in the deserialization process.


    Raises:
    -------

    * `velox.exceptions.VeloxConstraintError` if no matching files to load the
        object from are found.

    * `velox.exceptions.RuntimeError` if `secret` does not match the secret
        that was used to save the object.
    """
    if not name.isalnum():
        raise ValueError('name must be alphanumeric, got: {}'.format(name))
    if versioned:
        matching_files = filesystem.find_matching_files(
            prefix=prefix,
            specifier='{}-*'.format(name)
        )
        if not matching_files:
            raise exceptions.VeloxConstraintError(
                'No matching files at prefix: {} with name: {}. '
                'Did you mean to load this binary with an unversioned scheme?'
                .format(prefix, name)
            )
        logger.debug('found {} matching filenames'.format(len(matching_files)))
        filename = filesystem.stitch_filename(prefix, matching_files[0])
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
    logger.debug('will load from filename: {}'.format(filename))
    serializer = itsdangerous.Serializer(secret or DEFAULT_SECRET,
                                         serializer=dill)
    with filesystem.get_aware_filepath(filename, 'rb') as fileobject:
        try:
            data = serializer.load(fileobject)
        except itsdangerous.BadSignature:
            raise RuntimeError(
                'Mismatched secret - deserialization not authorized'
            )
        deserialization_hook = _get_deserialization_hook(data['class'])
        obj = deserialization_hook(io.BytesIO(data['data']))

    return obj
