#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
## `velox.wrapper`

The `velox.wrapper` submodule provides pre-packaged wrappers around Keras
models and around generally pickleable objects in the python ecosystem.
"""

import dill

from .obj import VeloxObject, register_object, _fail_bad_init, _zero_downtime


@register_object(registered_name='simplepickle')
class SimplePickle(VeloxObject):
    """ SimplePickle is a passthru wrapper for any pickle-able Python object,
    allowing you to save, load, and swap in a consistent manner.

    Args:
    -----

    * `managed_object (object)`: an pickleable object


    Example:
    ---------

        #!python
        managed_object = SimplePickle({'foo': 'bar'}})

        print(managed_object.save(prefix='/path/to/saved/model'))

        # accesses underlying dictionary attribute.
        managed_object.get('foo')
    """

    def __init__(self, managed_object=None):
        super(SimplePickle, self).__init__()
        self._managed_object = managed_object

    def _save(self, fileobject):
        dill.dump(self, fileobject)

    @classmethod
    def _load(cls, fileobject):
        return dill.load(fileobject)

    @_fail_bad_init
    @_zero_downtime
    def __getattr__(self, name):
        try:
            return VeloxObject.__getattr__(self, name)
        except AttributeError:
            return getattr(self._managed_object, name)


@register_object(registered_name='simplekeras')
class SimpleKeras(VeloxObject):
    """ SimpleKeras is a passthru wrapper for a keras model, allowing you to
    save, load, and swap in a consistent manner.

    Args:
    -----

    * `keras_model(keras.models.Model)`: an instance of a Keras model

    Raises:
    -------

    * `TypeError` if keras_model is not None and is not a Keras model

    Notes:
    ------

    a user can access attributes of the underlying object by simply the
    attribute normally.

    Example:
    ---------

        #!python
        net = keras.models.Model(x, y)
        managed_object = SimpleKeras(net)

        # will use the Velox save rather than the Keras model save
        print(managed_object.save(prefix='/path/to/saved/model'))

        # accesses underlying keras attribute.
        managed_object.summary()

    """

    def __init__(self, keras_model=None):

        from keras.models import Model
        if keras_model is not None and not isinstance(keras_model, Model):
            raise TypeError('must be a Keras model - found type: {}'
                            .format(type(keras_model)))

        super(SimpleKeras, self).__init__()
        self._keras_model = keras_model

    def _save(self, fileobject):
        self._keras_model.save(fileobject.name, include_optimizer=False)

    @classmethod
    def _load(cls, fileobject):
        from keras.models import load_model
        o = cls()
        setattr(o, '_keras_model', load_model(fileobject.name))
        return o

    @_fail_bad_init
    @_zero_downtime
    def __getattr__(self, name):
        try:
            return VeloxObject.__getattr__(self, name)
        except AttributeError:
            return getattr(self._keras_model, name)
