from .obj import VeloxObject, register_model


@register_model(registered_name='simple_pickle')
class SimplePickle(VeloxObject):

    def __init__(self, managed_object=None):
        super(SimplePickle, self).__init__()
        self._managed_object = managed_object

    def _save(self, fileobject):
        pickle.dump(self, fileobject)

    @classmethod
    def _load(cls, fileobject):
        return pickle.load(fileobject)

    def __getattr__(self, name):
        try:
            return VeloxObject.__getattr__(self, name)
        except AttributeError:
            return eval('self._managed_object.{}'.format(name))


@register_model(registered_name='simple_keras')
class SimpleKeras(VeloxObject):

    """ SimpleKeras is a passthru wrapper for a keras model, allowing you to 
    save, load, and swap in a consistent manner.

    Args:
    -----
        keras_model: an instance of a Keras model

    Raises:
    -------
        TypeError if keras_model is not None and is not a Keras model

    Attributes:
    -----------

        _keras_model: underlying Keras model


    Notes:
    ------

        a user can access attributes of the underlying object by simply 
        the attribute normally

    Examples:
    ---------

        >>> net = keras.models.Model(x, y)
        >>> managed_object = SimpleKeras(net)
        # will use the Velox save rather than the Keras model save
        >>> print(managed_object.save(prefix='/path/to/saved/model'))
        /path/to/saved/model/20170523052025_simplekeras_v0.1.0-alpha.vx
        >>> managed_object.summary() # accesses underlying keras attribute.

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

    def __getattr__(self, name):
        try:
            return VeloxObject.__getattr__(self, name)
        except AttributeError:
            return eval('self._keras_model.{}'.format(name))
