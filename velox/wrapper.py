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

    def __init__(self, managed_object=None):
        super(SimpleKeras, self).__init__()
        self._managed_object = managed_object

    def _save(self, fileobject):
        self._managed_object.save(fileobject.name, include_optimizer=False)

    @classmethod
    def _load(cls, fileobject):
        from keras.models import load_model
        o = cls()
        setattr(o, '_managed_object', load_model(fileobject.name))
        return o

    def __getattr__(self, name):
        try:
            return VeloxObject.__getattr__(self, name)
        except AttributeError:
            return eval('self._managed_object.{}'.format(name))
