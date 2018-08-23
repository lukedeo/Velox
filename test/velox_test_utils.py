import dill


from velox import VeloxObject, register_object


class FooBar(object):

    def __init__(self, foo):
        self.foo = foo

    def _save(self, fobj):
        dill.dump(self.foo, fobj)

    @classmethod
    def _load(cls, fobj):
        return cls(dill.load(fobj))


def create_class(name, version='0.1.0', constraints=None):
    @register_object(
        registered_name=name,
        version=version,
        version_constraints=constraints
    )
    class _Model(VeloxObject):

        def __init__(self, o=None):
            super(_Model, self).__init__()
            self._o = o

        def _save(self, fileobject):
            dill.dump(self._o, fileobject)

        @classmethod
        def _load(cls, fileobject):
            r = cls()
            setattr(r, '_o', dill.load(fileobject))
            return r

        def obj(self):
            return self._o

    return _Model


def RESET():
    VeloxObject.clear_registered_names()


def with_reset(fn):
    from functools import wraps

    @wraps(fn)
    def wrapped(*args, **kwargs):
        result = fn(*args, **kwargs)
        RESET()
        return result
    return wrapped
