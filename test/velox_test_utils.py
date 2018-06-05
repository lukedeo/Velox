import dill


class FooBar(object):

    def __init__(self, foo):
        self.foo = foo

    def _save(self, fobj):
        dill.dump(self.foo, fobj)

    @classmethod
    def _load(cls, fobj):
        return cls(dill.load(fobj))
