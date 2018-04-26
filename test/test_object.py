import pytest

import os
from glob import glob
from backports.tempfile import TemporaryDirectory
import pickle

import time


from velox import VeloxObject, register_object, load_velox_object
from velox.exceptions import VeloxCreationError, VeloxConstraintError
from velox.tools import timestamp


def RESET():
    VeloxObject.clear_registered_names()

import logging
logging.basicConfig(level=logging.DEBUG)


def test_inconsistent_load_type():

    @register_object(registered_name='inconsistent')
    class InconsistentTypedModel(VeloxObject):

        def __init__(self):
            super(InconsistentTypedModel, self).__init__()
            self.foo = 'bar'

        def _save(self, fileobject):
            pickle.dump(self.foo, fileobject)

        @classmethod
        def _load(cls, fileobject):
            return 'this is an inconsistent return type'

    with TemporaryDirectory() as d:
        InconsistentTypedModel().save(prefix=d)

        with pytest.raises(TypeError):
            o = InconsistentTypedModel.load(prefix=d)

    RESET()


def test_missing_super_class_init():

    @register_object(registered_name='missing')
    class MissingInit(VeloxObject):

        def __init__(self):
            pass

        def _save(self, fileobject):
            pass

        @classmethod
        def _load(cls, fileobject):
            return cls()

        def method(self):
            pass

    with pytest.raises(VeloxCreationError):
        MissingInit().method()

    RESET()


@register_object(
    registered_name='veloxmodel',
    version='0.1.0'
)
class VeloxModel(VeloxObject):

    def __init__(self, o=None):
        super(VeloxModel, self).__init__()
        self._o = o

    def _save(self, fileobject):
        pickle.dump(self, fileobject)

    @classmethod
    def _load(cls, fileobject):
        return pickle.load(fileobject)


def test_load_save_self():

    with TemporaryDirectory() as d:
        VeloxModel({1: 2}).save(prefix=d)
        o = VeloxModel.load(prefix=d)

    assert o._o[1] == 2

    RESET()


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
            pickle.dump(self._o, fileobject)

        @classmethod
        def _load(cls, fileobject):
            r = cls()
            setattr(r, '_o', pickle.load(fileobject))
            return r

        def obj(self):
            return self._o

    return _Model


def test_correct_definition():

    CorrectModel = create_class('correctmodel')

    _ = CorrectModel()
    assert True
    RESET()


def test_missing_registration():

    class IncorrectModel(VeloxObject):

        def __init__(self, clf=None):
            super(IncorrectModel, self).__init__()
            self._clf = clf

        def _save(self, fileobject):
            pickle.dump(self, fileobject)

        @classmethod
        def _load(cls, fileobject):
            return pickle.load(fileobject)

    with pytest.raises(VeloxCreationError):
        _ = IncorrectModel()


def test_double_registration():

    FirstModel = create_class('foobar')

    with pytest.raises(VeloxCreationError):
        SecondModel = create_class('foobar')

    RESET()


def test_prefix_defaults():
    from velox.obj import _default_prefix

    with TemporaryDirectory() as d:
        assert _default_prefix() == os.path.abspath('.')

        os.environ['VELOX_ROOT'] = d

        assert _default_prefix() == d

        del os.environ['VELOX_ROOT']


def test_basic_saving_loading():

    Model = create_class('foobar')

    with TemporaryDirectory() as d:
        m = Model({})
        p = m.save(prefix=d)

        assert len(glob(os.path.join(d, '*'))) == 1

        assert os.path.split(p)[0] == d

        m2 = Model({'foo': 'bar'})
        _ = m2.save(prefix=d)

        assert len(glob(os.path.join(d, '*'))) == 2

        o = Model.load(prefix=d)

        assert o._o['foo'] == 'bar'

    RESET()


def test_reloading():
    Model = create_class('foobar')

    with TemporaryDirectory() as d:
        m = Model({'foo': 'bar'})

        p = m.save(prefix=d)

        o = Model({})
        assert o.current_sha is None

        o.reload(prefix=d, scheduled=True, seconds=0.5)

        time.sleep(0.75)

        cur_sha = o.current_sha
        assert cur_sha is not None
        assert o.obj()['foo'] == 'bar'

        Model({'foo': 'baz'}).save(prefix=d)

        time.sleep(1)

        assert cur_sha != o.current_sha

        assert o.obj()['foo'] == 'baz'

        with pytest.raises(ValueError):
            o.current_sha = 'foo'

        o.cancel_scheduled_reload()

        with pytest.raises(ValueError):
            o.cancel_scheduled_reload()

    RESET()


def test_local_cache_save_on_load():
    RESET()
    Model = create_class('foobar')
    import os
    with TemporaryDirectory() as prefix_dir:
        with TemporaryDirectory() as cache_dir:
            m = Model({'foo': 'bar'})

            p = m.save(prefix=prefix_dir)

            _ = Model.load(prefix=prefix_dir, local_cache_dir=cache_dir)

            filename = os.path.basename(p)

            assert os.path.isfile(os.path.join(cache_dir, filename))

    RESET()


def test_local_cache_load():

    Model = create_class('foobar')
    import os
    with TemporaryDirectory() as prefix_dir:
        with TemporaryDirectory() as cache_dir:
            m = Model({'foo': 'bar'})
            p = m.save(prefix=prefix_dir)

            _ = Model.load(prefix=prefix_dir, local_cache_dir=cache_dir)

            # _ = Model.load(prefix=prefix_dir, local_cache_dir=cache_dir)

            filename = os.path.basename(p)

            # if the load function is trying to load from the cache, this
            # causes an unpickling error
            with open(os.path.join(cache_dir, filename), 'w+') as fp:
                fp.write('0000000000')

            with pytest.raises(Exception):
                _ = Model.load(prefix=prefix_dir, local_cache_dir=cache_dir)

            # this should be fine, and should not raise an error
            _ = Model.load(prefix=prefix_dir)

    RESET()


def test_version_constraints():

    ModelA = create_class('foobar', version='0.2.1', constraints='<1.0.0')
    ModelB = create_class('foobar', version='0.3.0')
    ModelC = create_class('foobar', version='1.0.0', constraints='>=0.3.0')

    with TemporaryDirectory() as d:

        ModelA({'foo': 'bar'}).save(prefix=d)

        _ = ModelB.load(prefix=d)

        with pytest.raises(VeloxConstraintError):
            _ = ModelC.load(prefix=d)

        ModelB({'foo': 'baz'}).save(prefix=d)

        o = ModelA.load(prefix=d)
        assert o.obj()['foo'] == 'baz'

    RESET()


def test_nothing_to_reload():

    Model = create_class('foobar')

    with TemporaryDirectory() as d:
        with pytest.raises(VeloxConstraintError):
            _ = Model.load(prefix=d)

    with TemporaryDirectory() as d:
        o = Model()
        o.reload(prefix=d, scheduled=True, seconds=1)
        time.sleep(1.2)

    RESET()


def test_raises_on_empty_file(tmpdir):

    Model = create_class('foobar')
    prefix = str(tmpdir.mkdir('sub'))

    Model({'foo': 'bar'}).save(prefix=prefix)

    filename = glob(os.path.join(prefix, '*'))[0]

    # delete the content of the file
    with open(filename, "w"):
        pass

    with pytest.raises(VeloxConstraintError):
        _ = Model.load(prefix=prefix)

    RESET()
