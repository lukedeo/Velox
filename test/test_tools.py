import pytest
import pickle
import numpy as np

from velox import VeloxObject, register_object
from velox.tools import (fullname, import_from_qualified_name,
                         obtain_padding_bytes, obtain_qualified_name,
                         VELOX_NEW_FILE_SIGNATURE,
                         VELOX_NEW_FILE_EXTRAS_LENGTH)

from velox_test_utils import create_class


def test_proper_fullname():
    x = np.random.normal(0, 1, (10, ))
    assert fullname(x) == 'numpy.ndarray'

    m = create_class('foo')()
    assert fullname(m) == 'velox_test_utils._Model'


def test_import_from_qualified_name():
    clf = import_from_qualified_name('sklearn.linear_model.SGDRegressor')()
    assert clf

    from sklearn.linear_model import SGDRegressor
    assert isinstance(clf, SGDRegressor)


def test_obtain_padding_bytes():
    x = np.random.normal(0, 1, (10, ))

    b = obtain_padding_bytes(x).decode()

    assert len(b) == VELOX_NEW_FILE_EXTRAS_LENGTH
    assert VELOX_NEW_FILE_SIGNATURE in b
    assert obtain_qualified_name(b) == 'numpy.ndarray'
