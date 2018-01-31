import pytest

from backports.tempfile import TemporaryDirectory

import numpy as np
from velox.wrapper import SimplePickle, SimpleKeras
from keras.layers import Dense
from keras.models import Sequential


def build_test_data():

    X = np.random.normal(0, 1, (100, 3))
    X_test = np.random.normal(0, 1, (100, 3))
    y = X.dot([1, 2, 3])
    y += np.random.normal(0, 5, y.shape)

    return X, X_test, y


def test_simplepickle():
    from sklearn.linear_model import SGDRegressor

    X, X_test, y = build_test_data()

    clf = SGDRegressor(n_iter=20).fit(X, y)

    y_orig = clf.predict(X_test)

    with TemporaryDirectory() as d:
        SimplePickle(clf).save(prefix=d)

        model = SimplePickle.load(prefix=d)

    assert np.allclose(y_orig, model.predict(X_test))


def test_simplekeras():

    X, X_test, y = build_test_data()

    net = Sequential([
        Dense(20, activation='relu', input_dim=3),
        Dense(1)
    ])

    net.compile('adam', 'mse')

    net.fit(X, y, epochs=1)

    y_orig = net.predict(X_test)

    with TemporaryDirectory() as d:
        SimpleKeras(net).save(prefix=d)

        model = SimpleKeras.load(prefix=d)

    assert np.allclose(y_orig, model.predict(X_test))
