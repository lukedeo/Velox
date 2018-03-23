import pytest

from backports.tempfile import TemporaryDirectory

import boto3
from moto import mock_s3
import numpy as np
from velox.wrapper import SimplePickle, SimpleKeras
from velox.obj import load_velox_object
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


@mock_s3
def test_load_function_with_wrapper_class_local_and_s3():
    from sklearn.linear_model import SGDRegressor

    X, X_test, y = build_test_data()
    clf = SGDRegressor(n_iter=20).fit(X, y)
    y_orig = clf.predict(X_test)

    conn = boto3.resource('s3', region_name='us-east-1')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS
    # account
    TEST_BUCKET = 'foo-test-bucket'
    conn.create_bucket(Bucket=TEST_BUCKET)

    with TemporaryDirectory() as tmpdir:
        for d in [tmpdir, 's3://{}/bazprefix/'.format(TEST_BUCKET)]:
            SimplePickle(clf).save(prefix=d)
            model = load_velox_object('simplepickle', prefix=d)
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
