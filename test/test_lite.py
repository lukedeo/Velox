import pytest
import dill
import os
from backports.tempfile import TemporaryDirectory
from velox.lite import save_object, load_object
from velox.exceptions import VeloxConstraintError

from sklearn.linear_model import SGDClassifier
from sklearn.datasets import make_blobs

import boto3
from moto import mock_s3

import logging
logging.basicConfig(level=logging.DEBUG)

import velox_test_utils

TEST_BUCKET = 'ci-velox-bucket'


@pytest.fixture(
    params=['sklearn', 'dict', 'custom_obj']
)
def obj_instance(request):
    if request.param == 'sklearn':
        return SGDClassifier().fit(*make_blobs())
    elif request.param == 'custom_obj':
        return velox_test_utils.FooBar(92)
    else:
        return {'foo': 'bar', 'biz': 'bap'}


@pytest.fixture(
    params=['versioned', 'unversioned']
)
def versioned(request):
    return request.param == 'versioned'


@pytest.fixture(
    params=['secret', 'no_secret']
)
def secret(request):
    if request.param == 'secret':
        return 'VeloxTesting123'
    return None


@pytest.fixture(
    params=['s3', 'local']
)
def prefix(request):
    if request.param == 'local':
        with TemporaryDirectory() as d:
            yield d
    else:
        with mock_s3():
            conn = boto3.resource('s3', region_name='us-east-1')
            # We need to create the bucket since this is all in Moto's 'virtual' AWS
            # account
            conn.create_bucket(Bucket=TEST_BUCKET)
            yield 's3://{}/path'.format(TEST_BUCKET)


@pytest.fixture
def name():
    return 'OBJECTNAME'


def test_save_load(name, prefix, obj_instance, versioned, secret):
    save_object(obj_instance, name, prefix, versioned=versioned, secret=secret)
    _ = load_object(name, prefix, versioned=versioned, secret=secret)


def test_save_once_unversioned(name, prefix, obj_instance, secret):
    save_object(obj_instance, name, prefix, versioned=False, secret=secret)
    with pytest.raises(IOError):
        save_object(obj_instance, name, prefix, versioned=False, secret=secret)


def test_load_versioned(name, prefix, secret):
    save_object(1, name, prefix, versioned=True, secret=secret)
    assert 1 == load_object(name, prefix, versioned=True, secret=secret)

    save_object(2, name, prefix, versioned=True, secret=secret)
    save_object('foo', name, prefix, versioned=True, secret=secret)
    assert 'foo' == load_object(name, prefix, versioned=True, secret=secret)


def test_load_not_saved(name, prefix, versioned, secret):
    with pytest.raises(VeloxConstraintError):
        load_object(name, prefix, versioned=versioned, secret=secret)


def test_load_secret_mismatch(name, prefix, versioned, secret):
    save_object('foo', name, prefix, versioned=versioned, secret=secret)
    with pytest.raises(RuntimeError):
        load_object(name, prefix, versioned=versioned, secret='WrongSecret')
