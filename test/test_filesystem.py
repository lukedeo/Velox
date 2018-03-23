import pytest

import os
from glob import glob
from backports.tempfile import TemporaryDirectory
import pickle
import time
import numpy as np


from velox.filesystem import (get_aware_filepath, find_matching_files,
                              stitch_filename, ensure_exists, parse_s3)

from velox.tools import timestamp, obtain_padding_bytes

import boto3
from moto import mock_s3

import logging
logging.basicConfig(level=logging.DEBUG)

TEST_BUCKET = 'ci-velox-bucket'


@mock_s3
def test_aware_filepath():
    conn = boto3.resource('s3', region_name='us-east-1')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS
    # account
    conn.create_bucket(Bucket=TEST_BUCKET)

    with TemporaryDirectory() as d:
        fpaths = ['s3://{}/file.txt'.format(TEST_BUCKET),
                  os.path.join(d, 'file.txt')]

        for fp in fpaths:
            with pytest.raises(ValueError):
                with get_aware_filepath(fp, 'w+') as f:
                    f.write('foobar')
            with get_aware_filepath(fp, 'w') as f:
                f.write('foobar')

            with get_aware_filepath(fp, 'r') as f:
                assert f.read() == 'foobar'


@mock_s3
def test_aware_filepath_with_type_hint():
    conn = boto3.resource('s3', region_name='us-east-1')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS
    # account
    conn.create_bucket(Bucket=TEST_BUCKET)
    x = np.random.normal(0, 1, (10, ))
    with TemporaryDirectory() as d:
        fpaths = [
            's3://{}/file.txt'.format(TEST_BUCKET),
            os.path.join(d, 'file.txt')
        ]

        for fp in fpaths:
            with get_aware_filepath(fp, 'wb') as f:
                f.write(b'foobarbaz')
                f.write(obtain_padding_bytes(x))

            with get_aware_filepath(fp, 'rb') as f:
                assert f.read() == b'foobarbaz'

            with get_aware_filepath(fp, 'rb', yield_type_hint=True) as \
                    (f, type_hint):
                assert f.read() == b'foobarbaz'
                assert type_hint == 'numpy.ndarray'


def test_stitch_filename():

    reference = 's3://myBucket/file.txt'

    assert stitch_filename('s3://myBucket/', '/file.txt') == reference
    assert stitch_filename('s3://myBucket', '/file.txt') == reference
    assert stitch_filename('s3://myBucket', 'file.txt') == reference
    assert stitch_filename('s3://myBucket/', 'file.txt') == reference

    with TemporaryDirectory() as d:
        reference = os.path.join(d, 'file.txt')
        assert stitch_filename(d, 'file.txt') == reference


@mock_s3
def test_ensure_exists():
    ensure_exists('s3://{}'.format(timestamp()))
    with TemporaryDirectory() as d:
        ensure_exists(os.path.join(d, timestamp()))


def test_bad_s3():
    with pytest.raises(ValueError):
        with TemporaryDirectory() as d:
            _ = parse_s3(d)


@mock_s3
def test_list_contents():
    conn = boto3.resource('s3', region_name='us-east-1')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS
    # account
    conn.create_bucket(Bucket=TEST_BUCKET)

    nb_files = 5

    with TemporaryDirectory() as d:

        fpaths = ['s3://{}'.format(TEST_BUCKET) + '/file-{}.txt',
                  os.path.join(d, 'file-{}.txt')]

        for fp in fpaths:
            for i in range(nb_files):
                with get_aware_filepath(fp.format(i), 'w') as f:
                    f.write('foobar-{}'.format(i))

        s3match = find_matching_files('s3://{}'.format(TEST_BUCKET),
                                      specifier='*txt')
        fsmatch = find_matching_files(d, specifier='*txt')

        assert len(s3match) == nb_files
        assert len(fsmatch) == nb_files
