import os
import time
import tempfile
import datetime
import hashlib
import freezegun
from src.filestore import Filestore


def generate_objects(num, key_format, value_format):
    objs = dict()
    for i in range(num):
        objs[key_format.format(i=i)] = value_format.format(i=i).encode()
    return objs


def test_crud_temp_dir():
    filestore = Filestore('test-storage')
    objs = generate_objects(
        10,
        'test-{i}',
        'test-{i}-value'
    )
    objs_updates = generate_objects(
        10,
        'test-{i}',
        'test-{i}-value-update'
    )
    for key, value in objs.items():
        # test post
        filestore.post(key, value)
        # test get posted value
        assert filestore.get(key).read() == value
        updated_value = objs_updates[key]
        # test update/override
        filestore.post(key, updated_value)
        # test get updated value
        assert filestore.get(key).read() == updated_value
        # test delete, must return True on success
        assert filestore.delete(key)
        # test delete, must return False, file doesn't exist
        assert not filestore.delete(key)
        # test get, must return None, file doesn't extist
        assert filestore.get(key) is None


def test_list_count():
    filestore = Filestore('test')
    objs = {
        '': generate_objects(
            10,
            'item-{i}',
            'item-{i}-value'
        ),
        'prefix-1': generate_objects(
            10,
            'prefix-1/item-{i}',
            'prefix-1-item-{i}-value'
        ),
        'prefix-2/prefix': generate_objects(
            5,
            'prefix-2/prefix/item-{i}',
            'prefix-2-item-{i}-value'
        )
    }
    all_objs = {
        **objs[''],
        **objs['prefix-1'],
        **objs['prefix-2/prefix']
    }
    for prefix in objs.values():
        for key, value in prefix.items():
            filestore.post(
                key,
                value
            )
    # testing count method
    assert filestore.count() == 25
    assert filestore.count('prefix-1') == 10
    assert filestore.count('prefix-2/prefix') == 5

    # testing list method
    count = 0
    for key in filestore.list():
        assert key in all_objs
        count += 1
    assert count == 25

    count = 0
    for key in filestore.list('prefix-1'):
        assert key in objs['prefix-1']
        count += 1
    assert count == 10

    count = 0
    for key in filestore.list('prefix-2/prefix'):
        assert key in objs['prefix-2/prefix']
        count += 1
    assert count == 5


def test_clear():
    filestore = Filestore('test-storage')
    objs = {
        **generate_objects(
            30,
            'directory-{i}/item-{i}',
            'item-{i}-value'
        ),
        **generate_objects(
            10,
            'directory-{i}/directory-{i}/item-{i}',
            'item-{i}-value'
        )
    }
    for key, value in objs.items():
        filestore.post(key, value)
    assert filestore.count() == len(objs)
    filestore.clear()
    assert filestore.count() == 0


def test_conditional_get():
    filestore = Filestore('test')
    key = 'test/conditional/get/item'
    value = b'test-item-value'
    filestore.post(key, value)
    # checking regular get and head to obtain metadata
    assert filestore.get(key).read() == value
    metadata = filestore.head(key)
    assert metadata
    # extracting required data from metadata
    mtimestamp = metadata[filestore.METADATA_MTIMESTAMP_KEY]
    md5hash = metadata[filestore.METADATA_MD5_KEY]
    md5hash_modified = md5hash[:-1]
    # datetime conditions
    mdatetime = datetime.datetime.fromtimestamp(mtimestamp)
    before_mdatetime = mdatetime - datetime.timedelta(days=1)
    after_mdatetime = mdatetime + datetime.timedelta(days=1)
    # must return value exact time match
    assert filestore.get(
        key,
        if_unchanged_since=mdatetime
    ) is not None
    # must return value condition is after mtimestamp
    assert filestore.get(
        key,
        if_unchanged_since=after_mdatetime
    ) is not None
    # must not return value, condition is before mtimestamp
    assert filestore.get(
        key,
        if_unchanged_since=before_mdatetime
    ) is None
    # must return value hash is correct
    assert filestore.get(
        key,
        if_match_md5=md5hash
    ) is not None
    # must not return value hash is modified
    assert filestore.get(
        key,
        if_match_md5=md5hash_modified
    ) is None


@freezegun.freeze_time()
def test_head_metadata():
    filestore = Filestore('test')
    key = 'item'
    value = b'item-value'
    filestore.post(key, value)
    # head method return metadata json of requested key
    metadata = filestore.head(key)
    assert (
        metadata[filestore.METADATA_MD5_KEY]
        == str(hashlib.md5(value).hexdigest())
    )
    assert (
        metadata[filestore.METADATA_CONTENTSIZE_KEY]
        == len(value)
    )
    assert metadata[filestore.METADATA_MTIMESTAMP_KEY] == time.time()
    assert filestore.delete(key)
    assert filestore.head(key) is None


def test_exists():
    filestore = Filestore('test')
    directory = 'directory'
    name = 'item'
    key = f'{directory}/{name}'
    value = b'item-value'

    # no files in filestore, nothing exist
    assert not filestore.exists(key)
    assert not filestore.exists(directory)

    filestore.post(key, value)
    # file posted therefore exists
    assert filestore.exists(key)
    # directory is not recognized as an independed object only as part of a key
    # therefore doesn't exist
    assert not filestore.exists(directory)
    assert filestore.delete(key)
    # file removed therefore nothing exist
    assert not filestore.exists(key)
    assert not filestore.exists(directory)


def test_specific_directories():
    metadata_dir = tempfile.TemporaryDirectory()
    data_dir = tempfile.TemporaryDirectory()
    filestore_name = 'test'
    filestore = Filestore(filestore_name, data_dir.name, metadata_dir.name)
    key = 'item'
    value = b'item-value'
    expected_data_directory = os.path.join(
        data_dir.name,
        filestore_name
    )
    expected_metadata_directory = os.path.join(
        metadata_dir.name,
        filestore_name
    )
    # directories created before file post
    assert os.path.exists(expected_data_directory)
    assert os.path.isdir(expected_data_directory)
    assert os.path.exists(expected_metadata_directory)
    assert os.path.isdir(expected_metadata_directory)
    expected_data_file_path = os.path.join(
        expected_data_directory,
        key
    )
    expected_metadata_file_path = os.path.join(
        expected_metadata_directory,
        key
    )
    filestore.post(key, value)
    # data and metadata files created
    assert os.path.exists(expected_data_file_path)
    assert os.path.isfile(expected_data_file_path)
    assert os.path.exists(expected_metadata_file_path)
    assert os.path.isfile(expected_metadata_file_path)
