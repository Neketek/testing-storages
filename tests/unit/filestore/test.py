from src.loggers import root
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


def test_list():
    filestore = Filestore('test-storage')
    objs = generate_objects(10, 'directory-{i}/item-{i}', 'item-{i}-value')
    for key, value in objs.items():
        filestore.post(key, value)
    for key in filestore.list(''):
        root.info(key)
