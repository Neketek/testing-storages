import tempfile
import os


class Filestore:

    def __init__(self, name, root=None):
        self.name = name
        if root:
            self.dir = os.mkdir(os.path.join(root, name))
        else:
            self.temp_dir = tempfile.TemporaryDirectory()
            self.dir = self.temp_dir.name

    def _get_filename_from_key(self, key):
        return os.path.join(self.dir, key)

    def post(self, key, byte_value):
        with open(self._get_filename_from_key(key), 'wb') as f:
            f.write(byte_value)
        return True

    def delete(self, key):
        try:
            os.remove(self._get_filename_from_key(key))
        except FileNotFoundError:
            return False
        return True

    def get(self, key):
        try:
            return open(self._get_filename_from_key(key))
        except FileNotFoundError:
            return None
