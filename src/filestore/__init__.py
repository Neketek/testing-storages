import tempfile
import os
import posixpath


class Filestore:
    """
    Filestore class uses native filesystem to immitate
    object storage service. Uses posix file pathes which
    internally converted in case of win system.
    """

    def __init__(self, name, root=None):
        self.name = name
        if root:
            self.dir = os.mkdir(os.path.join(root, name))
        else:
            self.temp_dir = tempfile.TemporaryDirectory()
            self.dir = self.temp_dir.name

    def _convert_key_to_path(self, key):
        # removes leading slash and converts to current system sep
        return os.path.sep.join(
            [
                part for part in key.split(posixpath.sep)
                if part
            ]
        )

    def _convert_path_to_key(self, path):
        # ensures that key is valid posixpath
        return posixpath.sep.join(part for part in path.split(os.path.sep))

    def _convert_key_to_fullpath(self, key):
        return os.path.join(
            self.dir,
            self._convert_key_to_path(key)
        )

    def post(self, key, byte_value):
        filename = self._convert_key_to_fullpath(key)
        dirname = os.path.dirname(filename)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(filename, 'wb') as f:
            f.write(byte_value)
        return True

    def delete(self, key):
        try:
            filename = self._convert_key_to_fullpath(key)
            os.remove(filename)
        except FileNotFoundError:
            return False
        return True

    def get(self, key):
        try:
            filename = self._convert_key_to_fullpath(key)
            return open(filename, 'rb')
        except FileNotFoundError:
            return None

    def list(self, key_prefix=''):
        path_prefix = self._convert_key_to_path(key_prefix)

        def iterator():
            top = os.path.join(self.dir, path_prefix)
            for root, dirnames, filenames in os.walk(top):
                for filename in filenames:
                    yield self._convert_path_to_key(
                        os.path.relpath(
                            os.path.join(root, filename),
                            self.dir
                        )
                    )
        return iterator()
