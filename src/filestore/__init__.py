import os
import tempfile
import posixpath
import hashlib
import json
import time
import datetime


class Filestore:
    """
    Filestore class uses native filesystem to immitate
    object storage service. Uses posix file pathes which
    internally converted in case of win system.
    """

    METADATA_MD5_KEY = 'md5'
    # modification timestamp
    METADATA_MTIMESTAMP_KEY = 'mtimestamp'
    METADATA_CONTENTSIZE_KEY = 'content_size'

    def __init__(self, name, root=None, metadata_root=None):
        self._name = name
        if root:
            self._dir = os.path.join(root, name)
            os.makedirs(self._dir, exist_ok=True)
        else:
            self._temp_dir = tempfile.TemporaryDirectory()
            self._dir = self._temp_dir.name
        if metadata_root:
            self._metadata_dir = os.path.join(metadata_root, name)
            os.makedirs(self._metadata_dir, exist_ok=True)
        else:
            self._temp_metadata_dir = tempfile.TemporaryDirectory()
            self._metadata_dir = self._temp_metadata_dir.name

    def _convert_key_to_path(self, key):
        # removes leading slash and converts to current system sep
        return os.path.sep.join(
            [
                part for part in key.split(posixpath.sep) if part
            ]
        )

    def _convert_path_to_key(self, path):
        # ensures that key is valid posixpath
        return posixpath.sep.join(part for part in path.split(os.path.sep))

    def _convert_key_to_fullpath(self, key):
        return os.path.join(
            self._dir,
            self._convert_key_to_path(key)
        )

    def _convert_key_to_metadata_fullpath(self, key):
        return os.path.join(
            self._metadata_dir,
            self._convert_key_to_path(key)
        )

    def _generate_file_metadata(self, key, byte_value):
        md5_hash = str(hashlib.md5(byte_value).hexdigest())
        return json.dumps(
            {
                self.METADATA_MD5_KEY: md5_hash,
                self.METADATA_MTIMESTAMP_KEY: time.time(),
                self.METADATA_CONTENTSIZE_KEY: len(byte_value)
            }
        ).encode()

    def _clear_directory(self, path):
        for root, dirnames, filenames in os.walk(path, False):
            for filename in filenames:
                os.remove(
                    os.path.join(root, filename)
                )
            for dirname in dirnames:
                os.rmdir(
                    os.path.join(root, dirname)
                )

    def post(self, key, byte_value):
        # Note: I know that this is not very optimized,
        # but I don't care because performance reduction is minimal
        filename = self._convert_key_to_fullpath(key)
        metadata_filename = self._convert_key_to_metadata_fullpath(key)
        metadata_bytes = self._generate_file_metadata(key, byte_value)
        dirname = os.path.dirname(filename)
        metadata_dirname = os.path.dirname(metadata_filename)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        if metadata_filename:
            os.makedirs(metadata_dirname, exist_ok=True)
        with open(filename, 'wb') as f:
            f.write(byte_value)
        with open(metadata_filename, 'wb') as f:
            f.write(metadata_bytes)
        return True

    def delete(self, key):
        try:
            filename = self._convert_key_to_fullpath(key)
            metadata_filename = self._convert_key_to_metadata_fullpath(key)
            os.remove(filename)
            os.remove(metadata_filename)
        except FileNotFoundError:
            return False
        return True

    def get(
        self,
        key,
        if_match_md5=None,
        if_unchanged_since=None
    ):
        filename = self._convert_key_to_fullpath(key)
        metadata_filename = self._convert_key_to_metadata_fullpath(key)
        try:
            metadata = json.load(open(metadata_filename, 'rb'))
            body = open(filename, 'rb')
            if (
                if_match_md5 is not None
                and metadata[self.METADATA_MD5_KEY] != if_match_md5
            ):
                return None
            if (
                if_unchanged_since is not None
                and datetime.datetime.fromtimestamp(
                    metadata[self.METADATA_MTIMESTAMP_KEY]
                ) > if_unchanged_since
            ):
                return None
            return body
        except FileNotFoundError:
            return None

    def head(self, key):
        try:
            filename = self._convert_key_to_metadata_fullpath(key)
            with open(filename, 'rb') as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    def exists(self, key):
        try:
            filename = self._convert_key_to_fullpath(key)
            os.stat(filename)
            return os.path.isfile(filename)
        except FileNotFoundError:
            return False

    def list(self, key_prefix=''):
        path_prefix = self._convert_key_to_path(key_prefix)

        def iterator():
            top = os.path.join(self._dir, path_prefix)
            for root, dirnames, filenames in os.walk(top):
                for filename in filenames:
                    yield self._convert_path_to_key(
                        os.path.relpath(
                            os.path.join(root, filename),
                            self._dir
                        )
                    )
        return iterator()

    def count(self, key_prefix=''):
        count_value = 0
        for file in self.list(key_prefix):
            count_value += 1
        return count_value

    def clear(self):
        self._clear_directory(self._dir)
        self._clear_directory(self._metadata_dir)
