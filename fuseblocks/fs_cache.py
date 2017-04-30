import os
import hashlib
import tempfile
from os.path import stat
from collections import namedtuple

from .base import OpenFile, FileLike, VirtStat
from .realfs import FSFile
from .passthrough import Passthrough


class CachedFSFile(FSFile):
    def get_size(self):
        return VirtStat.from_stat(os.fstat(self.fd)).st_size


class FSStore:
    """Stores file data in filesystem.
    Stores data as files with filenames matching their md5 hash.
    
    Mappings matches path to hash.
    TODO: reach for the underlying storage of the file doing the processing and hash what's there (requires file cooperation).
    TODO: delete when overflowing
    TODO: watch for changes.
    """
    OpenFile = CachedFSFile
    def __init__(self, path):
        self.path = path # path to directory containing files
        self.hashes = {} # path to hash mapping
        if not os.path.isdir(path):
            os.mkdir(path)
    
    def get(self, path):
        try:
            hash_ = self.hashes[path]
        except KeyError:
            return None
        return self.OpenFile(os.path.join(self.path, hash_), os.O_RDONLY)

    def update(self, path, src):
        halg = hashlib.md5()
        with FileLike(src) as in_stream:
            with tempfile.NamedTemporaryFile(mode='w+b', dir=self.path, delete=False) as dest:
                while True:
                    chunk = in_stream.read(2**16)
                    if len(chunk) == 0:
                        break
                    halg.update(chunk)
                    dest.write(chunk)
            dest_name = dest.name
        hash_ = halg.hexdigest()
        self.hashes[path] = hash_
        cached_path = os.path.join(self.path, hash_)
        os.rename(dest.name, cached_path)
        return self.OpenFile(cached_path, os.O_RDONLY)


class DataCache(Passthrough):
    """Block for caching file contents inside a filesystem directory.
    Only contents and size are cached, metadata is obtained from the original.
    Meant to be used with layers which perform expensive operations in order to arrive at file data.
    
    To use, inherit and set CACHE_PATH.
    """
    CACHE_PATH = None # directory where temporary data will be stored
    def __init__(self, parent):
        """parent - underlying block"""
        self.store = FSStore(self.CACHE_PATH)
        Passthrough.__init__(self, parent)
    
    def getattr(self, path):
        ret = Passthrough.getattr(self, path)
        if stat.S_ISDIR(ret.st_mode):
            return ret

        cached = self.store.get(path)
        if cached is None:
            print("cache miss: {}".format(path))
            cached = self.store.update(path, Passthrough.open(self, path, os.O_RDONLY))
        ret = VirtStat.from_stat(ret)
        ret.st_size = cached.get_size()
        return ret
