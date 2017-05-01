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
    HashAlg = hashlib.md5
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
        print("run cache hit")
        cache_path = os.path.join(self.path, hash_)
        return self.OpenFile(cache_path, os.O_RDONLY)

    def rehash(self, path, backing):
        """Refresh parent's hash and return if cached version found"""
        print("rehashing")
        halg = self.HashAlg()
        with FileLike(backing) as in_stream:
            while True:
                chunk = in_stream.read(2**16)
                if len(chunk) == 0:
                    break
                halg.update(chunk)
        hash_ = halg.hexdigest()
        self.hashes[path] = hash_
        cached_path = os.path.join(self.path, hash_)
        try:
            return self.OpenFile(cached_path, os.O_RDONLY)
        except FileNotFoundError:
            return None

    def update(self, path, src):
        """Regenerate cache contents. Expects the hash is already known."""
        print("updating cache")
        hash_ = self.hashes[path]
        with FileLike(src) as in_stream:
            with tempfile.NamedTemporaryFile(mode='w+b', dir=self.path, delete=False) as dest:
                while True:
                    chunk = in_stream.read(2**16)
                    if len(chunk) == 0:
                        break
                    dest.write(chunk)
            dest_name = dest.name
        cached_path = os.path.join(self.path, hash_)
        os.rename(dest.name, cached_path)
        return self.OpenFile(cached_path, os.O_RDONLY)


class DataCache(Passthrough):
    """Block for caching file contents inside a filesystem directory.
    Only contents and size are cached, metadata is obtained from the original.
    Meant to be used with layers which perform expensive operations in order to arrive at file data. These layers should do no path processing.
    
    To use, inherit and set CACHE_PATH.
    """
    CACHE_PATH = None # directory where temporary data will be stored
    Store = FSStore
    def __init__(self, parent):
        """parent - underlying block"""
        self.store = self.Store(self.CACHE_PATH)
        Passthrough.__init__(self, parent)
    
    def getattr(self, path):
        ret = Passthrough.getattr(self, path)
        if stat.S_ISDIR(ret.st_mode):
            return ret

        cached = self.store.get(path)
        if cached is None:
            # ASSUMPTION: parent transforms file data but does not create any
            # ASSUMPTION: parent does not change paths
            # these assumptions allow us to reach for parent.parent.open directly
            # A more elegant solution would implement a "datasource" interface on cacheable transformation blocks.
            cached = self.store.rehash(path, self.parent.datasource.open(path, os.O_RDONLY))
            if cached is None:
                cached = self.store.update(path, Passthrough.open(self, path, os.O_RDONLY))
        ret = VirtStat.from_stat(ret)
        ret.st_size = cached.get_size()
        return ret
