"""Contains caching primitives for avoiding expensive fetching of data from underlying layers.
Currently works best when data of an input file is transformed in an expensive way into a different representation, with a different size.
"""

import os
import hashlib
import tempfile
from os.path import stat
from collections import namedtuple

from .base import OpenFile, FileLike, VirtStat
from .realfs import FSFile
from .passthrough import Passthrough

import logging
logger = logging.getLogger('fuseblocks.fs_cache')


class CachedFSFile(FSFile):
    """Allows to use the cached file for supplying information that must be generated."""
    def get_size(self):
        return VirtStat.from_stat(os.fstat(self.fd)).st_size


class FSStore:
    """Stores file data in filesystem.
    Stores data as files with filenames matching their md5 hash.
    
    Mappings matches path to hash.
    TODO: delete when overflowing
    TODO: watch for changes.
    """
    OpenFile = CachedFSFile
    HashAlg = hashlib.md5
    def __init__(self, path):
        self.path = path # path to directory containing files
        self.hashes = {} # id to hash mapping
        if not os.path.isdir(path):
            os.mkdir(path)
    
    def get(self, id_):
        """Fetch file from cache.
        
        id_: arbitrary unique id of the file which is useful to parent, usually path
        """
        try:
            hash_ = self.hashes[id_]
        except KeyError:
            return None
        logger.debug("run cache hit")
        cache_path = os.path.join(self.path, hash_)
        return self.OpenFile(cache_path, os.O_RDONLY)

    def rehash(self, id_, src):
        """Refresh parent's hash and return if cached version found.

        id_: file identifier useful to the parent block
        src: stream to hash
        """
        logger.info("rehashing")
        halg = self.HashAlg()
        with src:
            while True:
                chunk = src.read(2**16)
                if len(chunk) == 0:
                    break
                halg.update(chunk)
        hash_ = halg.hexdigest()
        self.hashes[id_] = hash_
        cached_path = os.path.join(self.path, hash_)
        try:
            return self.OpenFile(cached_path, os.O_RDONLY)
        except FileNotFoundError:
            return None

    def update(self, id_, src):
        """Regenerate cache contents. Expects the hash is already known.

        id_: file identifier useful to parent
        data_stream: stream with data to store
        """
        logger.info("updating cache")
        hash_ = self.hashes[id_]
        with src:
            with tempfile.NamedTemporaryFile(mode='w+b', dir=self.path, delete=False) as dest:
                while True:
                    chunk = src.read(2**16)
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
        self.store = self.Store(self.CACHE_PATH)
        Passthrough.__init__(self, parent)

    def getattr(self, path):
        ret = Passthrough.getattr(self, path)
        if stat.S_ISDIR(ret.st_mode):
            return ret
        f = self.open(path, os.O_RDONLY)
        ret = VirtStat.from_stat(ret)
        ret.st_size = f.get_size()
        return ret

    def open(self, path, flags):
        if flags & (os.O_WRONLY | os.O_RDWR):
            raise NotImplementedError("Only reading supported.")
        cached = self.store.get(path)
        if cached is not None:
            return cached
        # ASSUMPTION: parent transforms file data but does not create any
        # ASSUMPTION: parent does not change paths
        # these assumptions allow us to reach for parent.parent.open directly
        # A more elegant solution would implement a "datasource" interface on cacheable transformation blocks.
        cached = self.store.rehash(path,
            FileLike(self.parent.datasource.open(path, os.O_RDONLY)))
        if cached is not None:
            return cached

        return self.store.update(path,
            FileLike(Passthrough.open(self, path, os.O_RDONLY)))
