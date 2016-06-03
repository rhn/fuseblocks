import threading
import os

from .base import OpenFile
from .passthrough import Passthrough


class CacheFile(OpenFile):
    def __init__(self, store, mode):
        self.store = store
        self.mode = mode
    
    def read(self, size, offset):
        return self.store.data[offset:offset+size]


class DataStore:
    def __init__(self):
        self.complete_lock = threading.Lock()
        self.data = None
    
    def wait_more(self, length, size):
        lock = threading.Lock()
        self.queue.append(length, size, lock)
        self.lock.acquire()
        self.lock.acquire()


class DataCacheBlock(Passthrough):
    """Caches file data."""
    def __init__(self, backend):
        self.backend = backend
        self.data_mapping = {}
        self.mapping_lock = threading.Lock() # guards data_mapping
    
    def get_cache(self, path):
        self.mapping_lock.acquire()
        try:
            if path not in self.data_mapping:
                open_file = Passthrough.open(self, path, os.O_RDONLY)
                store = DataStore()
                self.data_mapping[path] = store
                with store.complete_lock:
                    self.mapping_lock.release()
                    data = b''
                    offset = 0
                    readsize = 2 ** 16
                    while True:
                        new_data = open_file.read(readsize, offset)
                        data += new_data
                        offset = len(data)
                        if len(new_data) < readsize:
                            break
                    store.data = data
                open_file.release()
            else:
                store = self.data_mapping[path]
                self.mapping_lock.release()
                with store.complete_lock:
                    pass
            return store
        except:
            try: # no nice way to only catch errors up until some point in alternative ifs
                self.mapping_lock.release()
            except RuntimeError:
                pass
            raise

    def open(self, path, mode):
        return CacheFile(self.get_cache(path), mode)
