from abc import ABCMeta, abstractmethod
import itertools
from fuse import FuseOSError, Operations, LoggingMixIn

class OpenFile:
    # TODO: fill in ABC
    pass


class Backend(metaclass=ABCMeta):
    # TODO: fill in ABC
    def __init__(self, mount_dir):
        self.mount_dir = mount_dir
    
    @abstractmethod
    def access(self, path, mode): pass


class FDTracker:
    def __init__(self):
        self.handles = {}
        
    def add(self, item):
        for i in itertools.count(1):
            if i not in self.handles:
                self.handles[i] = item
                return i
                
    def __getitem__(self, key):
        return self.handles[key]
        
    def __delitem__(self, key):
        del self.handles[key]


class ObjectMapper(Operations):
    def __init__(self, mount, backend):
        self.mount = mount
        self.backend = backend
        
        self.fd_tracker = FDTracker()

    def access(self, path, mode):
        return self.backend.access(path, mode)
    
    def getattr(self, path, fh=None):
        return self.backend.getattr(path)
    
    def open(self, path, flags):
        fobj = self.backend.open(self, path, flags)
        return self.fd_tracker.add(fobj)
    
    def read(self, path, size, offset, fh):
        return self.fd_tracker[fh].read(size, offset)

    def readdir(self, path, fh):
        return self.backend.readdir(path)
    
    def readlink(self, path):
        return self.backend.readlink(path)

    def release(self, path, fh):
        del self.fd_tracker[fh]
        return 0
    
    def statvfs(self, path):
        return self.backend.statvfs(path)
    
    def utimens(self, path):
        return self.backend.utimens(path)
