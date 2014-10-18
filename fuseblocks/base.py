from abc import ABCMeta, abstractmethod
import itertools
from fuse import FuseOSError, Operations, LoggingMixIn


def open_direction(flags):
    return flags & 0x2


class VirtStat:
    """Read-write os.stat_result replacement object."""
    @classmethod
    def from_stat(cls, stat):
        vstat = cls()
        for field in dir(stat):
            if field.startswith('st_'):
                setattr(vstat, field, getattr(stat, field))
        return vstat

    def __str__(self):
        return '{}({})'.format(self.__class__.__name__,
                               ', '.join('{}={!r}'.format(name, value)
                                         for name, value in
                                         sorted(self.__dict__.items())
                                         if name.startswith('st_')))
            

class BlockException(Exception):
    """Signifies an error in the backend."""
    pass


class OpenFile(metaclass=ABCMeta):
    """Basic abstraction for open files"""
    # TODO: fill in ABC
    def release(self): pass


class Block(metaclass=ABCMeta):
    """Basic building block that can be stacked and chained with other blocks to create a FUSE filesystem."""
    # TODO: fill in ABC
    @abstractmethod
    def access(self, path, mode): pass

    @abstractmethod
    def getattr(self, path): pass


class FDTracker:
    """Helper object tracking file handles passed to FUSE."""
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
    """Object that wraps Block objects in a FUSE interface."""
    def __init__(self, mount, backend):
        self.mount = mount
        self.backend = backend
        
        self.fd_tracker = FDTracker()

    def access(self, path, mode):
        return self.backend.access(path, mode)
    
    def getattr(self, path, fh=None):
        st = self.backend.getattr(path)
        return dict((key, getattr(st, key))
                    for key in
                        filter(lambda x: x.startswith('st_'), dir(st)))
    
    getxattr = None # to silence "operation not supported"
    
    def open(self, path, flags):
        fobj = self.backend.open(path, flags)
        return self.fd_tracker.add(fobj)
    
    def read(self, path, size, offset, fh):
        return self.fd_tracker[fh].read(size, offset)

    def readdir(self, path, fh):
        return self.backend.readdir(path)
    
    def readlink(self, path):
        return self.backend.readlink(path)

    def release(self, path, fh):
        self.fd_tracker[fh].release()
        del self.fd_tracker[fh]
        return 0
    
    def statvfs(self, path):
        return self.backend.statvfs(path)
    
    def utimens(self, path):
        return self.backend.utimens(path)
