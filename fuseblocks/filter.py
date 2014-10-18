import os.path
import errno
from abc import ABCMeta, abstractmethod
from fuse import FuseOSError
from .base import Block


def approved(func):
    def method(self, path, *args, **kwargs):
        if not self.is_accessible(path):
            raise FuseOSError(errno.ENOENT)
        return func(self, path, *args, **kwargs)
    return method

def approved_pass(func_name):
    @approved
    def method(self, path, *args, **kwargs):
        return getattr(self.backend, func_name)(path, *args, **kwargs)
    return method


class FilterBlock(Block, metaclass=ABCMeta):
    def __init__(self, parent_block):
        Block.__init__(self)
        self.backend = parent_block
    
    access = approved_pass('access')
    getattr = approved_pass('getattr')
    open = approved_pass('open')
    readlink = approved_pass('readlink')
    statvfs = approved_pass('statvfs')

    @approved
    def readdir(self, path):
        return [entname for entname in self.backend.readdir(path) if self.is_accessible(os.path.join(path, entname))]

    @abstractmethod
    def is_accessible(self, path): pass
