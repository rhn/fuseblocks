import os.path
import errno
from fuse import FuseOSError
from .base import Backend
from .passthrough import PassthroughBackend


def approved(func):
    def method(self, path, *args, **kwargs):
        if not self.predicate(path):
            raise FuseOSError(errno.ENOENT)
        return func(self, path, *args, **kwargs)
    return method

def approved_pass(func_name):
    @approved
    def method(self, path, *args, **kwargs):
        return getattr(self.backend, func_name)(path, *args, **kwargs)
    return method


class FilterBackend(Backend):
    # predicate(self, path)
    
    def __init__(self, mount_dir, base_dir):
        Backend.__init__(self, mount_dir)
        self.backend = PassthroughBackend(mount_dir, base_dir)
    
    access = approved_pass('access')
    getattr = approved_pass('getattr')
    open = approved_pass('open')
    readlink = approved_pass('readlink')
    statvfs = approved_pass('statvfs')

    @approved
    def readdir(self, path):
        return [entname for entname in self.backend.readdir(path) if self.predicate(os.path.join(path, entname))]
