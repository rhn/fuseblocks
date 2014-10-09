import errno
from fuse import FuseOSError
from .base import Backend
from .passthrough import PassthroughBackend


def preapproved(func_name):
    def method(self, path, *args, **kwargs):
        if not self.predicate(path):
            raise FuseOSError(errno.ENOENT)
        return getattr(self.backend, func_name)(path, *args, **kwargs)
    return method


class FilterBackend(Backend):
    def __init__(self, mount_dir, base_dir):
        Backend.__init__(self, mount_dir)
        self.backend = PassthroughBackend(mount_dir, base_dir)
    
    access = preapproved('access')
    getattr = preapproved('getattr')
    open = preapproved('open')
    readdir = preapproved('readdir')
    readlink = preapproved('readlink')
    statvfs = preapproved('statvfs')
