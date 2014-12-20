import os.path
import errno
from fuse import FuseOSError
from .base import Block


def pass_to_backend(func_name):
    def method(self, *args, **kwargs):
        return self.apply_method(func_name, *args, **kwargs)
    return method


class OverlayBlock(Block):
    def __init__(self, parent, overlay):
        Block.__init__(self)
        self.parent = parent
        self.overlay = overlay
    
    access = pass_to_backend('access')
    getattr = pass_to_backend('getattr')
    open = pass_to_backend('open')
    readlink = pass_to_backend('readlink')
    statvfs = pass_to_backend('statvfs')
    
    def readdir(self, path):
        return list(frozenset(self.parent.readdir(path)).union(frozenset(self.overlay.readdir(path))))
    
    def apply_method(self, func_name, path, *args, **kwargs):
        print('APPLY')
        ovf = getattr(self.overlay, func_name)
        try:
            return ovf(path, *args, **kwargs)
        except FuseOSError as e:
            if e.errno != errno.ENOENT:
                raise e
        return getattr(self.parent, func_name)(path, *args, **kwargs)


