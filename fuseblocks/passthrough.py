import os.path
import errno
from fuse import FuseOSError
from .base import Block


def pass_to_backend(func_name):
    def method(self, *args, **kwargs):
        return self._apply_method(func_name, *args, **kwargs)
    return method


class Passthrough(Block):
    REALFS_RESOLVE = True
    def __init__(self, backend):
        Block.__init__(self)
        self.backend = backend
        if self.REALFS_RESOLVE and hasattr(backend, '_get_base_path'): # poke a hole through the abstraction to see if file has an underlying FS file
            self._get_base_path = lambda path: self._apply_method('_get_base_path', path)

    access = pass_to_backend('access')
    getattr = pass_to_backend('getattr')
    open = pass_to_backend('open')
    readlink = pass_to_backend('readlink')
    statvfs = pass_to_backend('statvfs')
    readdir = pass_to_backend('readdir')

    def _apply_method(self, func_name, path, *args, **kwargs):
        """Override this to alter behaviour."""
        return getattr(self.backend, func_name)(path, *args, **kwargs)

class OverlayBlock(Passthrough):
    def __init__(self, base, overlay):
        Passthrough.__init__(self, base)
        self.overlay = overlay
    
    def readdir(self, path):
        def get_entries(source, path):
            try:
                return source.readdir(path)
            except FuseOSError as e:
                if e.errno != errno.ENOENT:
                    raise e
                return []
        
        return list(frozenset(get_entries(self.backend, path)) \
                    .union(get_entries(self.overlay, path)))
    
    def _apply_method(self, func_name, path, *args, **kwargs):
        ovf = getattr(self.overlay, func_name)
        try:
            return ovf(path, *args, **kwargs)
        except FuseOSError as e:
            if e.errno != errno.ENOENT:
                raise e
        return Passthrough._apply_method(self, func_name, path, *args, **kwargs)
