import errno
import os.path
from fuse import FuseOSError
from .base import Block, OpenFile, BlockException


class FSFile(OpenFile):
    """File object that accesses a file on a host filesystem."""
    def __init__(self, path, flags):
        self.fd = os.open(path, flags)
    
    def read(self, size, offset):
        os.lseek(self.fd, offset, 0)
        return os.read(self.fd, size)


def path_translated(method):
    def decorated(self, path, *args, **kwargs):
        return method(self, self._get_base_path(path), *args, **kwargs)
    return decorated


class DirectoryBlock(Block):
    """This block accesses a directory on a host filesystem."""
    OpenFile = FSFile
    
    def __init__(self, base_dir):
        Block.__init__(self)
        if not os.path.isdir(base_dir):
            raise BlockException("Not a directory, can't use for backend: {!r}".format(base_dir))
        self.base_dir = base_dir
    
    def _get_base_path(self, relpath):
        return self.base_dir + relpath
    
    @path_translated
    def access(self, path, mode):
        if os.access(path, mode):
            return 0
        else:
            raise FuseOSError(errno.EACCES)

    @path_translated
    def getattr(self, path):
        try:
            return os.stat(path)
        except OSError as e:
            raise FuseOSError(e.errno) from e
    
    @path_translated
    def open(self, path, flags):
        return self.OpenFile(path, flags)

    @path_translated
    def readdir(self, path):
        return os.listdir(path)
    
    @path_translated
    def readlink(self, path):
        return os.readlink(path)
    
    @path_translated
    def statvfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))
