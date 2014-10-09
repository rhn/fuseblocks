import errno
import os.path
from fuse import FuseOSError
from .base import Backend, OpenFile


class DumbFile(OpenFile):
    def __init__(self, path, flags):
        self.fd = os.open(path, flags)
    
    def read(self, size, offset):
        os.lseek(self.fd, offset, 0)
        return os.read(self.fd, size)
        

class PassthroughBackend(Backend):
    OpenFile = DumbFile
    
    def __init__(self, mount_dir, base_dir):
        Backend.__init__(self, mount_dir)
        self.base_dir = base_dir
    
    def _get_base_path(self, relpath):
        return self.base_dir + relpath
    _gbp = _get_base_path
    
    def access(self, path, mode):
        if os.access(self._gbp(path), mode):
            return 0
        else:
            raise FuseOSError(errno.EACCES)

    def getattr(self, path):
        st = os.stat(self._gbp(path))
        return dict((key, getattr(st, key))
                    for key in
                        filter(lambda x: x.startswith('st_'), dir(st)))
        
    def open(self, path, flags):
        return self.OpenFile(self._gbp(path), flags)

    def readdir(self, path):
        return os.listdir(self._gbp(path))
    
    def readlink(self, path):
        return os.readlink(self._gbp(path))
    
    def statvfs(self, path):
        stv = os.statvfs(self._gbp(path))
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))
