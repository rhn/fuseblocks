import os
from subprocess import Popen, PIPE

class EasyStat:
    pass

def match_picture(filename):
    return filename.endswith('.py')

def get_pictures(dirname):
    return filter(match_picture, os.listdir(dirname))

def get_abs_pictures(dirname):
    return (os.path.join(dirname, x) for x in get_pictures(dirname))

class Metadata:
    def __init__(self, fd_tracker):
        self.open_fds = {}
        self.fd_tracker = fd_tracker
    
    def get_zipname(self, path):
        return os.path.basename(path.rstrip(os.path.sep)) + '_all.zip'
    
    def get_tarname(self, path):
        return os.path.basename(path.rstrip(os.path.sep)) + '_all.tar'
    
    def get_vfiles(self, path):
        return [self.get_zipname(path), self.get_tarname(path)]
    
    def virt_getattr(self, path, fh=None):
        ret = EasyStat
        
        ret.st_gid = os.getgid()
        ret.st_uid = os.getuid()
        
        st = os.st
        ret.st_mode = st.S_IFREG | st.S_IRUSR | st.S_IRGRP
        #ret.st_size = 0
        
        dirname = os.path.dirname(path)
        dirstat = os.lstat(dirname)
        ret.st_atime = dirstat.st_atime
        ret.st_ctime = dirstat.st_ctime
        
        picstats = list(map(os.lstat, get_abs_pictures(dirname)))
        ret.st_mtime = max(map(lambda x: x.st_mtime, picstats))
        
        ret.st_nlink = 1
        ret.st_dev = 0
        return ret

    def get_fddata(self, path, handle):
        dirname, filename = os.path.split(path)
        if self.get_zipname(dirname) == filename:
            archtype = 'zip'
        elif self.get_tarname(dirname) == filename:
            archtype = 'tar'
        else:
            return None
            
        if handle is None:
            new_handle = self.fd_tracker.new()
            print("-----NEW", path, new_handle)
            fddata = FDMetadata(path=path, arch=archtype, fd_handle=new_handle)
            key = (path, new_handle)
            assert key not in self.open_fds
            return self.open_fds.setdefault(key, fddata) # will always insert the new item - key guaranteed unique
        else:
            print("-----GET", path, handle)
            return self.open_fds[(path, handle)]
    
    def read_archive_next(self, fddata, size):
        if fddata.offset == 0:
            dirname = os.path.dirname(fddata.path)
            if fddata.arch == 'tar':
                cmd = ['tar', '-C', dirname, '-c', '-m', '--numeric-owner', '--group=0', '--owner=0']
                filenames = get_pictures(dirname)
            elif fddata.arch == 'zip':
                cmd = ['zip', '-', '-j', '-X', '--compression-method', 'store']
                filenames = get_abs_pictures(dirname)
            else:
                raise RuntimeError
            cmd.extend(filenames)
            fddata.archiver_handle = Popen(cmd, stdout=PIPE)
        ret = fddata.archiver_handle.stdout.read(size)
        fddata.offset += len(ret)
        return ret
        
    def close(self, fddata):
        if fddata.archiver_handle is not None:
            fddata.archiver_handle.kill()
            fddata.archiver_handle.wait(100)
        del self.open_fds[(fddata.path, fddata.fd_handle)]
        self.fd_tracker.discard(fddata.fd_handle)
        return 0


class FDMetadata:
    def __init__(self, path, arch, fd_handle):
        self.path = path
        self.offset = 0
        self.arch = arch
        self.archiver_handle = None
        self.fd_handle = fd_handle

    def get_handle(self):
        return self.fd_handle
