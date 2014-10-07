#!/usr/bin/env python3

from __future__ import with_statement

from errno import EACCES
import errno
from os.path import realpath
from sys import argv, exit
from threading import Lock
import itertools

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import zipfiles

import logging
logging.basicConfig(level=logging.DEBUG)

def trace(*args):
    print(*args)
    
class FDHandle:
    def __init__(self):
        self.handles = {}
    def new(self):
        for i in itertools.count(1):
            if i not in self.handles:
                self.handles[i] = True
                return i
    def discard(self, handle):
        del self.handles[handle]
        
class RealData:
    def __init__(self, fd_tracker):
        self.fd_tracker = fd_tracker
        self.open_fds = {}
        
    def reg_fd(self, real_fd):
        assert(real_fd not in self.open_fds.values())
        new_fd = self.fd_tracker.new()
        assert(new_fd not in self.open_fds.keys())
        self.open_fds[new_fd] = real_fd
        return new_fd

    def get_fd(self, fh):
        return self.open_fds[fh]
        

class Loopback(LoggingMixIn, Operations):
    def __init__(self, root, metadata, fd_tracker):
        self.root = realpath(root)
        self.rwlock = Lock()
        self.realdata = RealData(fd_tracker)
        self.metadata = metadata

    def __call__(self, op, path, *args):
        return super(Loopback, self).__call__(op, self.root + path, *args)

    def access(self, path, mode):
        if self.metadata.get_fddata(path):
            return 0
        elif not os.access(path, mode):
            raise FuseOSError(EACCES)

    def flush(self, path, fh):
        return 0

    def getattr(self, path, fh=None):
        try:
            st = os.lstat(path)
        except FileNotFoundError:
            st = self.metadata.virt_getattr(path, fh)
        return dict((key, getattr(st, key))
                    for key in
                        filter(lambda x: x.startswith('st_'), dir(st)))

    getxattr = None
    listxattr = None
    def open(self, path, flags):
        try:
            fd = os.open(path, flags)
            return self.realdata.reg_fd(fd)
        except FileNotFoundError:
            fddata = self.metadata.get_fddata(path, None)
            if fddata:
                return fddata.get_handle()
            raise FuseOSError(errno.ENOENT)

    def read(self, path, size, offset, fh):
        fddata = self.metadata.get_fddata(path, fh)
        if fddata is None:
            fd = self.realdata.get_fd(fh)
            os.lseek(fd, offset, 0)
            return os.read(fd, size)
        else:
            if offset != fddata.offset:
                raise FuseOSError(errno.ESPIPE)
            ret = self.metadata.read_archive_next(fddata, size)
            return ret

    def readdir(self, path, fh):
        all_files = os.listdir(path)
        pictures = list(filter(zipfiles.match_picture, all_files))
        entries = all_files[:]
        if pictures:
            entries.extend(self.metadata.get_vfiles(path))
        return ['.', '..'] + entries

    readlink = os.readlink

    def release(self, path, fh):
        fddata = self.metadata.get_fddata(path, fh)
        if fddata is None:
            fd = self.realdata.get_fd(fh)
            return os.close(fd)
        else:
            return self.metadata.close(fddata)

    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    utimens = os.utime

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    fd_tracker = FDHandle()
    fuse = FUSE(Loopback(argv[1], zipfiles.Metadata(fd_tracker), fd_tracker), argv[2], direct_io=True, foreground=True)
