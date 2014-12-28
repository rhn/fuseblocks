#!/usr/bin/env python3

import os.path
from sys import argv
import errno

from fuse import FUSE, LoggingMixIn, FuseOSError

import fuseblocks
from fuseblocks import start_fuse
import fuseblocks.filter

import logging
logging.basicConfig(level=logging.DEBUG)


"""Filters out files that are not pictures, and "o" directories, and empty directories."""


def is_picture_extension(path):
    for ext in ['.jpg', '.png', '.tiff', '.tif', '.jpeg']:
        if path.lower().endswith(ext):
            return True
    return False


class FilterExtension(fuseblocks.filter.FilterBlock):
    def is_accessible(self, path):
        isdir = os.path.stat.S_ISDIR(self.backend.getattr(path).st_mode)
        if not isdir:
            return is_picture_extension(path)
        if os.path.basename(path) == 'o':
            return False
        return len(self.backend.readdir(path)) > 0

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    backend = FilterExtension(fuseblocks.realfs.DirectoryBlock(argv[1]))
    fuseblocks.start_fuse(backend, argv[2], direct_io=True, foreground=True)

