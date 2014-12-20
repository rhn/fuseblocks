#!/usr/bin/env python3

from os.path import realpath
from sys import argv, exit

from fuse import FUSE, LoggingMixIn

import fuseblocks
import fuseblocks.virtual

import logging
logging.basicConfig(level=logging.DEBUG)


"""An overlay filesystem example."""


class ObjectMapper(LoggingMixIn, fuseblocks.ObjectMapper): pass

if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <root> <overlay> <mountpoint>' % argv[0])
        exit(1)

    backend = fuseblocks.DirectoryBlock(argv[1])
    overlay = fuseblocks.DirectoryBlock(argv[2])
    merge = fuseblocks.virtual.OverlayBlock(backend, overlay)
    fuseblocks.start_fuse(merge, argv[3], direct_io=True, foreground=True, mapper_class=ObjectMapper)
