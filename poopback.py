#!/usr/bin/env python3

from os.path import realpath
from sys import argv, exit

from fuse import FUSE, LoggingMixIn

import fuseblocks
import fuseblocks.passthrough

import logging
logging.basicConfig(level=logging.DEBUG)

class ObjectMapper(LoggingMixIn, fuseblocks.ObjectMapper): pass

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    backend = fuseblocks.passthrough.PassthroughBackend(argv[2], argv[1])
    fuse = FUSE(ObjectMapper(argv[2], backend), argv[2], direct_io=True, foreground=True)
