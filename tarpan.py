#!/usr/bin/env python3

import os
from os.path import realpath
from sys import argv, exit

from fuse import FUSE, LoggingMixIn

import fuseblocks
import fuseblocks.stream

import logging
logging.basicConfig(level=logging.DEBUG)

class ObjectMapper(LoggingMixIn, fuseblocks.ObjectMapper): pass


class CatFile(fuseblocks.stream.ReadOnlyProcess):
    def get_cmd(self, path):
        return ['cat', path]


class CatProcessor(fuseblocks.stream.ProcessBackend):
    OpenFile = CatFile

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    backend = CatProcessor(argv[2], argv[1])
    fuse = FUSE(ObjectMapper(argv[2], backend), argv[2], direct_io=True, foreground=True)


