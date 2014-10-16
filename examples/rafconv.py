#!/usr/bin/env python3

import os
from sys import argv, exit
import errno

from fuse import FUSE, LoggingMixIn, FuseOSError

import fuseblocks
import fuseblocks.transform
import fuseblocks.stream
from fuseblocks import util as fbutil

import logging
logging.basicConfig(level=logging.DEBUG)


"""Transparently converts RAF files."""


class ObjectMapper(LoggingMixIn, fuseblocks.ObjectMapper): pass


class RAFFile(fuseblocks.stream.ReadOnlyProcess):
    def get_cmd(self, path):
        """The actual command"""
        return ['ufraw-batch', '--out-type=png', '--output=-', path]

class RAFProcessor(fuseblocks.stream.ProcessBackend):
    OpenFile = RAFFile

class RAFRenameBackend(fuseblocks.transform.TransformNameBackend):
    def encode_name(self, dec_path, dec_name):
        extname = '.raf'
        if not fbutil.isdir(self.backend, os.path.join(dec_path, dec_name)) and dec_name.lower().endswith(extname):
            return dec_name[:-len(extname)] + '.png'
        else:
            return dec_name


if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    backend = RAFRenameBackend(argv[2], RAFProcessor(argv[2], argv[1]))
    fuse = FUSE(ObjectMapper(argv[2], backend), argv[2], direct_io=True, foreground=True)


