#!/usr/bin/env python3

import os
from sys import argv, exit
import errno

from fuse import FUSE, LoggingMixIn, FuseOSError

from fuseblocks import start_fuse
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


class RAFProcessor(fuseblocks.stream.ProcessBlock):
    OpenFile = RAFFile


class RAFRenameBlock(fuseblocks.transform.TransformNameBlock):
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

    backend = RAFRenameBlock(RAFProcessor(argv[1]))
    fuseblocks.start_fuse(backend, argv[2], direct_io=True, foreground=True, mapper_class=ObjectMapper)
