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


"""Transparently converts WAV files to FLAC."""


class ObjectMapper(LoggingMixIn, fuseblocks.ObjectMapper): pass


class FLACFile(fuseblocks.stream.ReadOnlyProcess):
    def get_cmd(self, path):
        """The actual command"""
        return ['flac', '--best', '--verify', '--stdout', path]

class FLACProcessor(fuseblocks.stream.ProcessBlock):
    OpenFile = FLACFile

class FLACRenameBackend(fuseblocks.transform.TransformNameBlock):
    def encode_name(self, dec_path, dec_name):
        extname = '.wav'
        if not fbutil.isdir(self.backend, os.path.join(dec_path, dec_name)) and dec_name.lower().endswith(extname):
            return dec_name[:-len(extname)] + '.flac'
        else:
            return dec_name


if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    backend = FLACRenameBackend(FLACProcessor(argv[1]))
    fuse = FUSE(ObjectMapper(argv[2], backend), argv[2], direct_io=True, foreground=True)


