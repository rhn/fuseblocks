#!/usr/bin/env python3

import os
from sys import argv, exit
import errno

from fuse import FUSE, LoggingMixIn, FuseOSError

import fuseblocks
import fuseblocks.transform
import fuseblocks.stream

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

class FLACRenameBlock(fuseblocks.transform.FileEndingChangeBlock):
    ending_conversions = [('.wav', '.flac', True)]
    

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    backend = FLACRenameBlock(FLACProcessor(argv[1]))
    fuseblocks.start_fuse(backend, argv[2], direct_io=True, foreground=True, mapper_class=ObjectMapper)


