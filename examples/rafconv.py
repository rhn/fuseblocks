#!/usr/bin/env python3

import os
from sys import argv, exit
import errno

from fuse import FUSE, LoggingMixIn, FuseOSError

import fuseblocks
from fuseblocks import start_fuse
from fuseblocks.transform import ProcessFileByEndingBlock
import fuseblocks.stream

import logging
logging.basicConfig(level=logging.DEBUG)


"""Transparently converts RAF files, leaving other files intact."""

class ObjectMapper(LoggingMixIn, fuseblocks.ObjectMapper): pass


class RAFFile(fuseblocks.stream.ReadOnlyProcess):
    def get_cmd(self, path):
        """The actual command"""
        return ['ufraw-batch', '--out-type=jpg', '--shrink=2', '--output=-', path]


class RAFFileProcessor(fuseblocks.stream.RawProcessBlockFS):
    OpenFile = RAFFile

#class EagerRAFProcessor(fuseblocks.stream.EagerProcessBlock):
 #   OpenFile = RAFFile

class ProcessUFRAW(fuseblocks.stream.ProcessBlockFS):
    def __init__(self, backend):
        fuseblocks.stream.ProcessBlockFS.__init__(self,
            RAFFileProcessor(backend))

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    source_dir = argv[1]
    base_block = fuseblocks.DirectoryBlock(source_dir)
    ending_conversions = [('.raf', '.jpg', False, ProcessUFRAW(base_block))]
    
    backend = ProcessFileByEndingBlock(base_block, ending_conversions)
    fuseblocks.start_fuse(backend, argv[2], direct_io=True, foreground=True, mapper_class=ObjectMapper)
