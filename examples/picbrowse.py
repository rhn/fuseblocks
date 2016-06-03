#!/usr/bin/env python3

import os.path
from sys import argv
import errno

from fuse import FUSE, LoggingMixIn, FuseOSError

import fuseblocks
from fuseblocks import start_fuse
import fuseblocks.filter
import fuseblocks.stream
import fuseblocks.cache

import logging
logging.basicConfig(level=logging.DEBUG)


"""Filters out files that are not pictures, and "o" directories, and empty directories. Automatically converts from ufraw files in `o` directories.
"""


def is_picture_extension(path):
    for ext in ['.jpg', '.png', '.tiff', '.tif', '.jpeg', '.mp4', '.avi']:
        if path.lower().endswith(ext):
            return True
    return False


class RAFFile(fuseblocks.stream.ReadOnlyProcess):
    #out_type = 'png'
    out_type = 'jpg'
    def get_cmd(self, path):
        """The actual command"""
        return ['ufraw-batch', '--out-type={}'.format(self.out_type), '--create-id=no', '--output=-', path]


class RAFFileProcessor(fuseblocks.stream.RawProcessBlockFS):
    OpenFile = RAFFile
    source_ending = '.ufraw'
    destination_ending = '.jpg'
    def _transform_path(self, path):
        isdir = os.path.stat.S_ISDIR(self.backend.getattr(path).st_mode)
        if isdir or not path.endswith(self.source_ending):
            return path
        return path[:-len(self.source_ending)] + self.destination_ending
    
    def _transform_back(self, path):
        if not path.endswith(self.destination_ending):
            return path
        real_path = path[:-len(self.destination_ending)] + self.source_ending
        try:
            self.backend.getattr(real_path)
        except FuseOSError as e:
            if e.errno == errno.ENOENT:
                return path
        return real_path

    def readdir(self, path):
        return (os.path.basename(self._transform_path(os.path.join(path, entname))) for entname
                in fuseblocks.stream.RawProcessBlockFS.readdir(self, path))

    def _apply_method(self, func_name, path, *args, **kwargs):
        return fuseblocks.stream.RawProcessBlockFS._apply_method(self, func_name, self._transform_back(path), *args, **kwargs)


class ProcessUFRAW(fuseblocks.stream.ProcessBlockFS):
    def __init__(self, backend):
        fuseblocks.stream.ProcessBlockFS.__init__(self,
            RAFFileProcessor(backend))

'''
Ideally, this exposes underlying FS as another FUSE tree at a different mount point for programs which can't take streams for input.

class ProcessUFRAW(fuseblocks.passthrough.Passthrough):
    def __init__(self, underlying):
        tempmount = underlying
        fuseblocks.passthrough.Passthrough.__init__(self, 
'''

class Rename(fuseblocks.passthrough.Passthrough):
    def readdir(self, path, *args, **kwargs):
        path = os.path.join(path, 'o')
        return self._apply_method('readdir', path, *args, **kwargs)

    def _apply_method(self, func_name, path, *args, **kwargs):
        if func_name != 'readdir':
            base, filename = os.path.split(path)
            req_path = os.path.join(base, 'o', filename)
            try:
                self.backend.getattr(req_path)
            except FuseOSError as e:
                if e.errno != errno.ENOENT:
                    raise e
            else:
                path = req_path
        return fuseblocks.passthrough.Passthrough._apply_method(self, func_name, path, *args, **kwargs)


class FilterUFRAW(fuseblocks.filter.FilterBlock):
    def is_accessible(self, path):
        isdir = os.path.stat.S_ISDIR(self.backend.getattr(path).st_mode)
        if isdir:
            return True
        return path.endswith('.ufraw')


class FilterPictures(fuseblocks.filter.FilterBlock):
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
        
    real_fs = fuseblocks.realfs.DirectoryBlock(argv[1])
    processed_raws = ProcessUFRAW(Rename(FilterUFRAW(real_fs)))
    filtered_fs = FilterPictures(real_fs)
    backend = fuseblocks.passthrough.OverlayBlock(filtered_fs, processed_raws)
    fuseblocks.start_fuse(backend, argv[2], direct_io=True, foreground=True)

