#!/usr/bin/env python3

import re
import time

from fuse import FUSE, LoggingMixIn

import fuseblocks
from fuseblocks.passthrough import Passthrough
from fuseblocks import fs_cache

import logging
logging.basicConfig(level=logging.INFO)


"""A read-only filesystem logging calls to a selected path - example."""

class ObjectMapper(LoggingMixIn, fuseblocks.ObjectMapper): pass


class LogBlock(Passthrough):
    def __init__(self, backend, paths):
        self.matches = [re.compile(path) for path in paths]
        Passthrough.__init__(self, backend)

    def _apply_method(self, func_name, path, *args, **kwargs):
        def log_call(func):
            logging.info("{func_name}({path}) {args} {kwargs}"
                         .format(func_name=func_name,
                                 path=path,
                                 args=args, kwargs=kwargs))
            try:
                ret = func()
            except Exception as e:
                logging.info("{func_name}({path}) ! {e}"
                             .format(func_name=func_name,
                                     path=path, e=e))
                raise
            logging.info("{func_name}({path}) -> {ret}"
                         .format(func_name=func_name,
                                 path=path, ret=ret))
            return ret

        def real_call():
            return getattr(self.backend, func_name)(path, *args, **kwargs)
        call = real_call
        for match in self.matches:
            if match.search(path) is not None:
                def call():
                    print("calling")
                    return log_call(real_call)
                break
        return call()


class DataCache(fs_cache.DataCache):
    CACHE_PATH = '/home/rhn/play/fuseblocks/cache'


class SlowTransform(Passthrough):
    def getattr(self, path):
        return Passthrough.getattr(self, path)

    def open(self, path, flags):
        print("Open - simulate cache miss!")
        time.sleep(2)
        return Passthrough.open(self, path, flags)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Analyze calls requiring access to data (as opposed to metadata).')
    parser.add_argument('source', help="Directory with data source")
    parser.add_argument('mountpoint', help="Mount destination")
    parser.add_argument('path_re', nargs='+', help="Display detailed logs for files matching this regex")
    args = parser.parse_args()

    source = fuseblocks.DirectoryBlock(args.source)
    transform = SlowTransform(source)
    logger = LogBlock(transform, args.path_re)
    cache = DataCache(logger)
    fuseblocks.start_fuse(cache, args.mountpoint, direct_io=False, foreground=True, mapper_class=ObjectMapper)
