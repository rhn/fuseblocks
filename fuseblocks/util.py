import os.path

def isdir(block, path):
    return os.path.stat.S_ISDIR(block.getattr(path).st_mode)

from fuse import FUSE
from .base import ObjectMapper

def start_fuse(block, mount_directory, *args, mapper_class=ObjectMapper, **kwargs):
    return FUSE(mapper_class(mount_directory, block), mount_directory, **kwargs)
