import os.path
import errno
from abc import ABCMeta, abstractmethod
from fuse import FuseOSError
from .base import Block
from .passthrough import Passthrough


def approved(func):
    def method(self, path, *args, **kwargs):
        if not self.is_accessible(path):
            raise FuseOSError(errno.ENOENT)
        return func(self, path, *args, **kwargs)
    return method


class FilterBlock(Passthrough, metaclass=ABCMeta):
    @approved
    def readdir(self, path):
        return [entname for entname in self.backend.readdir(path) if self.is_accessible(os.path.join(path, entname))]

    def _apply_method(self, func_name, path, *args, **kwargs):
        """Override this to alter behaviour."""
        if not self.is_accessible(path):
            raise FuseOSError(errno.ENOENT)
        return getattr(self.backend, func_name)(path, *args, **kwargs)

    @abstractmethod
    def is_accessible(self, path): pass
