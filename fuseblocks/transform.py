import os.path
import errno
from abc import ABCMeta, abstractmethod
from fuse import FuseOSError
from .base import Backend
from .passthrough import PassthroughBackend


def path_decoded(func):
    def method(self, path, *args, **kwargs):
        return func(self, self.decode_path(path), *args, **kwargs)
    return method

def pass_back_dec(func_name):
    @path_decoded
    def method(self, path, *args, **kwargs):
        return getattr(self.backend, func_name)(path, *args, **kwargs)
    return method

class TransformNameBackend(Backend, metaclass=ABCMeta):
    def __init__(self, mount_dir, transformed_backend):
        Backend.__init__(self, mount_dir)
        self.backend = transformed_backend
        self.path_decodes = {}
    
    access = pass_back_dec('access')
    getattr = pass_back_dec('getattr')
    open = pass_back_dec('open')
    readlink = pass_back_dec('readlink')
    statvfs = pass_back_dec('statvfs')

    def readdir(self, enc_path):
        dec_path = self.decode_path(enc_path)
        for dec_entry in self.backend.readdir(dec_path):
            enc_entry = self._encode_name_save(enc_path, dec_path, dec_entry)
            if enc_entry is not None:
                yield enc_entry

    def _encode_name_save(self, enc_path, dec_path, dec_entry):
        enc_name = self.encode_name(dec_path, dec_entry)
        if enc_name is None:
            return None
        
        self.path_decodes[os.path.join(enc_path, enc_name)] = os.path.join(dec_path, dec_entry)
        return enc_name

    @abstractmethod
    def encode_name(self, dec_path, dec_name):
        """May not raise errors, return None if has no mapping.
        """
        pass
        
    def decode_path(self, path):
        if path not in self.path_decodes:
            base, entry = os.path.split(path)
            if base == path: # root
                self._encode_name_save(base, base, '')
            else:
                self.readdir(base) # populate entries
        try:
            return self.path_decodes[path]
        except KeyError:
            raise FuseOSError(errno.ENOENT)
