import os.path
import errno
from abc import ABCMeta, abstractmethod
from fuse import FuseOSError
from . import util
from .base import Block
from .passthrough import DirectoryBlock


def pass_back_dec(func_name):
    def method(self, *args, **kwargs):
        return self.pass_to_backend(func_name, *args, **kwargs)
    return method

class TransformNameBlock(Block, metaclass=ABCMeta):
    def __init__(self, parent_block):
        Block.__init__(self)
        self.backend = parent_block
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

    def pass_to_backend(self, method_name, enc_path, *args, **kwargs):
        method = getattr(self.backend, method_name)
        return method(self.decode_path(enc_path), *args, **kwargs)

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


class FileNameChangeBlock(TransformNameBlock, metaclass=ABCMeta):
    """Changes file names."""
    def encode_name(self, dec_path, dec_name):
        if util.isdir(self.backend, os.path.join(dec_path, dec_name)): # ignore directories
            return dec_name
        return self.encode_file_name(dec_name)
    
    @abstractmethod
    def encode_file_name(self, dec_name): pass


class FileEndingChangeBlock(FileNameChangeBlock):
    """Changes arbitrary file ending to a different one (doesn't need to be dot-separated extension). Directories are ignored.
    Requires ending_changes array to be filled.
    ending_conversions needs to be filled in with tuples (from, to, case_sensitive), processed in order.
    e.g.
    ending_conversions = [("_Africa.jpg", "_Vacation.jpg", True),
                          (".wav", ".baz", False),
                          ("jpg", "png", True)] # this will not work for files ending with "_Africa.jpg" which were matched earlier.
    """
    ending_conversions = []
    def encode_file_name(self, dec_name):
        dec_name_low = dec_name.lower()
        for from_ending, to_ending, case_sensitive in self.ending_conversions:
            if case_sensitive:
                from_ending = from_ending.lower()
            if dec_name_low.endswith(from_ending):
                return dec_name[:-len(from_ending)] + to_ending
        return dec_name # file not in ending list


def pass_back_dec(func_name):
    def method(self, path, *args, **kwargs):
        dec_path, backend = self.get_backend(path)
        return getattr(backend, func_name)(path, *args, **kwargs)
    return method


class ProcessFileByEndingBlock(TransformNameBlock):
    """Pipes files through a backend while at the same time changing its ending."""
    def __init__(self, parent_block, conversions):
        """Parent block is the backing block for all transformations. It is used for all directory handling and for handling files without any conversion.
        All other file handling goes through block specified in conversion. These blocks must accept the same file names as parent_block.
        ending_conversions = [('.raf', '.png', False, ChangeFileContentsBlock(directory))]
        """
        FileNameChangeBlock.__init__(self, parent_block)
        self.parent_block = parent_block
        self.ending_conversions = conversions
        
    def find_conversion(self, dec_name):
        dec_name_low = dec_name.lower()
        for conversion in self.ending_conversions:
            from_ending, to_ending, case_sensitive, backend = conversion
            if case_sensitive:
                from_ending = from_ending.lower()
            if dec_name_low.endswith(from_ending):
                return conversion
        return None # file not in ending list

    def get_path_conversion(self, path):
        if util.isdir(self.backend, path):
            return None
        else:
            return self.find_conversion(os.path.basename(path))

    def get_backend(self, path):
        conv = self.get_path_conversion(path)
        if conv is None:
            return self.backend
        from_ending, to_ending, case_sensitive, backend = conv
        return backend

    def pass_to_backend(self, method_name, path, *args, **kwargs):
        dec_path = self.decode_path(path)
        method = getattr(self.get_backend(dec_path), method_name)
        return method(dec_path, *args, **kwargs)
    
    def encode_name(self, dec_path, dec_name):
        conversion = self.get_path_conversion(os.path.join(dec_path, dec_name))
        if conversion is None:
            return dec_name
        from_ending, to_ending, case_sensitive, backend = conversion
        return dec_name[:-len(from_ending)] + to_ending
