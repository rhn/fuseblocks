import os
import sys
import subprocess
import errno
from abc import ABCMeta, abstractmethod
from fuse import FuseOSError
from .base import Block, OpenFile, VirtStat, open_direction
from .passthrough import DirectoryBlock, path_translated


class ProcessFSFile(OpenFile):
    """This file type allows passing a real file through a process and exposing the contents."""
    parent_stderr = True    # print child process stderr output to the FUSE process stderr (usually console)
    exit_timeout = 60   # timeout after which close() call will return after an unsuccessful killing
    def __init__(self, path, flags):
        if flags & os.O_APPEND:
            raise FuseOSError(errno.EACCES)
        self.set_properties(path, flags)
        self.process = self.start_process(path, flags)
    
    def set_properties(self, path, flags):
        direction = open_direction(flags)
        self.writeable = direction in (os.O_WRONLY, os.O_RDWR)
        self.readable = direction in (os.O_RDONLY, os.O_RDWR)

    def get_pipes(self):
        stdin_pipe = subprocess.PIPE if self.writeable else open('/dev/null')
        stdout_pipe = subprocess.PIPE if self.readable else open('/dev/null')
        stderr_pipe = sys.stderr if self.parent_stderr else open('/dev/null')
        return stdin_pipe, stdout_pipe, stderr_pipe
    
    def start_process(self, path, flags):
        stdin_pipe, stdout_pipe, stderr_pipe = self.get_pipes()
        cmd = self.get_cmd(path)
        self.read_offset = 0
        self.write_offset = 0
        return subprocess.Popen(cmd, stdin=stdin_pipe, stdout=stdout_pipe, stderr=stderr_pipe)
    
    def read(self, size, offset):
        if not self.readable:
            raise FuseOSError(errno.EACCES)
        if self.read_offset != offset:
            raise FuseOSError(errno.EIO)
        ret = self.process.stdout.read(size)
        
        if self.check_failed(): # check if process returned with an acceptable error code
            raise FuseOSError(errno.EIO)
        
        self.read_offset += len(ret)
        return ret
    
    def release(self):
        try:
            self.process.kill()
            self.process.wait(self.exit_timeout)
        except ProcessLookupError: # process doesn't exist already
            pass

    def check_failed(self):
        """Method checking whether the process exited with an error, producing invalid output. Override to customize acceptable error codes."""
        return bool(self.process.poll())

    @abstractmethod
    def get_cmd(path):
        """Returns a list of strings, each element being an argument to the executable"""
        pass
        

class ReadOnlyProcess(ProcessFSFile):
    def __init__(self, path, flags):
        if open_direction(flags) in (os.O_WRONLY, os.O_RDWR):
            raise FuseOSError(errno.EACCES)
        ProcessFSFile.__init__(self, path, flags)
        

class WriteOnlyProcess(ProcessFSFile):
    def __init__(self, path, flags):
        if open_direction(flags) in (os.O_RDONLY, os.O_RDWR):
            raise FuseOSError(errno.EACCES)
        ProcessFSFile.__init__(self, path, flags)


class ProcessBlockMixIn:
    """
    Block mixin that passes files on the filesystem through a process.
    It requires a real file, so it will only work with DirectoryBlock and its descendats.
    """
    OpenFile = ProcessFSFile

    def getattr(self, path):
        ret = VirtStat.from_stat(DirectoryBlock.getattr(self, path))
        ret.st_size = 0
        return ret


class CacheFile(OpenFile):
    def __init__(self, store, mode):
        self.store = store
        self.mode = mode
    
    def read(self, size, offset):
        return self.store.data[offset:offset+size]

import threading
class DataStore:
    def __init__(self):
        self.complete_lock = threading.Lock()
        self.data = None

class FileCache:
    def __init__(self, file_class):
        self.open_file = file_class
        self.data_mapping = {}
        self.mapping_lock = threading.Lock()
    
    def get_size(self, path):
        self.mapping_lock.acquire()
        try:
            if path not in self.data_mapping:
                store = DataStore()
                self.data_mapping[path] = store
                with store.complete_lock:
                    self.mapping_lock.release()
                    data = b''
                    open_file = self.open_file(path, os.O_RDONLY)
                    offset = 0
                    readsize = 2 ** 16
                    while True:
                        new_data = open_file.read(readsize, offset)
                        data += new_data
                        offset = len(data)
                        if len(new_data) < readsize:
                            break
                    store.data = data
                open_file.release()
            else:
                self.mapping_lock.release()
                store = self.data_mapping[path]
                with store.complete_lock:
                    pass
            return len(store.data)
        except:
            self.mapping_lock.release()
        
    def open(self, path, mode):
        return CacheFile(self.data_mapping[path], mode)


class EagerProcessBlock(DirectoryBlock):
    OpenFile = ProcessFSFile
    def __init__(self, root):
        DirectoryBlock.__init__(self, root)
        self.cache = FileCache(self.OpenFile)
    
    
    def getattr(self, path):
        ret = VirtStat.from_stat(DirectoryBlock.getattr(self, path))
        ret.st_size = self.cache.get_size(self._get_base_path(path))
        return ret

    @path_translated
    def open(self, path, mode):
        return self.cache.open(path, mode)
        

class ProcessBlock(ProcessBlockMixIn, DirectoryBlock):
    """Block that passes all files on the filesystem through a process."""
    pass
