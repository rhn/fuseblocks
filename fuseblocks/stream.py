import os
import sys
import subprocess
import errno
from abc import ABCMeta, abstractmethod
from fuse import FuseOSError
from .base import Block, OpenFile, VirtStat, open_direction
from .realfs import DirectoryBlock, path_translated
from .passthrough import Passthrough
from .cache import DataCacheBlock


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


class RawProcessBlockFS(Passthrough):
    """
    Block mixin that passes files on the filesystem through a process.
    It requires a real file, so it will only work with unbroken chains to DirectoryBlock.
    """
    OpenFile = ProcessFSFile
    def getattr(self, path):
        ret = VirtStat.from_stat(os.stat(self._get_base_path(path)))
        ret.st_size = 0
        return ret

    def open(self, path, flags):
        return self.OpenFile(self._get_base_path(path), flags)


class ProcessBlockFS(Passthrough):
    """Block that passes all files backed by the filesystem through a processing block, and caches them.
    Wraps RawProcessBlockFS.
    """
    def __init__(self, process_backend):
        Passthrough.__init__(self, VerifySizeBlock(
                                   DataCacheBlock(
                                   process_backend)))


def read_file_size(open_file):
    offset = 0
    readsize = 2 ** 16
    while True:
        new_data = open_file.read(readsize, offset)
        offset += len(new_data)
        if len(new_data) < readsize:
            break
    return offset
    

class VerifySizeBlock(Passthrough):
    """On first request for the file size (getattr, directory listing), reads the whole file and displays its size.
    Useful only for read-only files.
    """
    def __init__(self, backend):
        Passthrough.__init__(self, backend)
        self.sizes = {}
    
    def getattr(self, path):
        ret = VirtStat.from_stat(Passthrough.getattr(self, path))
        if os.path.stat.S_ISDIR(ret.st_mode):
            return ret
        if path not in self.sizes:
            open_file = self.open(path, os.O_RDONLY)
            self.sizes[path] = read_file_size(open_file)
            open_file.release()
        ret.st_size = self.sizes[path]
        return ret
