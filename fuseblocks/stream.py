import os
import sys
import subprocess
import errno
from abc import ABCMeta, abstractmethod
from fuse import FuseOSError
from .base import Backend, OpenFile, VirtStat, open_direction
from .passthrough import PassthroughBackend


class ProcessFile(OpenFile):
    parent_stderr = False
    exit_timeout = 60
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
            raise FuseOSError(errno.EACCES)
        ret = self.process.stdout.read(size)
        self.read_offset += len(ret)
        return ret
    
    def release(self):
        self.process.kill()
        self.process.wait(self.exit_timeout)

    @abstractmethod
    def get_cmd(path):
        """Returns a list of strings, each element being an argument to the executable"""
        pass
        

class ReadOnlyProcess(ProcessFile):
    def __init__(self, path, flags):
        if open_direction(flags) in (os.O_WRONLY, os.O_RDWR):
            raise FuseOSError(errno.EACCES)
        ProcessFile.__init__(self, path, flags)
        

class WriteOnlyProcess(ProcessFile):
    def __init__(self, path, flags):
        if open_direction(flags) in (os.O_RDONLY, os.O_RDWR):
            raise FuseOSError(errno.EACCES)
        ProcessFile.__init__(self, path, flags)


class ProcessBackend(PassthroughBackend):
    OpenFile = ProcessFile

    def getattr(self, path):
        ret = VirtStat.from_stat(PassthroughBackend.getattr(self, path))
        ret.st_size = 0
        return ret
