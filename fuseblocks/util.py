import os.path

def isdir(backend, path):
    return os.path.stat.S_ISDIR(backend.getattr(path).st_mode)
