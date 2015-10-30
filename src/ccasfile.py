'''
2015 John Ko <git@johnko.ca>
This is modified from ZipFS and fs.remote from PyFilesystem
"zipfs.py" Copyright (c) 2009-2015, Will McGugan <will@willmcgugan.com> and contributors.
ISCL License
'''

from fs import SEEK_SET, SEEK_CUR, SEEK_END
from fs.remote import RemoteFileBuffer

class _CCASFile(RemoteFileBuffer):
    """Proxies a file object and calls a callback when the file is closed."""

    def __init__(self, fs, filename, mode, handler, close_callback, debug=0):
        self.debug = debug
        self.fs = fs
        self.filename = filename
        self.mode = mode
        self.ccasclient = handler
        self.close_callback = close_callback
        self._changed = False
        self._readlen = 0  # How many bytes already loaded from rfile
        self._eof = False  # Reached end of rfile?
        if getattr(fs, "_lock", None) is not None:
            self._lock = fs._lock.__class__()
        else:
            self._lock = threading.RLock()
        if "r" in mode or "+" in mode or "a" in mode:
            if not self.ccasclient.exists(self.filename):
                # File was just created, force to write anything
                self._changed = True
                self._eof = True
        else:
            # Do not use remote file object
            self._eof = True
            self._changed = True

    def write(self, data):
        if self.debug > 0: print "_CCASFile.write %s" % self.mode
        with self._lock:
            self._changed = True
            if not self.ccasclient.exists(self.filename):
                return self.ccasclient.write(self.filename, data)
            else:
                return self.ccasclient.write_append(self.filename, data)

    def read(self, length=None):
        if self.debug > 0: print "_CCASFile.read %s" % self.mode
        if length is not None and length < 0:
            length = None
        with self._lock:
            data = self.ccasclient.read(self.filename, length if length != None else -1)
            if not data:
                data = None
            return data

    def tell(self):
        # return self._file.tell()
        if self.debug > 0: print "_CCASFile.tell"
        return

    def close(self):
        if self.debug > 0: print "_CCASFile.close"
        self.close_callback(self.filename)

    def flush(self):
        if self.debug > 0: print "_CCASFile.flush"
        #self._file.flush()
        return

    def seek(self, offset, whence=SEEK_SET):
        if self.debug > 0: print "_CCASFile.seek %i %i" % (offset, whence)
        # TODO handle seek to turn on self.append
        return

    def truncate(self, size):
        if self.debug > 0: print "_CCASFile.truncate %i" % size
        # TODO handle seek to turn on self.append
        with self._lock:
            if not self._eof and self._readlen < size:
                # Lock rfile
                self._eof = True
            elif self._readlen >= size:
                # Crop rfile metadata
                self._readlen = size if size != None else 0
                # Lock rfile
                self._eof = True
            # self.ccasclient.truncate(size)
            self._changed = True

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
