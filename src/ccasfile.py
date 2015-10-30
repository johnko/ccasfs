'''
2015 John Ko <git@johnko.ca>
This is modified from ZipFS and fs.remote from PyFilesystem
"zipfs.py" Copyright (c) 2009-2015, Will McGugan <will@willmcgugan.com> and contributors.
ISCL License
'''

from fs.errors import FSError
from fs.remote import RemoteFileBuffer
from fs.filelike import StringIO, SpooledTemporaryFile, FileWrapper
from fs import SEEK_SET, SEEK_CUR, SEEK_END

class _CCASFile(RemoteFileBuffer):

    max_size_in_memory = 1024 * 64

    def __init__(self, fs, filename, mode, handler, close_callback, write_on_flush=True, debug=0):
        self.debug = debug
        self.fs = fs
        self.filename = filename
        self.mode = mode
        self.ccasclient = handler
        self.close_callback = close_callback
        self.write_on_flush = write_on_flush
        self.offset = 0
        self.op = None
        wrapped_file = SpooledTemporaryFile(max_size=self.max_size_in_memory)
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
                self.op = 'write'
                self._changed = True
                self._eof = True
        else:
            # Do not use remote file object
            self.op = 'write'
            self._eof = True
            self._changed = True
        super(RemoteFileBuffer,self).__init__(wrapped_file,mode)
        # FIXME: What if mode with position on eof?
        if "a" in mode:
            # Not good enough...
            self.seek(0, SEEK_END)

    def __del__(self):
        #  Don't try to close a partially-constructed file
        if "_lock" in self.__dict__:
            if not self.closed:
                try:
                    self.close()
                except FSError:
                    pass

    def _write(self,data,flushing=False):
        if self.debug > 0: print "_CCASFile.write %s" % self.mode
        with self._lock:
            toread = len(data) - (self._readlen - self.wrapped_file.tell())
            if toread > 0:
                if not self._eof:
                    self._fillbuffer(toread)
                else:
                    self._readlen += toread
            self._changed = True
            self.wrapped_file.write(data)

    def _read_remote(self, length=None):
        """Read data from the remote file into the local buffer."""
        chunklen = 1024 * 256
        bytes_read = 0
        while True:
            toread = chunklen
            if length is not None and length - bytes_read < chunklen:
                toread = length - bytes_read
            if not toread:
                break
            #data = self._rfile.read(toread)
            data = self.ccasclient.read(self.filename)
            datalen = len(data)
            if not datalen:
                self._eof = True
                break
            bytes_read += datalen
            self.wrapped_file.write(data)
            if datalen < toread:
                # We reached EOF,
                # no more reads needed
                self._eof = True
                break
        #if self._eof:
        #    self._rfile.close()
        self._readlen += bytes_read

    def _fillbuffer(self, length=None):
        """Fill the local buffer, leaving file position unchanged.
        This method is used for on-demand loading of data from the remote file
        into the buffer.  It reads 'length' bytes from rfile and writes them
        into the buffer, seeking back to the original file position.
        """
        curpos = self.wrapped_file.tell()
        if length == None:
            if not self._eof:
                # Read all data and we didn't reached EOF
                # Merge endpos - tell + bytes from rfile
                self.wrapped_file.seek(0, SEEK_END)
                self._read_remote()
                self._eof = True
                self.wrapped_file.seek(curpos)
        elif not self._eof:
            if curpos + length > self._readlen:
                # Read all data and we didn't reached EOF
                # Load endpos - tell() + len bytes from rfile
                toload = length - (self._readlen - curpos)
                self.wrapped_file.seek(0, SEEK_END)
                self._read_remote(toload)
                self.wrapped_file.seek(curpos)

    def _read(self, length=None):
        if self.debug > 0: print "_CCASFile.read %s" % self.mode
        if length is not None and length < 0:
            length = None
        with self._lock:
            self._fillbuffer(length)
            data = self.wrapped_file.read(length if length != None else -1)
            # data = self.ccasclient.read(self.filename, length if length != None else -1)
            if not data:
                data = None
            return data

    def _seek(self,offset,whence=SEEK_SET):
        self.offset = offset
        if self.debug > 0: print "_CCASFile.seek %i %i" % (offset, whence)
        with self._lock:
            if not self._eof:
                # Count absolute position of seeking
                if whence == SEEK_SET:
                    abspos = offset
                elif whence == SEEK_CUR:
                    abspos =  offset + self.wrapped_file.tell()
                elif whence == SEEK_END:
                    abspos = None
                else:
                    raise IOError(EINVAL, 'Invalid whence')
                if abspos != None:
                    toread = abspos - self._readlen
                    if toread > 0:
                        self.wrapped_file.seek(self._readlen)
                        self._fillbuffer(toread)
                else:
                    self.wrapped_file.seek(self._readlen)
                    self._fillbuffer()
            self.wrapped_file.seek(offset, whence)

    def _truncate(self,size):
        if size == self.offset + 1:
            self.op = 'append'
        if self.debug > 0: print "_CCASFile.truncate %i" % size
        with self._lock:
            if not self._eof and self._readlen < size:
                # Read the rest of file
                self._fillbuffer(size - self._readlen)
                # Lock rfile
                self._eof = True
            elif self._readlen >= size:
                # Crop rfile metadata
                self._readlen = size if size != None else 0
                # Lock rfile
                self._eof = True

            self.wrapped_file.truncate(size)
            self._changed = True

            self.flush()
            #if self._rfile is not None:
            #    self._rfile.close()

    def flush(self):
        if self.debug > 0: print "_CCASFile.flush"
        with self._lock:
            self.wrapped_file.flush()
            if self.write_on_flush:
                self._setcontents()

    def _setcontents(self):
        if self.debug > 0: print "_CCASFile._setcontents"
        if not self._changed:
            # Nothing changed, no need to write data back
            return
        # If not all data loaded, load until eof
        if not self._eof:
            self._fillbuffer()
        if "w" in self.mode or "a" in self.mode or "+" in self.mode:
            pos = self.wrapped_file.tell()
            self.wrapped_file.seek(0)
            self.ccasclient.setcontents(self.filename, self.wrapped_file, op=self.op)
            self.wrapped_file.seek(pos)

    def close(self):
        if self.debug > 0: print "_CCASFile.close"
        with self._lock:
            if not self.closed:
                self._setcontents()
                #if self._rfile is not None:
                #    self._rfile.close()
                super(RemoteFileBuffer,self).close()
        self.close_callback(self.filename)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
