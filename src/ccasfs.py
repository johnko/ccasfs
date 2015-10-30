'''
2015 John Ko <git@johnko.ca>
This is modified from ZipFS from PyFilesystem
"zipfs.py" Copyright (c) 2009-2015, Will McGugan <will@willmcgugan.com> and contributors.
ISCL License
'''

import datetime
import os.path
import sys
from fs.base import *
from fs.path import *
import fs.errors
import fs.remote
import tempfs
import osfs
import ccas
import ccasutil
scandir = None
try:
    scandir = os.scandir
except AttributeError:
    try:
        from scandir import scandir
    except ImportError:
        pass


class _CCASFile(object):
    """Proxies a file object and calls a callback when the file is closed."""

    def __init__(self, fs, filename, mode, handler, close_callback):
        self.fs = fs
        self.filename = filename
        self.mode = mode
        self.ccasclient = handler
        self.close_callback = close_callback

    def write(self, data):
        if 'w' in mode:
            return self.ccasclient.write(self.filename, data)
        elif 'a' in mode:
            return self.ccasclient.write_append(self.filename, data)

    def read(self, seek=0):
        return self.ccasclient.read(self.filename)

    def tell(self):
        # return self._file.tell()
        return

    def close(self):
        self.close_callback(self.filename)

    def flush(self):
        #self._file.flush()
        return

    def seek(self, offset, whence=0):
        #return self._file.seek(offset, whence)
        return

    def truncate(self, size):
        return

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class CCASFS(FS):
    """A Chunking Content Addressable Store FileSystem."""

    _meta = {'thread_safe': True,
             'virtual': False,
             'read_only': False,
             'unicode_paths': True,
             'case_insensitive_paths': False,
             'network': False,
             'atomic.setcontents': False
             }

    def __init__(self, root_path_array, manifest_path, index_path, tmp_path, write_algorithm="mirror", thread_synchronize=True, encoding='utf-8', debug=0):
        """Create a FS that maps to chunks.

        :param root_path_array: a (system) path
        :param write_algorithm: can be 'deflated' (default) to compress data or 'stored' to just store date
        :param thread_synchronize: set to True (default) to enable thread-safety

        """
        super(CCASFS, self).__init__(thread_synchronize=thread_synchronize)
        if write_algorithm in ('stripe','mirror'):
            self.write_algorithm = write_algorithm
        else:
            raise ValueError("write_algorithm should be 'mirror' (default) or 'stripe'")

        self.root_path_array = root_path_array
        self.debug = debug
        self.temp_fs = tempfs.TempFS()
        if not os.access(index_path, os.W_OK):
            os.makedirs(index_path)
        self._path_fs = osfs.OSFS(index_path) #MemoryFS()
        self.ccasmaster = ccas.CcasMaster( root_path_array, manifest_path, index_path, tmp_path, write_algorithm=self.write_algorithm, debug=self.debug )
        self.ccasclient = ccas.CcasClient(self.ccasmaster, debug=self.debug )
        #  Enable long pathnames on win32
        if sys.platform == "win32":
            if use_long_paths and not index_path.startswith("\\\\?\\"):
                if not index_path.startswith("\\"):
                    index_path = u"\\\\?\\" + index_path
                else:
                    # Explicitly mark UNC paths, seems to work better.
                    if index_path.startswith("\\\\"):
                        index_path = u"\\\\?\\UNC\\" + index_path[2:]
                    else:
                        index_path = u"\\\\?" + index_path
            #  If it points at the root of a drive, it needs a trailing slash.
            if len(index_path) == 6 and not index_path.endswith("\\"):
                index_path = index_path + "\\"

    def __str__(self):
        return "<CCASFS: %s>" % self.index_path

    def __unicode__(self):
        return u"<CCASFS: %s>" % self.index_path

    def validatepath(self, path):
        super(CCASFS, self).validatepath(path)

    def _add_resource(self, path):
        if path.endswith('/'):
            path = path[:-1]
            if path:
                self._path_fs.makedir(path, recursive=True, allow_recreate=True)
                self._path_fs.setcontents(path + '/.__ccasfs_dir__', data="git doesn't track empty dirs, so we add this file.")
        else:
            dirpath, _filename = pathsplit(path)
            if dirpath:
                self._path_fs.makedir(dirpath, recursive=True, allow_recreate=True)
            f = self._path_fs.open(path, 'w')
            f.close()

    def close(self):
        pass

    def setcontents(self, path, data, chunk_size=64*1024, encoding=None, errors=None, newline=None):
        self.ccasclient.write(path, data)

    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        path = normpath(relpath(path))
        if 'r' in mode and self.ccasclient.exists(path):
            # print "read %s" % mode
            pass
        if 'w' in mode:
            # print "write %s" % mode
            dirname, _filename = pathsplit(path)
            if dirname:
                self.temp_fs.makedir(dirname, recursive=True, allow_recreate=True)
            self._add_resource(path)
        f = _CCASFile(self.temp_fs, path, mode, self.ccasclient, self._on_write_close)
        return f

    def getcontents(self, path, mode="r", encoding=None, errors=None, newline=None):
        if not self.exists(path):
            raise fs.errors.ResourceNotFoundError(path)
        contents = self.ccasclient.read(path)
        return contents

    def _on_write_close(self, filename):
        # TODO notify transport layer
        return

    def isdir(self, path):
        return self._path_fs.isdir(path)

    def isfile(self, path):
        return self._path_fs.isfile(path)

    def exists(self, path):
        return self._path_fs.exists(path)

    def makedir(self, dirname, recursive=False, allow_recreate=False):
        dirname = normpath(dirname)
        if not dirname.endswith('/'):
            dirname += '/'
        self._add_resource(dirname)

    def removedir(self, path, recursive=False, force=False):
        #  Don't remove the root directory of this FS
        if path in ('', '/'):
            raise RemoveRootError(path)
        sys_path = self._path_fs.getsyspath(path)
        fn = self._path_fs.getsyspath(path, '.__ccasfs_dir__')
        if os.path.isfile(fn):
            os.remove(fn)
        if force:
            # shutil implementation handles concurrency better
            shutil.rmtree(sys_path, ignore_errors=True)
        else:
            os.rmdir(sys_path)
        #  Using os.removedirs() for this can result in dirs being
        #  removed outside the root of this FS, so we recurse manually.
        if recursive:
            try:
                if dirname(path) not in ('', '/'):
                    self.removedir(dirname(path), recursive=True)
            except DirectoryNotEmptyError:
                pass

    def remove(self, path):
        sys_path = self._path_fs.getsyspath(path)
        try:
            os.remove(sys_path)
            self.ccasclient.remove(path)
        except OSError, e:
            if e.errno == errno.EACCES and sys.platform == "win32":
                # sometimes windows says this for attempts to remove a dir
                if os.path.isdir(sys_path):
                    raise ResourceInvalidError(path)
            if e.errno == errno.EPERM and sys.platform == "darwin":
                # sometimes OSX says this for attempts to remove a dir
                if os.path.isdir(sys_path):
                    raise ResourceInvalidError(path)
            raise

    def listdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        #return self._path_fs.listdir(path, wildcard, full, absolute, dirs_only, files_only)
        sys_path = self._path_fs.getsyspath(path)
        if scandir is None:
            listing = os.listdir(sys_path)
            paths = [(p) for p in listing if p!=".__ccasfs_dir__"]
            return self._listdir_helper(path, paths, wildcard, full, absolute, dirs_only, files_only)
        else:
            if dirs_only and files_only:
                raise ValueError("dirs_only and files_only can not both be True")
            # Use optimized scandir if present
            scan = scandir(sys_path)
            if dirs_only:
                paths = [(dir_entry.name) for dir_entry in scan if dir_entry.is_dir()]
            elif files_only:
                paths = [(dir_entry.name) for dir_entry in scan if dir_entry.is_file() and dir_entry!=".__ccasfs_dir__"]
            else:
                paths = [(dir_entry.name) for dir_entry in scan and dir_entry!=".__ccasfs_dir__"]

            return self._listdir_helper(path, paths, wildcard, full, absolute, False, False)

    def rename(self, src, dst):
        path_dst = self._path_fs.getsyspath(dst)
        try:
            self._path_fs.rename(src, dst)
            self.ccasclient.rename(src, dst)
        except OSError, e:
            if e.errno:
                #  POSIX rename() can rename over an empty directory but gives
                #  ENOTEMPTY if the dir has contents.  Raise UnsupportedError
                #  instead of DirectoryEmptyError in this case.
                if e.errno == errno.ENOTEMPTY:
                    raise UnsupportedError("rename")
                #  Linux (at least) gives ENOENT when trying to rename into
                #  a directory that doesn't exist.  We want ParentMissingError
                #  in this case.
                if e.errno == errno.ENOENT:
                    if not os.path.exists(os.path.dirname(path_dst)):
                        raise ParentDirectoryMissingError(dst)
            raise

    def _stat(self, path):
        """Stat the given path, normalising error codes."""
        try:
            return _os_stat(path)
        except ResourceInvalidError:
            raise fs.errors.ResourceNotFoundError(path)

    def getmeta(self, meta_name, default=NoDefaultMeta):
        if meta_name == 'free_space':
            if platform.system() == 'Windows':
                try:
                    import ctypes
                    free_bytes = ctypes.c_ulonglong(0)
                    ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(self.index_path), None, None, ctypes.pointer(free_bytes))
                    return free_bytes.value
                except ImportError:
                    # Fall through to call the base class
                    pass
            else:
                stat = os.statvfs(self.index_path)
                return stat.f_bfree * stat.f_bsize
        elif meta_name == 'total_space':
            if platform.system() == 'Windows':
                try:
                    import ctypes
                    total_bytes = ctypes.c_ulonglong(0)
                    ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(self.index_path), None, ctypes.pointer(total_bytes), None)
                    return total_bytes.value
                except ImportError:
                    # Fall through to call the base class
                    pass
            else:
                stat = os.statvfs(self.index_path)
                return stat.f_blocks * stat.f_bsize
        return super(CCASFS, self).getmeta(meta_name, default)

    def getinfo(self, path):
        if not self.exists(path):
            raise fs.errors.ResourceNotFoundError(path)
        fn = self._path_fs.getsyspath(path)
        info = self._stat(fn)
        info['size'] = info['st_size']
        #  TODO: this doesn't actually mean 'creation time' on unix
        fromtimestamp = datetime.datetime.fromtimestamp
        ct = info.get('st_ctime', None)
        if ct is not None:
            info['created_time'] = fromtimestamp(ct)
        at = info.get('st_atime', None)
        if at is not None:
            info['accessed_time'] = fromtimestamp(at)
        mt = info.get('st_mtime', None)
        if mt is not None:
            info['modified_time'] = fromtimestamp(mt)
        return info

    def getinfokeys(self, path, *keys):
        info = {}
        fn = self._path_fs.getsyspath(path)
        stats = self._stat(fn)
        fromtimestamp = datetime.datetime.fromtimestamp
        for key in keys:
            try:
                if key == 'size':
                    info[key] = stats.st_size
                elif key == 'modified_time':
                    info[key] = fromtimestamp(stats.st_mtime)
                elif key == 'created_time':
                    info[key] = fromtimestamp(stats.st_ctime)
                elif key == 'accessed_time':
                    info[key] = fromtimestamp(stats.st_atime)
                else:
                    info[key] = getattr(stats, key)
            except AttributeError:
                continue
        return info

    def getsize(self, path):
        return self._stat(path).st_size

def _os_stat(path):
    """Replacement for os.stat that raises FSError subclasses."""
    st_size = None
    stats = os.lstat(path)
    info = dict((k, getattr(stats, k)) for k in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
    torrent = ccasutil.read_torrent(path)
    if torrent:
        torrent_info = torrent['info']
        if torrent_info:
            st_size = torrent_info['length']
        else:
            st_size = 0
    else:
        st_size = 0
    if st_size is not None:
        info['st_size'] = st_size
    info['size'] = info['st_size']
    return info
