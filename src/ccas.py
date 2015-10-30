'''
2015 John Ko <git@johnko.ca>
This extends "gfs.py"
No license was provided for "gfs.py" from which this file is based on.
'''
import os
import time
import operator
import ccasutil
from gfs import GFSClient, GFSMaster, GFSChunkserver

def read_in_chunks(file_object, chunk_size=1024):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

class CcasClient(GFSClient):
    def __init__(self, master, debug=0):
        self.debug = debug
        self.master = master

    def setcontents(self, filename, f, op=None):
        if op == 'append' and not self.exists(filename):
            raise Exception("append error, file does not exist: %s" % filename)
        chunkservers = self.master.get_chunkservers()
        chunkuuids = []
        for chunk in read_in_chunks(f, self.master.chunksize):
            write_copies = 0
            chunkuuid, write_copies = self.write_one_chunk(chunk, chunkservers)
            if chunkuuid is not None and write_copies > 0:
                chunkuuids.append(chunkuuid)
            else:
                raise Exception("FAULTED: Chunk %s failed to write anywhere." % (chunkuuid))
        if len(chunkuuids) > 0:
            if op == 'append':
                # TODO appended metadata like file size in a torrent
                self.master.alloc_append(filename, chunkuuids)
            elif op == 'write':
                self.master.alloc(filename, chunkuuids)
        return

    def write(self, filename, data): # filename is full namespace path
        if self.exists(filename): # if already exists, overwrite
            self.delete(filename)
        # track metadata like file size in a torrent
        if filename.startswith('/'): filename = filename[1:]
        local_filename = os.path.join(self.master.index_path, filename)
        torrent_info_path = ccasutil.write_torrent(local_filename, data, self.master.tmp_path)
        self.master.write_catalog(filename, torrent_info_path)
        chunkuuids = self.write_chunks(data)
        self.master.alloc(filename, chunkuuids)

    def write_one_chunk(self, chunk, chunkservers):
        write_copies = 0
        chunkuuid = ccasutil.hashdata(chunk)
        chunkloc = self.master.new_chunkloc(chunkuuid)
        if self.master.write_algorithm == 'stripe':
            while not chunkservers[chunkloc].enabled:
                chunkloc = self.master.new_chunkloc(chunkuuid)
            resp = chunkservers[chunkloc].write(chunkuuid, chunk)
            if resp is not None:
                write_copies += 1
            else:
                if self.debug > 0: print "Failed to write %s%s, consider checking the disk." % (chunkservers[chunkloc].local_filesystem_root, chunkuuid)
                for i in chunkservers:
                    # retry on another chunkserver but let master decide the location
                    # retryloc = i
                    retryloc = self.master.new_chunkloc(chunkuuid)
                    while not chunkservers[retryloc].enabled:
                        retryloc = self.master.new_chunkloc(chunkuuid)
                    resp = chunkservers[retryloc].write(chunkuuid, chunk)
                    if resp is not None:
                        print "Rewrote to %s%s." % (chunkservers[retryloc].local_filesystem_root, chunkuuid)
                        write_copies += 1
                        chunkuuids.append(chunkuuid)
                        break
                    else:
                        if self.debug > 0: print "Failed to write %s%s, consider checking the disk." % (chunkservers[retryloc].local_filesystem_root, chunkuuid)
        elif self.master.write_algorithm == 'mirror':
            for j in range(0, len(chunkservers)):
                if chunkservers[j].enabled:
                    resp = chunkservers[j].write(chunkuuid, chunk)
                    if resp is not None:
                        write_copies += 1
                    else:
                        if self.debug > 0: print "Failed to write a copy to %s%s, consider checking the disk." % (chunkservers[j].local_filesystem_root, chunkuuid)
        if write_copies < 1:
            raise Exception("FAULTED: Chunk %s failed to write anywhere." % (chunkuuid))
        return chunkuuid, write_copies


    def write_chunks(self, data):
        chunks = [ data[x:x+self.master.chunksize] \
            for x in range(0, len(data), self.master.chunksize) ]
        chunkservers = self.master.get_chunkservers()
        chunkuuids = []
        for i in range(0, len(chunks)):
            chunkuuid, write_copies = self.write_one_chunk(chunks[i], chunkservers)
            if chunkuuid is not None and write_copies > 0:
                chunkuuids.append(chunkuuid)
            else:
                raise Exception("FAULTED: Chunk %s failed to write anywhere." % (chunkuuid))
        return chunkuuids

    def num_chunks(self, size):
        return (size // self.master.chunksize) \
            + (1 if size % self.master.chunksize > 0 else 0)

    def write_append(self, filename, data):
        if not self.exists(filename):
            raise Exception("append error, file does not exist: %s" % filename)
        append_chunkuuids = self.write_chunks(data)
        # TODO appended metadata like file size in a torrent
        self.master.alloc_append(filename, append_chunkuuids)

    def exists(self, filename):
        return self.master.exists(filename)

    def read(self, filename, length=None):
        # TODO file-like
        pass

    def read_all(self, filename, length=None): # get metadata, then read chunks direct
        if not self.exists(filename):
            raise Exception("read error, file does not exist: %s" % filename)
        chunks = []
        chunkuuids = self.master.get_chunkuuids(filename)
        chunkservers = self.master.get_chunkservers()
        for chunkuuid in chunkuuids:
            read_copies = 0
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunk = chunkservers[chunkloc].read(chunkuuid)
            # verify data
            if chunk is not None:
                if chunkuuid == ccasutil.hashdata(chunk):
                    read_copies += 1
            else:
                if self.debug > 0: print "Chunk %s%s failed verification, consider checking the disk." % (chunkservers[chunkloc].local_filesystem_root, chunkuuid)
                for i in chunkservers:
                    # retry on another chunkserver but let master decide the location
                    # retryloc = i
                    retryloc = self.master.get_retryloc(chunkuuid)
                    while not chunkservers[retryloc].enabled:
                        retryloc = self.master.get_retryloc(chunkuuid)
                    chunk = chunkservers[retryloc].read(chunkuuid)
                    if chunk is not None:
                        if chunkuuid == ccasutil.hashdata(chunk):
                            if self.debug > 0: print "Found a good copy at %s%s." % (chunkservers[retryloc].local_filesystem_root, chunkuuid)
                            read_copies += 1
                            chunks.append(chunk)
                            break
                        else:
                            if self.debug > 0: print "Chunk %s%s failed verification, consider checking the disk." % (chunkservers[retryloc].local_filesystem_root, chunkuuid)
            if read_copies > 0:
                chunks.append(chunk)
            else:
                raise Exception("FAULTED: Chunk %s failed to verify anywhere." % (chunkuuid))
        data = reduce(lambda x, y: x + y, chunks) # reassemble in order
        return data

    def delete(self, filename):
        self.master.delete(filename)


class CcasMaster(GFSMaster):
    def __init__(self, root_path_array, manifest_path, index_path, catalog_path, tmp_path, write_algorithm='mirror', chunksize=10, debug=0):
        self.debug = debug
        self.num_chunkservers = len(root_path_array) # number of disks
        self.root_path_array = root_path_array
        if write_algorithm in ('stripe','mirror'):
            self.write_algorithm = write_algorithm # stripe, mirror...
        else:
            self.write_algorithm = 'mirror' # default to mirror for data safety
        self.manifest_path = manifest_path
        self.catalog_path = catalog_path
        self.index_path = index_path
        self.tmp_path = tmp_path
        self.chunksize = chunksize
        self.chunkrobin = 0
        self.chunkservers = {} # loc id to chunkserver mapping
        self.init_chunkservers()

    def init_chunkservers(self):
        for i in range(0, self.num_chunkservers):
            chunkserver = CcasChunkserver(self.root_path_array[i], debug=self.debug)
            self.chunkservers[i] = chunkserver
        return

    def get_chunkservers(self):
        return self.chunkservers

    def alloc(self, filename, chunkuuids): # save to manifest
        self.write_manifest(filename, chunkuuids)
        return

    def alloc_append(self, filename, append_chunkuuids): # append chunks
        chunkuuids = self.read_manifest(filename)
        chunkuuids.extend(append_chunkuuids)
        self.write_manifest(filename, chunkuuids)
        return

    def cycle_chunkrobin(self):
        self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers

    def new_chunkloc(self, chunkuuid):
        while not self.chunkservers[self.chunkrobin].enabled:
            self.cycle_chunkrobin()
        maybe_new = self.chunkrobin
        self.cycle_chunkrobin()
        return maybe_new

    def get_retryloc(self, chunkuuid):
        while not self.chunkservers[self.chunkrobin].enabled:
            self.cycle_chunkrobin()
        maybe_mirror = self.chunkrobin
        self.cycle_chunkrobin()
        return maybe_mirror

    def get_chunkloc(self, chunkuuid):
        while not self.chunkservers[self.chunkrobin].enabled:
            self.cycle_chunkrobin()
        maybe_original = self.chunkrobin
        self.cycle_chunkrobin()
        return maybe_original

    def get_chunkuuids(self, filename):
        return self.read_manifest(filename)

    def exists(self, filename):
        if filename.startswith('/'): filename = filename[1:]
        local_filename = os.path.join(self.manifest_path, filename)
        return os.path.exists(local_filename)

    def rename(self, old_path, new_path):
        if old_path.startswith('/'): old_path = old_path[1:]
        local_old_filename = os.path.join(self.manifest_path, old_path)
        if new_path.startswith('/'): new_path = new_path[1:]
        local_new_filename = os.path.join(self.manifest_path, new_path)
        if not os.access(os.path.dirname(local_new_filename), os.W_OK):
            os.makedirs(os.path.dirname(local_new_filename))
        os.rename(local_old_filename, local_new_filename)

    def delete(self, filename): # rename for later garbage collection
        # chunkuuids = self.read_manifest(filename)
        iso = time.strftime('%Y%m%dT%H%M%SZ')
        timestamp = repr(time.time())
        if self.debug > 0: print "CcasMaster.delete: iso %s" % iso
        if self.debug > 0: print "CcasMaster.delete: timestamp %s" % timestamp
        if self.debug > 0: print "CcasMaster.delete: filename %s" % filename
        old_path = filename
        if old_path.startswith('/'): old_path = old_path[1:]
        deleted_filename = os.path.join( 'hidden', 'deleted', iso, timestamp, old_path)
        if self.debug > 0: print "CcasMaster.delete: deleted_filename %s" % deleted_filename
        # self.write_manifest(deleted_filename, chunkuuids)
        self.rename(filename, deleted_filename)
        if self.debug > 0: print "deleted file: %s renamed to %s ready for gc" % (filename, deleted_filename)

    def dump_metadata(self):
        print "Chunkservers: ", len(self.chunkservers)

    def write_catalog(self, filename, torrent_info_path): # save to catalog
        if self.debug > 0: print "write_catalog: %s" % (filename)
        if filename.startswith('/'): filename = filename[1:]
        local_filename = os.path.join(self.catalog_path, filename)
        if not os.access(os.path.dirname(local_filename), os.W_OK):
            os.makedirs(os.path.dirname(local_filename))
        shutil.copyfile(torrent_info_path, local_filename)
        os.remove(torrent_info_path)
        return

    def write_manifest(self, filename, chunkuuids):
        if self.debug > 0: print "write_manifest: %s %s" % (filename, chunkuuids)
        if filename.startswith('/'): filename = filename[1:]
        local_filename = os.path.join(self.manifest_path, filename)
        if not os.access(os.path.dirname(local_filename), os.W_OK):
            os.makedirs(os.path.dirname(local_filename))
        with open(local_filename, "w") as f:
            f.write("%s" % ("\n".join(c for c in chunkuuids)))
        return

    def read_manifest(self, filename):
        if self.debug > 0: print "read_manifest: %s" % (filename)
        if filename.startswith('/'): filename = filename[1:]
        local_filename = os.path.join(self.manifest_path, filename)
        with open(local_filename, "r") as f:
            data = f.read()
        chunkuuids = data.split("\n")
        return chunkuuids

    '''
    def save_filetable(self):
        if not os.access(self.manifest_path, os.W_OK):
            os.makedirs(self.manifest_path)
        for filename, chunkuuids in sorted(self.filetable.iteritems(), key=operator.itemgetter(1)):
            if filename.startswith('/'): filename = filename[1:]
            local_filename = os.path.join(self.manifest_path, filename)
            if not os.access(os.path.dirname(local_filename), os.W_OK):
                os.makedirs(os.path.dirname(local_filename))
            with open(local_filename, "w") as f:
                f.write("%s" % ("\n".join(c for c in chunkuuids)))
        return

    def load_filetable(self):
        if not os.access(self.manifest_path, os.W_OK):
            os.makedirs(self.manifest_path)
        for root, dirs, files in os.walk(self.manifest_path):
            for fn in files:
                fn = os.path.join(root, fn)
                filename = fn.split(self.manifest_path)[1]
                with open(fn, "r") as f:
                    data = f.read()
                chunkuuids = data.split("\n")
        self.filetable[filename] = chunkuuids
        return
    '''

class CcasChunkserver(GFSChunkserver):
    def __init__(self, root_path, debug=0):
        self.debug = debug
        self.local_filesystem_root = root_path
        if root_path is None:
            self.enabled = False
        else:
            self.enabled = True
            if not os.access(self.local_filesystem_root, os.W_OK):
                os.makedirs(self.local_filesystem_root)

    def write(self, chunkuuid, chunk):
        ''' return None on any error '''
        if not self.enabled: return None
        local_filename = self.chunk_filename(chunkuuid)
        if not os.access(os.path.dirname(local_filename), os.W_OK):
            os.makedirs(os.path.dirname(local_filename))
        # return early if the chunk already exists and we verified it
        existing_data = self.read(chunkuuid)
        if existing_data is not None:
            if chunkuuid == ccasutil.hashdata(existing_data):
                if self.debug > 1: print '200 Skipping write: Chunk %s already verified on %s' % (chunkuuid, self.local_filesystem_root)
                return 200
        try:
            with open(local_filename, "wb") as f:
                f.write(chunk)
            if self.debug > 1: print '201 Chunk written to %s%s' % (self.local_filesystem_root, chunkuuid)
            return 201
        except:
            return None

    def read(self, chunkuuid):
        ''' return None on any error '''
        if not self.enabled: return None
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        try:
            with open(local_filename, "rb") as f:
                data = f.read()
            return data
        except:
            return None

    def chunk_filename(self, chunkuuid):
        ''' return None on any error '''
        if not self.enabled: return None
        local_filename = os.path.join(self.local_filesystem_root, os.sep.join(ccasutil.hashdepthwidth(chunkuuid, width=2, depth=4)), str(chunkuuid))
        return local_filename


def main():
    # test script for filesystem

    # setup
    master = CcasMaster(
        [
            "/tmp/gfs/disk0/chunks/",
            "/tmp/gfs/disk1/chunks/",
            None,
            "/tmp/gfs/disk3/chunks/"
        ],
        "/tmp/gfs/manifest",
        "/tmp/gfs/index",
        "/tmp/gfs/tmp"
        #, write_algorithm='stripe'
        )
    client = CcasClient(master)

    # test write, exist, read
    print "\nWriting..."
    client.write("/usr/python/readme.txt", """
        This file tells you all about python that you ever wanted to know.
        Not every README is as informative as this one, but we aim to please.
        Never yet has there been so much information in so little space.
        """)
    print "File exists? ", client.exists("/usr/python/readme.txt")
    print client.read("/usr/python/readme.txt")

    # test append, read after append
    print "\nAppending..."
    client.write_append("/usr/python/readme.txt", \
        "I'm a little sentence that just snuck in at the end.\n")
    print client.read("/usr/python/readme.txt")

    # test delete
    print "\nDeleting..."
    client.delete("/usr/python/readme.txt")
    print "File exists? ", client.exists("/usr/python/readme.txt")

    # test exceptions
    print "\nTesting Exceptions..."
    try:
        client.read("/usr/python/readme.txt")
    except Exception as e:
        print "This exception should be thrown:", e
    try:
        client.write_append("/usr/python/readme.txt", "foo")
    except Exception as e:
        print "This exception should be thrown:", e

    # show structure of the filesystem
    print "\nMetadata Dump..."
    print master.dump_metadata()

if __name__ == "__main__":
    main()
