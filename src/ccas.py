'''
This extends gfs.py
'''
import hashlib
import os
import time
import operator
from gfs import GFSClient, GFSMaster, GFSChunkserver

class VerifyException(Exception):
    pass

def hashdata(data):
    return hashlib.sha256(data).hexdigest()

class CcasClient(GFSClient):
    def __init__(self, master):
        self.master = master

    def write(self, filename, data): # filename is full namespace path
        if self.exists(filename): # if already exists, overwrite
            self.delete(filename)
        # num_chunks = self.num_chunks(len(data))
        chunkuuids = self.write_chunks(data)
        self.master.alloc(filename, chunkuuids)

    def write_chunks(self, data):
        chunks = [ data[x:x+self.master.chunksize] \
            for x in range(0, len(data), self.master.chunksize) ]
        chunkservers = self.master.get_chunkservers()
        chunkuuids = []
        for i in range(0, len(chunks)):
            chunkuuid = hashdata(chunks[i])
            chunkloc = self.master.new_chunkloc(chunkuuid)
            if self.master.algorithm == 'stripe':
                chunkservers[chunkloc].write(chunkuuid, chunks[i])
            elif self.master.algorithm == 'mirror':
                for j in range(0, len(chunkservers)):
                    chunkservers[j].write(chunkuuid, chunks[i])
            chunkuuids.append(chunkuuid)
        return chunkuuids

    def num_chunks(self, size):
        return (size // self.master.chunksize) \
            + (1 if size % self.master.chunksize > 0 else 0)

    def write_append(self, filename, data):
        if not self.exists(filename):
            raise Exception("append error, file does not exist: " \
                 + filename)
        append_chunkuuids = self.write_chunks(data)
        # num_append_chunks = self.num_chunks(len(data))
        self.master.alloc_append(filename, \
            append_chunkuuids)

    def exists(self, filename):
        return self.master.exists(filename)

    def read(self, filename): # get metadata, then read chunks direct
        if not self.exists(filename):
            raise Exception("read error, file does not exist: " \
                + filename)
        chunks = []
        chunkuuids = self.master.get_chunkuuids(filename)
        chunkservers = self.master.get_chunkservers()
        for chunkuuid in chunkuuids:
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunk = chunkservers[chunkloc].read(chunkuuid)
            # verify data
            if chunkuuid != hashdata(chunk):
                if self.master.algorithm == 'mirror':
                    print ("Chunk %s%s failed verification, consider checking the disk." % (chunkservers[chunkloc].local_filesystem_root, chunkuuid))
                    for i in chunkservers:
                        # retry on another chunkserver but let master decide the location
                        # retryloc = i
                        retryloc = self.master.get_retryloc(chunkuuid)
                        chunk = chunkservers[retryloc].read(chunkuuid)
                        if chunkuuid == hashdata(chunk):
                            print ("Found a good copy at %s%s." % (chunkservers[retryloc].local_filesystem_root, chunkuuid))
                            chunks.append(chunk)
                            break
                    if chunkuuid != hashdata(chunk):
                        raise VerifyException("FAULTED: Chunk %s%s failed verification." % (chunkservers[retryloc].local_filesystem_root, chunkuuid))
                else:
                    raise VerifyException("FAULTED: Chunk %s%s failed verification." % (chunkservers[chunkloc].local_filesystem_root, chunkuuid))
            else:
                chunks.append(chunk)
        data = reduce(lambda x, y: x + y, chunks) # reassemble in order
        return data

    def delete(self, filename):
        self.master.delete(filename)


class CcasMaster(GFSMaster):
    def __init__(self, root_path_array, algorithm='mirror'):
        self.num_chunkservers = len(root_path_array) # number of disks
        self.root_path_array = root_path_array
        if algorithm in ('stripe','mirror'):
            self.algorithm = algorithm # stripe, mirror...
        else:
            self.algorithm = 'mirror' # default to mirror for data safety
        self.chunksize = 10
        self.chunkrobin = 0
        self.filetable = {} # file to chunk mapping
        self.chunktable = {} # chunkuuid to chunkloc mapping
        self.chunkservers = {} # loc id to chunkserver mapping
        self.init_chunkservers()

    def init_chunkservers(self):
        for i in range(0, self.num_chunkservers):
            chunkserver = CcasChunkserver(self.root_path_array[i])
            self.chunkservers[i] = chunkserver
        return

    def get_chunkservers(self):
        return self.chunkservers

    def alloc(self, filename, chunkuuids): # return ordered chunkuuid list
        self.filetable[filename] = chunkuuids
        return

    def alloc_append(self, filename, append_chunkuuids): # append chunks
        chunkuuids = self.filetable[filename]
        chunkuuids.extend(append_chunkuuids)
        self.filetable[filename] = chunkuuids
        return

    def new_chunkloc(self, chunkuuid):
        '''
        assign first to be fast, then round robin cycle after
        '''
        self.chunktable[chunkuuid] = self.chunkrobin
        self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
        return self.chunktable[chunkuuid]

    def get_retryloc(self, chunkuuid):
        maybe_mirror = self.chunkrobin
        self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
        return maybe_mirror

    def get_chunkloc(self, chunkuuid):
        return self.chunktable[chunkuuid]

    def get_chunkuuids(self, filename):
        return self.filetable[filename]

    def exists(self, filename):
        return True if filename in self.filetable else False

    def delete(self, filename): # rename for later garbage collection
        chunkuuids = self.filetable[filename]
        del self.filetable[filename]
        timestamp = repr(time.time())
        deleted_filename = "/hidden/deleted/" + timestamp + filename
        self.filetable[deleted_filename] = chunkuuids
        print "deleted file: " + filename + " renamed to " + \
             deleted_filename + " ready for gc"

    def dump_metadata(self):
        print "Filetable:",
        for filename, chunkuuids in self.filetable.items():
            print filename, "with", len(chunkuuids),"chunks"
        print "Chunkservers: ", len(self.chunkservers)
        print "Chunkserver Data:"
        for chunkuuid, chunkloc in sorted(self.chunktable.iteritems(), key=operator.itemgetter(1)):
            chunk = self.chunkservers[chunkloc].read(chunkuuid)
            print chunkloc, chunkuuid, chunk


class CcasChunkserver(GFSChunkserver):
    def __init__(self, root_path):
        self.chunktable = {}
        self.local_filesystem_root = root_path
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def write(self, chunkuuid, chunk):
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "w") as f:
            f.write(chunk)
        self.chunktable[chunkuuid] = local_filename

    def read(self, chunkuuid):
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        try:
            with open(local_filename, "r") as f:
                data = f.read()
            return data
        except:
            return None

    def chunk_filename(self, chunkuuid):
        local_filename = self.local_filesystem_root + "/" \
            + str(chunkuuid)
        return local_filename


def main():
    # test script for filesystem

    # setup
    master = CcasMaster(
        [
            "/tmp/gfs/disk0/chunks/",
            "/tmp/gfs/disk1/chunks/",
            "/tmp/gfs/disk2/chunks/",
            "/tmp/gfs/disk3/chunks/"
        ]
        #, algorithm='stripe'
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
