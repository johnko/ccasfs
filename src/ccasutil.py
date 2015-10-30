'''
Copyright (c) 2015, John Ko
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''

import hashlib
import os
import uuid
import libtorrent

def hashdata(data):
    return hashlib.sha256(data).hexdigest()

def hashdepthwidth(digest, width=2, depth=4):
    return [digest[start:start+width] for start in range(0, depth*width, width)]

def make_torrent(torrent_path, data_path):
    fs = libtorrent.file_storage()
    libtorrent.add_files(fs, data_path)
    t = libtorrent.create_torrent(fs)
    t.set_creator("ccas")
    libtorrent.set_piece_hashes(t, os.path.dirname(data_path))
    tdata = t.generate()
    if not os.access(os.path.dirname(torrent_path), os.W_OK):
        os.makedirs(os.path.dirname(torrent_path))
    with open(torrent_path, 'wb') as f:
        f.write(libtorrent.bencode(tdata))
    return

def write_torrent(torrent_path, data, tmp_path):
    tmp_data_path = os.path.join(tmp_path, uuid.uuid4().hex)
    if not os.access(os.path.dirname(tmp_data_path), os.W_OK):
        os.makedirs(os.path.dirname(tmp_data_path))
    with open(tmp_data_path, 'wb') as f:
        f.write(data)
    make_torrent(torrent_path, tmp_data_path)
    os.remove(tmp_data_path)
    return

def read_torrent(path):
    if os.path.isfile(path):
        with open(path, 'rb') as f:
            return libtorrent.bdecode(f.read())

def main():
    # good idea to test via command line
    test = hashdata('Test')
    print test
    print hashdepthwidth(test)

if __name__ == "__main__":
    main()
