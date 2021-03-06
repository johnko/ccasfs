# ccasfs
Chunked Content Addressable Storage or "Experiments with file deduplication on low RAM systems"

# Status

## ** NOT READY **

** DO NOT USE FOR PRODUCTION **

_ IT WILL TRASH DATA _

## Dependencies

```
pkg install -y \
    python27 libffi indexinfo gettext-runtime py27-setuptools27 \
    py27-libtorrent-rasterbar libtorrent-rasterbar libiconv boost-python-libs boost-libs icu GeoIP \
    py27-fs \
    fusefs-libs
kldload fuse
```

## Install

```
git clone https://github.com/johnko/ccasfs.git ccasfs
```

## Usage

```
python2.7  ccasfs/src/ccasfs-fuse.py

cp  ~/a_test_file.txt  /mnt
ls -l  /mnt
cat  /mnt/a_test_file.txt

# files are split into chunksize and stored here
find  /scratch/ccasfs/chunks/

# file manifests are here, the paths are real filenames, and the contents are chunk id/hash in order
find  /scratch/ccasfs/meta/manifest

# file index is here (when walking the /mnt, it actually just reads the torrent info here)
find  /scratch/ccasfs/meta/index
```

## Known Issues

- Reading data doesn't work because of not-implemente ccasclient.read_chunk(length).
- Appending might not work.
- Appending does not update the size or time because we didn't recreate a torrent index for it.
- Actually, file info doesn't work either, because the torrent index is not created.
- Need to move the catalog to hashdepthwidth.

## What appears to work

- Writing a file (data is chunked and flushed on file close).

Patches welcome!
