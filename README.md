# ccasfs
Chunked Content Addressable Storage or "Experiments with file deduplication on low RAM systems"

## Dependencies

```
pkg install -y \
    python27 libffi indexinfo gettext-runtime py27-setuptools27 \
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
python  ccasfs/ccasfs-fuse.py

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
