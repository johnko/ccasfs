# ccasfs
Chunked Content Addressable Storage or "Experiments with file deduplication on low RAM systems"

## Usage

```
python ccasfs-fuse.py
```

## Dependencies

```
pkg install -y \
    python27 libffi indexinfo gettext-runtime py27-setuptools27 \
    py27-fs \
    py27-six \
    fusefs-libs
kldload fuse
```
