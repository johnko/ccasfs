'''
2015 John Ko
This mounts a CCASFS with FUSE
'''

from ccasfs import CCASFS
from logging import DEBUG, INFO, ERROR, CRITICAL
import fs
from fs.expose import fuse

logger = fs.getLogger('fs.ccasfs')
logger.setLevel(DEBUG)

ccasfs = CCASFS( [
            "/scratch/ccasfs/chunks/"
        ],
        "/scratch/ccasfs/meta/manifest",
        "/scratch/ccasfs/meta/index",
        "/scratch/ccasfs/meta/catalog",
        "/scratch/ccasfs/tmp",
        write_algorithm="mirror",
        debug=2)

mountpoint = fuse.mount(ccasfs, "/mnt", foreground=True, fsname="ccasfs")
