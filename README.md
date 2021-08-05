zookeeper-fuse
==============
The zookeeper-fuse project is a libfuse compatible userspace filesystem backended by zookeeper. It allows a user to mount the entire zoo or a specific node within the zoo as a filesystem.
[![Build Status](https://travis-ci.com/smok-serwis/zookeeper-fuse.svg)](https://travis-ci.com/smok-serwis/zookeeper-fuse)

Features
--------

  * Mount the entire zoo or a subset
  * Writes to the filesystem gets synched to the zoo
  * Reads from the filesystem are not cached
  * Supports authentication

Building
--------

* autoreconf -fi
* ./configure
* make
* make install

For development using Ubuntu 16.04 (Xenial Xerus)
Required Packages for compilation:
 - libzookeeper-mt-dev
 - libfuse-dev
 - zookeeper
 - libboost-filesystem-dev
 - libbsd-dev

Optional Packages:
 - liblog4cpp5-dev

Zookeeper Package:
 - zookeeperd

Useful packages for testing and debugging:
 - zooinspector


Mounting
--------

```bash
zookeper-fuse /mnt/zoo -- --zooHosts localhost:2181
```

Limitations
-----------
* Date of last access and modification are not kept track of
* Chmods are 755 for directories and 777 for files
    * unless you're using a leaf display mode of HYBRID, there it's all 777
* Displaying Leaf Nodes: In the Zookeeper, even directories can have contents. An aspect which is difficult to represent within the constraints of a fuse filesystem. As such, two leaf display modes are supported: DIR and FILE. In both modes the contents of directories are stored in special "_zoo_data_" files. The differences between the display modes are as follows:
    1. DIR: Display all leaf nodes as directories, has the side-effect that new files can only be created using mkdir.
    2. FILE: Display all leaf nodes as files, has the side-effect that directories cannot be created.
    3. HYBRID: Read more below.
* mv is not yet implemented in any leaf display mode
    * mv is implemented in HYBRID mode, but only for files, not for directories
* cp is not fully supported (unless you use the HYBRID mode)
    * When leaf display mode is FILE, files can be copied accurately but directories aren't
    * When leaf display mode is DIR, nodes are created but contents aren't copied

Hybrid mode
===========

The problem with current DIR and FILE mode is that zookeeper-fuse forgets what it has created
right after it's creation, which poses some problems when trying to mount a ZooKeeper directory
as a filesystem.

HYBRID mode makes use of caching in order to remember whether the file you just
created is a normal file, or a directory, permitting usage of ZooKeeper more like a standard filesystem.

If a file is not in cache, then following rules will apply:

* if it's `/` then it's a directory
* if it has any children, it's a directory
* if it has any data, it's a file
* if it's an empty childless node, it's a directory, so beware of touch

Note that *cache will be enabled ONLY in hybrid mode*.

However, this applies just to a single machine running zookeeper-fuse from the same volume, if a single machine is
to create an empty file, it will be still visible to another machine as a directory, so take care of that.

However, until cache eviction is implemented, your instance of zookeeper-fuse might grow unbounded in memory,
especially *when you process files with different names*.

Note that in hybrid mode data contained in nodes that have children is just flat out inaccessible.

If you keep on processing files with the same names, it should be OK.

But tl;dr - ZooKeeper systems are usable as a normal filesystem in HYBRID mode.

In general, it's enough for certbot to put certificates on, and renew them, so it's pretty good.

Symlinks
--------

Symlinks are supported in the HYBRID mode. Put simply, zookeeper-fuse creates a file
called `__symlinks__` in the root of your ZooKeeper mounting point and stores there
names of files in the form of `<name of symlink>=<name of the file that it points to><LF>`.

It also registers a watch on it, so that if it is changed by another zookeeper-fuse, 
it will know.

Note that in HYBRID mode `__symlinks__` becomes an invalid file name.

Other syscalls
--------------

`lock()` and `flock()` are no-ops (only in HYBRID mode).
They will create a file if it doesn't exist.

`release()` and `releasedir()` are available in HYBRID mode, they are simply no-ops.

`opendir()` is available in HYBRID mode. It is a "if that file does not exist",
make it a directory. It will also mark any unmarked file as a directory.

`access()` is available only in HYBRID mode and will always return OK for existing
files and symlinks and ENOTENT for those that do not exist.

Keep in mind that in all modes the maximum file size is 256 kB.

See also
========

* [zookeeper-volume](https://github.com/smok-serwis/zookeeper-volume) - a Docker volume plugin
    using this package
