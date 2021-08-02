/*
 * Copyright 2016 Kyle Borowski
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * File:   ZookeeperFuse.cpp
 * Author: Kyle Borowski
 *
 * Created on July 26, 2016, 8:24 PM
 */
#define FUSE_USE_VERSION 26

#include <zookeeper/zookeeper.h>
#include <fuse.h>
#include <string>
#include <bsd/string.h>
#include <iostream>
#include <errno.h>
#include <stdio.h>
#include <getopt.h>
#include <memory.h>
#include <unistd.h>
#include <boost/filesystem.hpp>
#include <unordered_map>
#include <boost/algorithm/string.hpp>
#include "ZooFile.h"
#include "ZookeeperFuseContext.h"

using namespace std;

unordered_map<string, string> global_symlinks;

static int getattr_callback(const char *path, struct stat *stbuf);
static int readdir_callback(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
static int open_callback(const char *path, struct fuse_file_info *fi);
static int read_callback(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
static int write_callback(const char *, const char *, size_t, off_t, struct fuse_file_info *);
static int chmod_callback(const char *, mode_t);
static int chown_callback(const char *, uid_t, gid_t);
static int utime_callback(const char *, struct utimbuf *);
static int create_callback(const char *, mode_t, struct fuse_file_info *);
static int truncate_callback(const char *, off_t);
static int rename_callback(const char *path, const char *target);
static int unlink_callback(const char *);
static int mkdir_callback(const char*, mode_t);
static int readlink_callback(const char * path, char * out, size_t buf_size);
static int symlink_callback(const char * path_to, const char * path_from);
static int lock_callback(const char * path, struct fuse_file_info * info, int cmd, struct flock * flock_s);
static int flock_callback(const char * path, struct fuse_file_info * info, int op);
static int releasedir_callback(const char * path, struct fuse_file_info * info);
static int release_callback(const char * path, struct fuse_file_info * info);
static int opendir_callback(const char * path, struct fuse_file_info * info);
static int access_callback(const char * path, int mode);


const static string dataNodeName = "_zoo_data_";
const static string symlinkNodeName = "__symlinks__";
const static string symlinkNodeNameWithPath = "/__symlinks__";
static struct fuse_operations fuse_zoo_operations;

#define LOG(context, level, msg, ...) \
    context->getLogger().log(level, msg, __VA_ARGS__)

/*
 * ZookeeperFuse Main Function
 *
 * Parses command line arguments. Everything before an empty "--" are handled by fuse, everything after by us
 * Registers the fuse callbacks.
 * Creates the zookeeper connection/context.
 *
 * Three display modes are supported for leaf nodes, each has its quirks
 * 1. LEAF_AS_DIR  - Display all leaf nodes as directories, make their data available in a special child data node
 *                   Has the side-effect of not being able to create new leaf nodes except via mkdir
 * 2. LEAF_AS_FILE - Display all leaf nodes as files
 *                   Has the side-effect of not being able to add child files or folders to leaf nodes
 * 3. LEAF_AS_HYBRID -
 *      1. / is always a directory
 *      2. If I've seen a mkdir() on that, it's a directory
 *      3. If I've seen an open() or create() on that, it's a file
 *      4. If it has any children, it's a directory
 *      5. If it does not have any content, it's a directory
 *      6. If it does have content, it's a file
 */
static void reread_symlinks();

int main(int argc, char** argv) {
    string zooHosts;
    string zooAuthScheme;
    string zooAuthentication;
    string zooPath = "/";
    LeafMode leafMode = LEAF_AS_DIR;
    size_t maxFileSize = 256*1024;
    Logger::LogLevel logLevel = Logger::INFO;
    string logPropFile;

    string division = "--";
    int argumentDivider = 0;

    while (argumentDivider < argc) {
        string argument = argv[argumentDivider];
        if (argument == division) {
            break;
        }
        argumentDivider++;
    }

    struct option longopts[] = {
        { "help", no_argument, NULL, 'h'},
        { "zooPath", required_argument, NULL, 'f'},
        { "zooHosts", required_argument, NULL, 's'},
        { "zooAuthScheme", required_argument, NULL, 'A'},
        { "zooAuthentication", required_argument, NULL, 'a'},
        { "leafMode", required_argument, NULL, 'l'},
        { "maxFileSize", required_argument, NULL, 'm'},
        { "logLevel", required_argument, NULL, 'd'},
        { 0, 0, 0, 0}
    };
    char c;
    while ((c = getopt_long(argc - argumentDivider, argv + argumentDivider, "hf:s:a:d:l:", longopts, NULL)) != -1) {
        switch (c) {
            case 'h':
                cerr << "Usage: "<< argv[0] << " [OPTIONS]\n"
                        "--help              -h          print this usage\n"
                        "--zooPath           -f          path to root of zoo for fuse\n"
                        "--zooHosts          -s          zookeeper servers to which to connect\n"
                        "--zooAuthScheme     -A          zookeeper authentication scheme (i.e. digest)\n"
                        "--zooAuthentication -a          zookeeper authentication string\n"
                        "--leafMode          -l          display mode for leaves, DIR or FILE (default=DIR) or HYBRID\n"
                        "--maxFileSize       -m          maximum size in bytes of file in the zoo (default=256 kB)\n"
                        "--logLevel          -d          verbosity of logging ERROR, WARNING, INFO, DEBUG, TRACE\n";
                exit(0);
                break;
            case 'f':
                zooPath = optarg;
                break;
            case 's':
                zooHosts = optarg;
                break;
            case 'A':
                zooAuthScheme = optarg;
                break;
            case 'a':
                zooAuthentication = optarg;
                break;
            case 'l':
                leafMode = (string(optarg) != "FILE") ? ((string(optarg) != "DIR") ? LEAF_AS_HYBRID : LEAF_AS_DIR) : LEAF_AS_FILE;
                break;
            case 'm':
                maxFileSize = atoi(optarg);
                break;
            case 'd':
                cout << "Setting log level to "<< optarg << endl;
                logLevel = Logger::stringToLevel(optarg);
                break;
        }
    }

    if (leafMode == LEAF_AS_HYBRID) {
        fuse_zoo_operations.symlink = symlink_callback;
        fuse_zoo_operations.readlink = readlink_callback;
        fuse_zoo_operations.lock = lock_callback;
        fuse_zoo_operations.flock = flock_callback;
        fuse_zoo_operations.release = release_callback;
        fuse_zoo_operations.releasedir = releasedir_callback;
        fuse_zoo_operations.opendir = opendir_callback;
        fuse_zoo_operations.access = access_callback;
        fuse_zoo_operations.rename = rename_callback;
    }
    fuse_zoo_operations.getattr = getattr_callback;
    fuse_zoo_operations.open = open_callback;
    fuse_zoo_operations.read = read_callback;
    fuse_zoo_operations.readdir = readdir_callback;
    fuse_zoo_operations.write = write_callback;
    fuse_zoo_operations.chmod = chmod_callback;
    fuse_zoo_operations.chown = chown_callback;
    fuse_zoo_operations.utime = utime_callback;
    fuse_zoo_operations.create = create_callback;
    fuse_zoo_operations.truncate = truncate_callback;
    fuse_zoo_operations.unlink = unlink_callback;
    fuse_zoo_operations.rmdir = unlink_callback;
    fuse_zoo_operations.mkdir = mkdir_callback;

    auto_ptr<ZookeeperFuseContext> context(
        new ZookeeperFuseContext(logLevel, zooHosts, zooAuthScheme, zooAuthentication, zooPath, leafMode, maxFileSize));

    if (leafMode == LEAF_AS_HYBRID) {
        enableHybridMode();
    }
    return fuse_main(argumentDivider, argv, &fuse_zoo_operations, context.get());
}

/**
 * If this flag is false, then next time that FUSE is invoked symlinks will be re-readed.
 */
bool were_symlinks_readed = false;

static string getFullPath(string path) {
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    string retval = context->getPath();

    // Avoid duplicate "/" issues at the start of paths
    if (retval == "/") {
        retval = "";
    }

    if ((context->getLeafMode() != LEAF_AS_HYBRID) && (boost::filesystem::path(path).filename() == dataNodeName)) {
        retval += boost::filesystem::path(path).parent_path().string();
        LOG(context, Logger::DEBUG, "Requesting the data node... aliasing to: %s", retval.c_str());
    } else {
        retval += path;
        LOG(context, Logger::DEBUG, "Requesting a regular node: %s", retval.c_str());
    }

    // Must avoid ending the path in "/" unless we are looking at the root, zookeeper is picky
    if (retval.length() > 1 && *(retval.rbegin())  == '/') {
        retval.erase(retval.length() - 1);
    }

    return retval;
}
static string getFullPath(const char * path) {
    string s_path(path);
    s_path = getFullPath(s_path);
    return s_path;
}

static string getFullPath_c(const string path) {
    string s_path(path);
    s_path = getFullPath(s_path);
    return s_path;
}

static void callback_init(const string &callback, const string &path) {
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    LOG(context, Logger::DEBUG, "In: %s. Path: %s", callback.c_str(), path.c_str());
    if (!were_symlinks_readed) {
        reread_symlinks();
    }
}

static void zookeeper_watcher(zhandle_t *zh, int type, int state, const char *path,void *watcherCtx) {
    // If we've got a watch, that can mean that __symlinks__ has just been altered
    if (type == ZOO_CHANGED_EVENT) {
        were_symlinks_readed = false;       // force a re-read of symlinks next time FUSE is invoked
    }
}

/**
 * This means that we've changed the symlink status.
 * @return
 */
static void store_symlinks() {
    callback_init("store_symlinks", "");
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    ostringstream out;
    for (unordered_map<string, string>::iterator it = global_symlinks.begin(); it != global_symlinks.end(); it++) {
        out << it->first << "=" << it->second;
        unordered_map<string, string>::iterator next_it = std::next(it);
        if (next_it != global_symlinks.end()) {
            out << "\x0A";
        }
    }
    ZooFile symlinks(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath_c(symlinkNodeNameWithPath));
    symlinks.setContent(out.str());
}


static void inner_reread_symlinks() {
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    // This will always succeed
    zoo_set_watcher(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), &zookeeper_watcher);
    // Read symlinks and register a watch for it
    if (context->getLeafMode() != LEAF_AS_HYBRID) {
        return;
    }
    vector<string> symlinks_pairs;
    ZooFile symlinks(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath_c(symlinkNodeNameWithPath));
    if (!symlinks.exists()) {
        symlinks.create();
    }
    string content = symlinks.getContentAndSetWatch();
    if (content.length() == 0) {
        return;
    }
    boost::split(symlinks_pairs, content, boost::is_any_of("\x0A"));
    // There is no hazard here since or FUSE is single threaded.
    global_symlinks.clear();
    for (vector<string>::iterator it = symlinks_pairs.begin(); it != symlinks_pairs.end(); it++) {
        vector<string> symlink_pairs_t;
        symlink_pairs_t.clear();
        boost::split(symlink_pairs_t, *it, boost::is_any_of("="));
        if (symlink_pairs_t.size() < 2) {
            // Most probably you made too many symlinks and the size of __symlink__ file hit
            // the size limit. Increase the size limit in that case
            LOG(context, Logger::WARNING, "Seen an error processing symlink entry \"%s\", skipping it", it->c_str());
            continue;
        }
        string symlink = symlink_pairs_t[0];
        string pointing_at = symlink_pairs_t[1];
        global_symlinks[symlink] = pointing_at;
        LOG(context, Logger::DEBUG, "Adding a symlink %s pointing to %s", symlink.c_str(), pointing_at.c_str());
    }
    were_symlinks_readed = true;
}

static void reread_symlinks() {
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    for (int i=0; i<3; i++) {
        try{
            inner_reread_symlinks();
            return;
        } catch (ZooFileException e) {
            LOG(context, Logger::ERROR, "Zookeeper Error during re-reading symlinks: %d", e.getErrorCode());
        } catch (ZookeeperFuseContextException e) {
            LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error during re-reading symlinks: %d", e.getErrorCode());
        }
    }
    LOG(context, Logger::ERROR, "Failed to re-read symlinks %d times", 3);
}

static int rename_callback(const char * path, const char * target) {
    callback_init("rename_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    string s_path(path), s_target(target);
    bool should_store_symlinks = false;

    // delete the target, if it exists
    unordered_map<string, string>::iterator it = global_symlinks.find(s_target);
    if (it != global_symlinks.end()) {
        string target = it->second;
        global_symlinks.erase(it);
        should_store_symlinks = true;
    } else {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(s_target));

        if (file.exists()) {
            file.remove();
        }

    }

    // copy the file
    it = global_symlinks.find(s_path);
    if (it != global_symlinks.end()) {
        string target = it->second;
        global_symlinks.erase(it);
        global_symlinks[target] = target;
        should_store_symlinks = true;
    } else {
        ZooFile source_file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(s_path));
        ZooFile target_file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(s_target));

        if (source_file.isDir()) {
            LOG(context, Logger::ERROR, "Renaming directories is not supported, tried to rename %s", s_path.c_str());
            return -ENOSYS;
        } else {
            target_file.create();
            target_file.setContent(source_file.getContent());
            target_file.markAsFile();
            source_file.remove();
        }
    }
    if (should_store_symlinks) {
        store_symlinks();
    }

    return 0;

}

static int access_callback(const char * path, int mode) {
    callback_init("access_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    string s_path(path);

    if (global_symlinks.find(s_path) != global_symlinks.end()) {
        return 0;   // We found a symlink!
    }

    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(s_path));
        if (file.exists()) {
            return 0;
        } else {
            return -ENOENT;
        }
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}

static int releasedir_callback(const char * path, struct fuse_file_info * info) {
    callback_init("releasedir_callback", path);
    return 0;
}
static int release_callback(const char * path, struct fuse_file_info * info) {
    callback_init("release_callback", path);
    return 0;
}


static int lock_callback(const char * path, struct fuse_file_info * info, int cmd, struct flock * flock_s) {
    callback_init("lock_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));

        if (!file.exists()) {
            file.create();
            file.markAsFile();
        }
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}

static int opendir_callback(const char * path, struct fuse_file_info * info) {
    callback_init("opendir_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));

        if (!file.exists()) {
            file.create();
        }
        file.markAsDirectory();
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;

}

static int flock_callback(const char * path, struct fuse_file_info * info, int op) {
    callback_init("flock_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));

        if (!file.exists()) {
            file.create();
        }
        file.markAsFile();
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}


/** Create a symbolic link */
static int symlink_callback(const char * path_to, const char * path_from) {
    callback_init("symlink_callback", path_to);
    string s_path_to(path_to), s_path_from(path_from);
    global_symlinks[s_path_from] = s_path_to;
    store_symlinks();
    return 0;
}

static int readlink_callback(const char * path, char * out, size_t buf_size) {
    callback_init("readlink_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    string s_path(path);
    unordered_map<string, string>::iterator it = global_symlinks.find(s_path);
    if (it == global_symlinks.end()) {
        LOG(context, Logger::DEBUG, "Requested nonexisting symlink: %s", s_path);

        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        if (!file.exists()) {
            return -ENOENT;
        }
        return -EINVAL;
    }
    if (buf_size < it->second.length()-1) {
        LOG(context, Logger::WARNING, "Too short buffer provided for readlink, buffer "
                                      "size was %d symlink length was %d", buf_size, it->second.length());
    }
    LOG(context, Logger::DEBUG, "Returning %s as symlink for %s", it->second.c_str(), path);
    strlcpy(out, it->second.c_str(), buf_size);
    return 0;
}


static int getattr_callback(const char *path, struct stat *stbuf) {
    callback_init("getattr_callback", path);
    memset(stbuf, 0, sizeof (struct stat));
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    string s_path(path);
    unordered_map<string, string>::iterator it = global_symlinks.find(s_path);
    if (it != global_symlinks.end()) {
        // We've hit a symlink
        stbuf->st_mode = S_IFLNK | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }
    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(s_path));
        if (file.exists()) {
            bool isDir = file.isDir();

            if (context->getLeafMode() == LEAF_AS_DIR) {
                // In LEAF_AS_DIR mode, override to make all nodes directories except the special data nodes
                isDir = boost::filesystem::path(path).filename() != dataNodeName;
            }

            if (isDir) {
                stbuf->st_mode = S_IFDIR | 0755;
                stbuf->st_nlink = 2;
                return 0;
            } else {
                size_t length = file.getLength();
                LOG(context, Logger::DEBUG, "Getting file size for: %s size: %d", getFullPath(path).c_str(), length);
                stbuf->st_mode = S_IFREG | 0777;
                stbuf->st_nlink = 1;
                stbuf->st_size = length;
                return 0;
            }
        }
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        if (e.getErrorCode() == ZNOAUTH) {
            return -EACCES;
        } else  {
            return -EIO;
        }
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return -ENOENT;
}

static int readdir_callback(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi) {
    callback_init("readdir_callback", path);
    (void) offset;
    (void) fi;
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    if (context->getLeafMode() != LEAF_AS_HYBRID) {
        filler(buf, dataNodeName.c_str(), NULL, 0);
    } else {        // if leaf mode is HYBRID
        boost::filesystem::path parent(path);
        // Check if there are any children among symlinks
        for (unordered_map<string, string>::iterator it = global_symlinks.begin(); it != global_symlinks.end(); it++) {
            boost::filesystem::path child_path(it->first);
            if (child_path.parent_path() == parent) {
                filler(buf, child_path.filename().c_str(), NULL, 0);
                LOG(context, Logger::DEBUG, "adding a symlink %s", child_path.filename().c_str());
            }
        }
    }
    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));

        vector<string> children = file.getChildren();
        for (size_t i = 0; i < children.size(); i++) {
            if ((context->getLeafMode() != LEAF_AS_HYBRID) && (dataNodeName == children[i])) {
                LOG(context, Logger::ERROR, "zookeeper-fuse error: cannot be used on a node which has a child node called %s", dataNodeName.c_str());
                return -EIO;
            }
            if ((context->getLeafMode() == LEAF_AS_HYBRID) && (symlinkNodeName == children[i])) {
                continue;
            }
            LOG(context, Logger::DEBUG, "adding a file %s", children[i].c_str());
            filler(buf, children[i].c_str(), NULL, 0);
        }
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}

static int open_callback(const char *path, struct fuse_file_info *fi) {
    callback_init("open_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    if (context->getLeafMode() != LEAF_AS_HYBRID) {
        return 0;
    }

    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));

        if (!file.exists()) {
            file.create();
        }
        file.markAsFile();
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}

static int read_callback(const char *path, char *buf, size_t size, off_t offset,
        struct fuse_file_info *fi) {
    callback_init("read_callback", path);
    string content;
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        content = file.getContent();

        LOG(context, Logger::DEBUG, "Reading from path: %s content: %s", getFullPath(path).c_str(), content.c_str());

        ssize_t len = content.size();
        if (offset >= len) {
            return 0;
        }

        ssize_t addr = offset + size;
        if (addr > len) {
            memcpy(buf, content.c_str() + offset, len - offset);
            return len - offset;
        }

        memcpy(buf, content.c_str() + offset, size);
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return size;
}

int write_callback(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    callback_init("write_callback", path);
    string content;
    string in = string(buf, size);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    try {
        if (offset + size > context->getMaxFileSize()) {
            LOG(context, Logger::ERROR, "Attempting to write past maximum file size of %d", context->getMaxFileSize());
            return -EINVAL;
        }

        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        content = file.getContent();
        content.resize(offset + size);
        content.replace(offset, size, in);
        file.setContent(content);
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return size;
}

int chmod_callback(const char *path, mode_t mode) {
    callback_init("chmod_callback", path);
    return 0;
}

int chown_callback(const char *path, uid_t uid, gid_t gid) {
    callback_init("chown_callback", path);
    return 0;
}

int utime_callback(const char *path, struct utimbuf *buf) {
    callback_init("utime_callback", path);
    return 0;
}

int create_callback(const char *path, mode_t mode, struct fuse_file_info *fi) {
    callback_init("create_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    try {
        if (context->getLeafMode() == LEAF_AS_DIR) {
            LOG(context, Logger::ERROR, "File creation is only allowed via mkdir in LEAF_AS_DIR mode. Path: %s", path);
            return -ENOENT;
        }

        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));

        if (!file.exists()) {
            file.create();
            file.markAsFile();
        }
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}

int truncate_callback(const char *path, off_t size) {
    callback_init("truncate_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        string content = file.getContent();
        content.resize(size);
        file.setContent(content);
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}

int unlink_callback(const char *path) {
    callback_init("unlink_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    string s_path(path);
    unordered_map<string, string>::iterator it = global_symlinks.find(s_path);
    if (it != global_symlinks.end()) {
        // We are deleting a symlink
        global_symlinks.erase(s_path);
        store_symlinks();
        return 0;
    }
    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(s_path));
        file.remove();
    } catch (ZooFileException e) {
        if (e.getErrorCode() == ZNOTEMPTY) {
            return -ENOTEMPTY;
        }
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}

int mkdir_callback(const char* path, mode_t mode) {
    callback_init("mkdir_callback", path);
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        if (!file.exists()) {
            file.create();
        }
        file.markAsDirectory();
    } catch (ZooFileException e) {
        LOG(context, Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        LOG(context, Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}
