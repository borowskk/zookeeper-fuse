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
#include <iostream>
#include <errno.h>
#include <stdio.h>
#include <getopt.h>
#include <memory.h>
#include <unistd.h>
#include <boost/filesystem.hpp>

#include "ZooFile.h"
#include "ZookeeperFuseContext.h"

using namespace std;

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
static int unlink_callback(const char *);
static int mkdir_callback(const char*, mode_t);

const static string dataNodeName = "_zoo_data_";
static struct fuse_operations fuse_zoo_operations;

/*
 * ZookeeperFuse Main Function
 *
 * Parses command line arguments. Everything before an empty "--" are handled by fuse, everything after by us
 * Registers the fuse callbacks.
 * Creates the zookeeper connection/context.
 * 
 * Two display modes are supported for leaf nodes, each has its quirks
 * 1. LEAF_AS_DIR  - Display all leaf nodes as directories, make their data available in a special child data node
 *                   Has the side-effect of not being able to create new leaf nodes except via mkdir
 * 2. LEAF_AS_FILE - Display all leaf nodes as files
 *                   Has the side-effect of not being able to add child files or folders to leaf nodes
 */
int main(int argc, char** argv) {
    string zooHosts;
    string zooAuthScheme;
    string zooAuthentication;
    string zooPath = "/";
    LeafMode leafMode = LEAF_AS_DIR;
    size_t maxFileSize = 1024;
    Logger::LogLevel logLevel = Logger::INFO;

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
    while ((c = getopt_long(argc - argumentDivider, argv + argumentDivider, "hf:s:a:l:", longopts, NULL)) != -1) {
        switch (c) {
            case 'h':
                cerr << "Usage: "<< argv[0] << " [OPTIONS]\n"
                        "--help              -h          print this usage\n"
                        "--zooPath           -f          path to root of zoo for fuse\n"
                        "--zooHosts          -s          zookeeper servers to which to connect\n"
                        "--zooAuthScheme     -A          zookeeper authentication scheme (i.e. digest)\n"
                        "--zooAuthentication -a          zookeeper authentication string\n"
                        "--leafMode          -l          display mode for leaves, DIR or FILE(default)\n"
                        "--maxFileSize       -m          maximum size in bytes of file in the zoo (default=1024)\n"
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
                leafMode = (optarg != "FILE") ? LEAF_AS_DIR : LEAF_AS_FILE;
                break;
            case 'm':
                maxFileSize = atoi(optarg);
                break;
            case 'd':
                logLevel = Logger::stringToLevel(optarg);
                break;
            default:
                break;
        }
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
    
    return fuse_main(argumentDivider, argv, &fuse_zoo_operations, context.get());
}

static string getFullPath(string path) {
    ZookeeperFuseContext* zooContext = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    
    string retval = zooContext->getPath();

    // Avoid duplicate "/" issues at the start of paths
    if (retval == "/") {
        retval = "";
    }

    if (boost::filesystem::path(path).filename() == dataNodeName) {
        retval += boost::filesystem::path(path).parent_path().string();
        zooContext->getLogger().log(Logger::DEBUG, "Requesting the data node... aliasing to: %s", retval.c_str());        
    } else {
        retval += path;
        zooContext->getLogger().log(Logger::DEBUG, "Requesting a regular node: %s", retval.c_str());
    }
            
    // Must avoid ending the path in "/" unless we are looking at the root, zookeeper is picky
    if (retval.length() > 1 && *(retval.rbegin())  == '/') {
        retval.erase(retval.length() - 1);
    }
    
    return retval;
}

static void callback_init(const string &callback, const string &path) {
    ZookeeperFuseContext* zooContext = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    zooContext->getLogger().log(Logger::DEBUG, "In: %s. Path: %s", callback.c_str(), path.c_str());
}

static int getattr_callback(const char *path, struct stat *stbuf) {
    callback_init("getattr_callback", path);
    memset(stbuf, 0, sizeof (struct stat));
    ZookeeperFuseContext* context = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    
    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        if (file.exits()) {
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
                string content = file.getContent();
                size_t length = content.length();
                context->getLogger().log(Logger::DEBUG, "Getting file size for: %s size: %d", getFullPath(path).c_str(), length);
                stbuf->st_mode = S_IFREG | 0777;
                stbuf->st_nlink = 1;
                stbuf->st_size = length;
                return 0;
            }
        }
    } catch (ZooFileException e) {
        context->getLogger().log(Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        if (e.getErrorCode() == ZNOAUTH) {
            return -EACCES;  
        } else  {
            return -EIO;
        }
    } catch (ZookeeperFuseContextException e) {
        context->getLogger().log(Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return -ENOENT;
}

static int readdir_callback(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi) {
    callback_init("readdir_callback", path);
    (void) offset;
    (void) fi;
    ZookeeperFuseContext* zooContext = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, dataNodeName.c_str(), NULL, 0);
    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));

        vector<string> children = file.getChildren();
        for (size_t i = 0; i < children.size(); i++) {
            if (dataNodeName == children[i]) {
                zooContext->getLogger().log(Logger::ERROR, "zookeeper-fuse error: cannot be used on a node which has a child node called %s", dataNodeName.c_str());
                return -EIO;
            }
            filler(buf, children[i].c_str(), NULL, 0);
        }
    } catch (ZooFileException e) {
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }
  
    return 0;
}

static int open_callback(const char *path, struct fuse_file_info *fi) {
    callback_init("open_callback", path);
    return 0;
}

static int read_callback(const char *path, char *buf, size_t size, off_t offset,
        struct fuse_file_info *fi) {
    callback_init("read_callback", path);
    string content;
    ZookeeperFuseContext* zooContext = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    
    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        content = file.getContent();
    
        zooContext->getLogger().log(Logger::DEBUG, "Reading from path: %s content: %s", getFullPath(path).c_str(), content.c_str());

        ssize_t len = strlen(content.c_str());
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
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
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
            context->getLogger().log(Logger::ERROR, "Attempting to write past maximum file size of %d", context->getMaxFileSize());
            return -EINVAL;
        }

        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        content = file.getContent();
        content.resize(offset + size);
        content.replace(offset, size, in);
        file.setContent(content);
    } catch (ZooFileException e) {
        context->getLogger().log(Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        context->getLogger().log(Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
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
            context->getLogger().log(Logger::ERROR, "File creation is only allowed via mkdir in LEAF_AS_DIR mode.");
            return -ENOENT;
        }

        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));

        if (!file.exits()) {
            file.create();
        }
    } catch (ZooFileException e) {
        context->getLogger().log(Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        context->getLogger().log(Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;  
}

int truncate_callback(const char *path, off_t size) {
    callback_init("truncate_callback", path);
    ZookeeperFuseContext* zooContext = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    
    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        string content = file.getContent();
        content.resize(size);
        file.setContent(content);
    } catch (ZooFileException e) {
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }
    
    return 0;
}

int unlink_callback(const char *path) {
    callback_init("unlink_callback", path);
    ZookeeperFuseContext* zooContext = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    
    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        file.remove();
    } catch (ZooFileException e) {
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;
}

int mkdir_callback(const char* path, mode_t mode) {
    callback_init("mkdir_callback", path);
    ZookeeperFuseContext* zooContext = ZookeeperFuseContext::getZookeeperFuseContext(fuse_get_context());
    
    try {
        ZooFile file(ZookeeperFuseContext::getZookeeperHandle(fuse_get_context()), getFullPath(path));
        if (!file.exits()) {
            file.create();
        }
    } catch (ZooFileException e) {
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Error: %d", e.getErrorCode());
        return -EIO;
    } catch (ZookeeperFuseContextException e) {
        zooContext->getLogger().log(Logger::ERROR, "Zookeeper Fuse Context Error: %d", e.getErrorCode());
        return -EIO;
    }

    return 0;    
}
