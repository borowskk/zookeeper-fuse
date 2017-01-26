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
 * File:   ZooContext.cpp
 * Author: kyle
 * 
 * Created on July 31, 2016, 10:37 AM
 */

#include <iostream>
#include <errno.h>
#include <unistd.h>

#include "ZookeeperFuseContext.h"
#include "logger/Logger.h"
#include "logger/Log4CPPLogger.h"

ZookeeperFuseContext::ZookeeperFuseContext(Logger::LogLevel maxLevel, const string &hosts, const string &authScheme, const string &auth, 
                                           const string &path, LeafMode leafMode, size_t maxFileSize):
hosts_(hosts), authSheme_(authScheme), auth_(auth), path_(path), handle_(NULL), leafMode_(leafMode), maxFileSize_(maxFileSize), eventQueue_(8) {


#ifdef HAVE_LOG4CPP
	Log4CPPLogger * logger = new Log4CPPLogger(maxLevel);
#endif
#ifndef HAVE_LOG4CPP
	Logger * logger = new Logger(maxLevel);
#endif

	logger_ = logger;
}

ZookeeperFuseContext::~ZookeeperFuseContext() {
    if (handle_ != NULL) {
        int rc = zookeeper_close(handle_);
        if (rc != ZOK) {
            cerr << "An error occurred freeing the zookeeper handle." << endl;
        }
    }
}

//the implementation of the global ZK event watcher
static void zkWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
    // This is probably not thread-safe!
    cout << "In zkWatcher for handle: " << zh << " type: " << type << " state: " << state << endl;
    ZookeeperFuseContext* context = reinterpret_cast<ZookeeperFuseContext*>(watcherCtx);
    context->fireConnectedEvent();
}

void ZookeeperFuseContext::fireConnectedEvent() {
    eventQueue_.push('c');
}

Logger& ZookeeperFuseContext::getLogger() {
    return *logger_;
}

zhandle_t* ZookeeperFuseContext::getZookeeperHandle() {
    if (!handle_) {
        handle_ = zookeeper_init(hosts_.c_str(), zkWatcher, 10, NULL, this, 0);
        if (handle_ == NULL) {
            cerr << "Failed to create zookeeper handle with error: " << errno << endl;
            return NULL;
        }
        
        if (!authSheme_.empty() && !auth_.empty()) {
            cout << "Will authenticate with scheme: " << authSheme_ << " and authentication: " << auth_ << endl;
            int rc = zoo_add_auth(handle_, authSheme_.c_str(), auth_.c_str(), auth_.size(), NULL, NULL);
            if (rc != ZOK) {
                cerr << "Failed to submit authentication request with error: " << rc;
            }
        }

        // Should eventually use a mechanism with a blocking wait    
        char event;
        while (!eventQueue_.pop(event)) {
            cout << "Waiting for the zookeeper connection to be established." << endl;
            sleep(1);
        }
    }
    return handle_;
}

string ZookeeperFuseContext::getPath() const {
    return path_;
}
 
void ZookeeperFuseContext::setPath(const string &path) {
    path_ = path;
}

LeafMode ZookeeperFuseContext::getLeafMode() const {
    return leafMode_;
}

void ZookeeperFuseContext::setLeafMode(LeafMode leafMode) {
    leafMode_ = leafMode;
}

size_t ZookeeperFuseContext::getMaxFileSize() const {
    return maxFileSize_;
}

void ZookeeperFuseContext::setMaxFileSize(size_t maxFileSize) {
    maxFileSize_ = maxFileSize;
}

ZookeeperFuseContext* ZookeeperFuseContext::getZookeeperFuseContext(fuse_context* context) {
    if (context) {
        ZookeeperFuseContext* zooContext = reinterpret_cast<ZookeeperFuseContext*>(context->private_data);
        if (zooContext) {
            return zooContext;
        }
    }
    throw ZookeeperFuseContextException("Could not get the zookeeper context from fuse", ZINVALIDSTATE);
}

zhandle_t* ZookeeperFuseContext::getZookeeperHandle(fuse_context* context) {
    ZookeeperFuseContext* zooContext = getZookeeperFuseContext(context);
    zhandle_t* retval = zooContext->getZookeeperHandle();
    if (retval) {
        return retval;
    }
    throw ZookeeperFuseContextException("Could not get the zookeeper context from fuse", ZINVALIDSTATE);
}
