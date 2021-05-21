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
 * File:   ZooContext.h
 * Author: kyle
 *
 * Created on July 31, 2016, 10:37 AM
 */

#ifndef ZOOCONTEXT_H
#define	ZOOCONTEXT_H

#include <string>

#include <fuse.h>
#include <zookeeper/zookeeper.h>
#include <boost/lockfree/queue.hpp>
#include <boost/shared_ptr.hpp>

#include "logger/Logger.h"

using namespace std;
using namespace boost;

enum LeafMode {
    LEAF_AS_DIR,
    LEAF_AS_FILE,
    LEAF_AS_HYBRID
};

class ZookeeperFuseContextException : public std::exception {
public:
    ZookeeperFuseContextException(string msg, int rc) :
    msg_(msg), rc_(rc) {

    }

    virtual ~ZookeeperFuseContextException() throw() {

    }

    virtual const char* what() const throw()
    {
      return msg_.c_str();
    }

    int getErrorCode() const throw() {
        return rc_;
    }

private:
    string msg_;
    int rc_;
};

class ZookeeperFuseContext {
public:
    ZookeeperFuseContext(Logger::LogLevel maxLevel, const string &hosts, const string &authScheme, const string &auth, const string &path, 
                         LeafMode leafMode, size_t maxFileSize);
    virtual ~ZookeeperFuseContext();

    Logger& getLogger();

    zhandle_t* getZookeeperHandle();
    
    string getPath() const;
    void setPath(const string &path);    

    LeafMode getLeafMode() const;
    void setLeafMode(LeafMode leafMode);

    size_t getMaxFileSize() const;
    void setMaxFileSize(size_t maxFileSize);
   
    void fireConnectedEvent();
 
    static ZookeeperFuseContext* getZookeeperFuseContext(fuse_context* context);
    static zhandle_t* getZookeeperHandle(fuse_context* context);

private:
    ZookeeperFuseContext(const ZookeeperFuseContext& orig);
    ZookeeperFuseContext& operator=(const ZookeeperFuseContext &rhs);
    
    string hosts_;
    string authSheme_;
    string auth_;
    string path_;
    LeafMode leafMode_;
    size_t maxFileSize_;
    zhandle_t* handle_;
    boost::lockfree::queue<char> eventQueue_;
    auto_ptr<Logger> logger_;
};

static void zkWatcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx);
#endif	/* ZOOCONTEXT_H */

