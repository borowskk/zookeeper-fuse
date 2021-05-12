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
 * File:   ZooFile.h
 * Author: kyle
 *
 * Created on July 27, 2016, 7:52 PM
 */

#ifndef ZOOFILE_H
#define	ZOOFILE_H
#define DBOOST_STACKTRACE_USE_ADDR2LINE
#include <vector>
#include <string>
#include <exception>

#include <boost/shared_ptr.hpp>
#include <zookeeper/zookeeper.h>

using namespace std;

class ZooFileException : public std::exception {
public:
    ZooFileException(string msg, int rc) :
    msg_(msg), rc_(rc) {
        
    }
    
    virtual ~ZooFileException() throw() {
        
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

class ZooFile {
public:
    static const size_t MAX_FILE_SIZE;
    
    ZooFile(zhandle_t*, const string &path);
    ZooFile(const ZooFile& orig);
    virtual ~ZooFile();
    
    bool exists() const;
    bool isDir() const;

    void markAsDirectory() const;
    void markAsFile() const;

    vector<string> getChildren() const;
    string getContent() const;
    void remove();
    /**
     * This is ever only called in LEAF_AS_HYBRID mode, and the only watch is then set is
     * for /__symlinks__
     */
    string getContentAndSetWatch() const;
    void setContent(string);
    void create();
    size_t getLength() const;

private:
    zhandle_t* handle_;
    const string path_;
};


void enableHybridMode();

#endif	/* ZOOFILE_H */

