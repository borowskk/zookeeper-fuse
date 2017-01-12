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
 * File:   ZooFile.cpp
 * Author: kyle
 * 
 * Created on July 27, 2016, 7:52 PM
 */

#include <string.h>
#include <string>
#include <iostream>

#include "ZooFile.h"

const size_t ZooFile::MAX_FILE_SIZE = 4096;

ZooFile::ZooFile(zhandle_t* handle, const string &path) :
handle_(handle),
path_(path) {

}

ZooFile::~ZooFile() {

}

bool ZooFile::exits() const {
    int rc = zoo_exists(handle_, path_.c_str(), 0, NULL);
    if (rc == ZNONODE) {
        return false;
    }
    if (rc != ZOK) {
        throw ZooFileException("An error occurred checking the existence of file: " + path_, rc);
    }
    return true;
}

bool ZooFile::isDir() const {
    return !getChildren().empty();
}

vector<string> ZooFile::getChildren() const {
    vector<string> retval;    
    String_vector children;
    
    int rc = zoo_get_children(handle_, path_.c_str(), 0, &children);
    if (rc != ZOK) {
        throw ZooFileException("An error occurred getting children of file: " + path_, rc);
    }    
    
    for (int i = 0; i < children.count; i++) {
        retval.push_back(children.data[i]);
    }
    deallocate_String_vector(&children);
    return retval;
}

string ZooFile::getContent() const {
    Stat stat;
    char content[MAX_FILE_SIZE];
    int contentLength = MAX_FILE_SIZE;
    
    memset( content, 0, MAX_FILE_SIZE );
    memset( &stat, 0, sizeof(stat) );
    
    int rc = zoo_get(handle_, path_.c_str(), 0, content, &contentLength, &stat);
    if (rc != ZOK) {
        throw ZooFileException("An error occurred getting the contents of file: " + path_, rc);
    }
    
    string retval;
    if (contentLength > 0) {
        retval = string(content, contentLength);
    } else {
        retval = "";
    }
    
    cout << "getContent() on " << path_ << " content length: " << contentLength << " value: " << string(content, contentLength) << endl;
    return retval;
}

void ZooFile::setContent(string content) {
    cout << "setContent() on " << path_ << " content length: " << content.length() << " content: " << content << endl;
    int rc = zoo_set(handle_, path_.c_str(), content.c_str(), content.length(), -1);
    if (rc != ZOK) {
        throw ZooFileException("An error occurred setting the contents of file: " + path_, rc);    
    }   
}

void ZooFile::create() {
    int rc = zoo_create(handle_, path_.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    if (rc != ZOK) {
        throw ZooFileException("An error occurred creating the file: " + path_, rc);
    }      
}

void ZooFile::remove() {
    int rc = zoo_delete(handle_, path_.c_str(), -1);
    if (rc != ZOK) {
        throw ZooFileException("An error occurred deleting the file: " + path_, rc);
    }         
}
