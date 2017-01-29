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
 * File:   Logger.h
 * Author: kyle
 * 
 * Created on July 27, 2016, 7:52 PM
 */


#include <iostream>
#include <string>
#include <sstream>

#ifndef LOGGER_H
#define LOGGER_H

using namespace std;

class Logger {
public:

    enum LogLevel {
        ERROR,
        WARNING,
        INFO,
        DEBUG,
        TRACE
    };

    Logger(LogLevel maxLevel = INFO);

    ~Logger();

    virtual void log(LogLevel level, const char *fmt, ...);

    virtual void setLogLevel(LogLevel level);

    LogLevel getLogLevel();
    
    virtual string getLogPrefix(LogLevel level);

    static Logger::LogLevel stringToLevel(string level) {
        for (int i = ERROR; i <= TRACE; i++) {
            LogLevel retval = static_cast<LogLevel> (i);
            if (levelToString(retval) == level) {
                return retval;
            }
        }
        return ERROR;
    }

    static string levelToString(LogLevel level) {
        string retval;
        switch (level) {
            case ERROR:
                retval = "ERROR";
                break;
            case WARNING:
                retval = "WARNING";
                break;
            case INFO:
                retval = "INFO";
                break;
            case DEBUG:
                retval = "DEBUG";
                break;
            case TRACE:
                retval = "TRACE";
                break;
            default:
                retval = "INVALID";
        }
        return retval;
    }
    
private:
    LogLevel maxLevel_;
};

#endif
