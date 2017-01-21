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
 * File:   Logger.cpp
 * Author: kyle
 * 
 * Created on July 27, 2016, 7:52 PM
 */


#include <cstdarg>
#include <stdio.h>
#include <sstream>

#include "Logger.h"

Logger::Logger(Logger::LogLevel maxLevel) : maxLevel_(maxLevel) {
    fprintf(stdout, "Log Level Set To: %s\n", levelToString(maxLevel).c_str());
    this->log(Logger::DEBUG, "testing");
}

Logger::~Logger() {
}

void Logger::log(LogLevel level, char *fmt, ...) {
    FILE* fd = level == ERROR ? stderr : stdout;

    if (level <= maxLevel_) {
        fprintf(fd, levelToString(level).c_str());
        fprintf(fd, " ");

        va_list args;
        va_start(args, fmt);
        vfprintf(fd, fmt, args);
        va_end(args);

        fprintf(fd, "\n");
    }

}

void Logger::setLogLevel(LogLevel level) {
    maxLevel_ = level;
}

Logger::LogLevel Logger::getLogLevel() {
    return maxLevel_;
}
