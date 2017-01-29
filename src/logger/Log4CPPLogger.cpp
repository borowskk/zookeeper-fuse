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
 * File:   Log4CPPLogger.cpp
 * Author: glantzn
 *
 * Created on January 1, 2017, 7:52 PM
 */

#ifdef HAVE_LOG4CPP

#include "Log4CPPLogger.h"

Log4CPPLogger::Log4CPPLogger(Logger::LogLevel& maxLevel) : Logger(maxLevel) {
    printf("Log Level Set To: %s for Log4CPP\n", levelToString(maxLevel).c_str());

    // Configure log4cpp root logger settings.
    log4cpp::Category& rootLog = log4cpp::Category::getRoot();
    log4cpp::Priority::Value logPriority;

    switch (maxLevel) {
        case Logger::INFO:
            logPriority = log4cpp::Priority::INFO;
            break;
        case Logger::DEBUG:
            logPriority = log4cpp::Priority::DEBUG;
            break;
        case Logger::WARNING:
            logPriority = log4cpp::Priority::WARN;
            break;
        default:
            logPriority = log4cpp::Priority::ERROR;
            break;
    }
    rootLog.setPriority(logPriority);

    log4cpp::Appender *appender = new log4cpp::OstreamAppender("console", &std::cout);
    appender->setLayout(new log4cpp::BasicLayout());

    zkLogger_ = &log4cpp::Category::getInstance(std::string("zkLogger"));
    zkLogger_->addAppender(appender);

    this->log(Logger::DEBUG, "Using LOG4CPP");
}

Log4CPPLogger::~Log4CPPLogger() {
    // TODO: Free appender and layout?
}

void Log4CPPLogger::log(LogLevel level, const char *fmt, ...) {
    char buffer[512];
    
    va_list args;
    va_start(args, fmt);
    vsnprintf(buffer, 512, fmt, args);
    va_end(args);
    
    // BEGIN LOG4CPP Logging
    // switch on Logger configurations and adapt them to log4cpp logging functions
    switch (level) {
        case ERROR:
            zkLogger_->error(buffer);
            break;
        case WARNING:
            zkLogger_->warn(buffer);
            break;
        case INFO:
            zkLogger_->info(buffer);
            break;
        case DEBUG:
            zkLogger_->debug(buffer);
            break;
        case TRACE:
            zkLogger_->debug(buffer);
            break;
    }
}
#endif
