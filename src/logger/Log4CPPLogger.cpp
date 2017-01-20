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



#ifdef HAVE_LOG4CPP

#include <cstdarg>
#include <stdio.h>
#include <sstream>

#include "Logger.h"

#include <log4cpp/Category.hh>
#include <log4cpp/BasicLayout.hh>
#include <log4cpp/Configurator.hh>
#include <log4cpp/Appender.hh>
#include <log4cpp/OstreamAppender.hh>


class Log4CPPLogger : public Logger {

private:
	log4cpp::Category* zkLogger_;

public:
	Log4CPPLogger(Logger::LogLevel& maxLevel) : Logger(maxLevel) {
		fprintf(stdout, "Log Level Set To: %s for Log4CPP\n", levelToString(maxLevel).c_str());

		// Configure log4cpp root logger settings.
		log4cpp::Category& rootLog = log4cpp::Category::getRoot();
		log4cpp::Priority::Value logPriority;

		switch(maxLevel) {
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

		this->log(Logger::DEBUG, "testing LOG4CPP");
	}

	virtual ~Log4CPPLogger() {}

	virtual void log(LogLevel level, char *fmt, ...) {

		// BEGIN LOG4CPP Logging
		// switch on Logger configurations and adapt them to log4cpp logging functions
		switch(level) {
		case ERROR:
			zkLogger_->error(fmt);
			break;
		case WARNING:
			zkLogger_->warn(fmt);
			break;
		case INFO:
			zkLogger_->info(fmt);
			break;
		case DEBUG:
			zkLogger_->debug(fmt);
			break;
		case TRACE:
			zkLogger_->debug(fmt);
			break;
		}

		}

		void setLogLevel(LogLevel level) {
		    maxLevel_ = level;
		}

		string levelToString(LogLevel level) {
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

		Logger::LogLevel stringToLevel(string level) {
		   for (int i = ERROR; i <= TRACE; i++) {
		       LogLevel retval = static_cast<LogLevel>(i);
		       if (levelToString(retval) == level) {
		           return retval;
		       }
		   }
		   return ERROR;
		}

};
#endif
