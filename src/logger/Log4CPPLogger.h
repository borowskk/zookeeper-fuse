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
 * File:   Log4CPPLogger.h
 * Author: glantzn
 *
 * Created on January 1, 2017, 7:52 PM
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
	Log4CPPLogger(Logger::LogLevel& maxLevel);

	virtual ~Log4CPPLogger();

	virtual void log(LogLevel level, char *fmt, ...);
};
#endif


