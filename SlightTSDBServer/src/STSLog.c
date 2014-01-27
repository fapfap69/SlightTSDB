/*
 * STSLog.c
 *
 *  Logging facility
 *
 *  Created on: 25/nov/2013
 *      Author: fap
 */


#include <stdarg.h>
#include <stdio.h>
#include <time.h>

#include "STSLog.h"

#define LOG_TO_SYSLOG
#define LOG_TO_STREAM

#ifdef LOG_TO_SYSLOG
#include <syslog.h>
#endif

#define SYSLOG_FACILITY		LOG_DAEMON

#define SYSLOG_NAME			"SlightTimeSeriesDB"
#define TIMESTAMP_MAX		20
#define TIMESTAMP_FORMAT	"%Y%m%d-%H:%M:%S"

static logLevel_ty gLogLevel = TSLL_CRITICAL;

void tsLogSetLevel(logLevel_ty newLogLevel) {
	gLogLevel = newLogLevel;
	INFO("tsLogSetLevel :: Log level set to %d\n", gLogLevel);
#ifdef LOG_TO_SYSLOG
	openlog(SYSLOG_NAME, LOG_CONS | LOG_PID | LOG_NDELAY, SYSLOG_FACILITY);
#endif
	return;
}

void tsLog(logLevel_ty logLevel, const char *file, int line, const char *fmt, ...) {
	va_list args;
#ifdef LOG_TO_SYSLOG
	int priority;
#endif
#ifdef LOG_TO_STREAM
	const char *llstr[] = {
			"CRITICAL",
			"ERROR",
			"WARNING",
			"INFO",
			"DEBUG",
			"TRACE",
	};
	char timestamp[TIMESTAMP_MAX];
	time_t now;
#endif

	if (logLevel > gLogLevel) return; // mute

#ifdef LOG_TO_SYSLOG
	// Log to syslog
	switch (logLevel) {
	case TSLL_CRITICAL: priority = LOG_CRIT; break;
	case TSLL_ERROR: priority = LOG_ERR; break;
	case TSLL_WARNING: priority = LOG_WARNING; break;
	case TSLL_INFO: priority = LOG_INFO; break;
	default: priority = LOG_DEBUG; break;
	}
	va_start(args, fmt);
	vsyslog(priority, fmt, args);
	va_end(args);
#endif

#ifdef LOG_TO_STREAM
	// Log to stdout
	now = time(NULL);
	strftime(timestamp, TIMESTAMP_MAX, TIMESTAMP_FORMAT, localtime(&now));
	fprintf(stderr, "%s:%s:%s(%d): ", timestamp, llstr[(int)logLevel], file, line);
	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);
	fflush(stderr);
#endif
	return;
}



