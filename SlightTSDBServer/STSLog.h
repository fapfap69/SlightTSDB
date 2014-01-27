/*
 * TSLog.h
 *
 *  Created on: 25/nov/2013
 *      Author: fap
 */

#ifndef TSLOG_H_
#define TSLOG_H_

typedef enum {
	TSLL_CRITICAL,
	TSLL_ERROR,
	TSLL_WARNING,
	TSLL_INFO,
	TSLL_DEBUG,
	TSLL_TRACE
} logLevel_ty;

#define CRITICAL(a,...)	tsLog(TSLL_CRITICAL, __FILE__, __LINE__, a, ##__VA_ARGS__)
#define ERROR(a,...)	tsLog(TSLL_ERROR, __FILE__, __LINE__, a, ##__VA_ARGS__)
#define WARNING(a,...)	tsLog(TSLL_WARNING, __FILE__, __LINE__, a, ##__VA_ARGS__)
#define INFO(a,...)		tsLog(TSLL_INFO, __FILE__, __LINE__, a, ##__VA_ARGS__)
#define DEBUG(a,...)	tsLog(TSLL_DEBUG, __FILE__, __LINE__, a, ##__VA_ARGS__)
#define TRACE(a,...)	tsLog(TSLL_TRACE, __FILE__, __LINE__, a, ##__VA_ARGS__)

#define FUNCTION_TRACE	TRACE("%s()\n", __FUNCTION__)

// Functions protptypes

// Set the log level.
void tsLogSetLevel(logLevel_ty newLogLevel);
// Log the message
void tsLog(logLevel_ty logLevel, const char *file, int line, const char *fmt, ...);



#endif /* TSLOG_H_ */
