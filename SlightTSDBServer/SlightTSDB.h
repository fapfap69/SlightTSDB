/*
 * SlightTSDB.h
 *
 *  Created on: 25/nov/2013
 *      Author: fap
 */

#ifndef TSSERVER_H_
#define TSSERVER_H_

#ifndef FALSE
#define FALSE (0)
#define TRUE (!(FALSE))
#endif

// Global define constants
#define TSS_VER	0.1
#define TSS_PORT		8080
#define TSS_USER		"pippo"
#define TSS_DATA_PATH	"/var/STSServer"
#define TSS_LOG_FILE	"/var/log/STSServer.log"
#define TSS_LOG_LEVEL	1

// #define TSS_USEAUTHENTICATION
// #define TSM_DOUBLE_TYPE
#define TSM_LONG_TIMESTAMP
#define TSM_PTHREAD_LOCKING
#define TSM_REMOVE_DATAFILES
#define TSM_JSON_OUTPUT
#define TSM_AUTOCORRECT_DB_STRUCT
// #define TSM_CACHEPRESERVE

#define TSM_MAX_METRICS	32	// Maximum number of metrics per data set
#define TSM_MAX_ACTIVEDP	225	// max number of active data points

// This define the maximum amount of seconds before the automatic
// close of a stream
#define TSM_MAXUNUSEDTIME 60

// Buffer dimensions
#define TS_MINDIMBUFFER	512
#define TS_MIDDIMBUFFER	2048
#define TS_MAXDIMBUFFER	32768

// The Date Time format string -> srtftime()
#define TS_DATETIMEFORMAT "%d/%m/%Y %H:%M:%S"

// Core types definition
#define TIMESTAMP_MAX		20
#define TIMESTAMP_FORMAT	"%Y%m%d-%H:%M:%S"


#endif /* TSSERVER_H_ */
