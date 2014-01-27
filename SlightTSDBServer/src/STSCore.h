/*
 * STScore.h
 *
 *  Created on: 15/gen/2014
 *      Author: fap
 */

#ifndef STSCORE_H_
#define STSCORE_H_

// standard C includes
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <time.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <pwd.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <pthread.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/time.h>
#include <dirent.h>

#include "STSLog.h"
// ------------  Data Type Definitions ------------

/* Type for data points */
#ifdef TSM_DOUBLE_TYPE
typedef double tsm_data_t;
#else
typedef float tsm_data_t;
#endif

/* Special value for passing a "don't care" timestamp by value */
/* Type for timestamp */
#ifdef TSM_LONG_TIMESTAMP
#define TSM_NO_TIMESTAMP	INT64_MAX
#define TSM_SCN SCNi64
#define TSM_PRI PRIi64
#define TSM_TIMESTAMP_SCN SCNi64
#define TSM_TIMESTAMP_PRI "f" // PRI64
#define TSM_TIMESTAMP_PRECISION 1000
typedef int64_t tsm_time;
#else
#define TSM_NO_TIMESTAMP	INT32_MAX
#define TSM_SCN SCNi32
#define TSM_PRI PRIi32
#define TSM_TIMESTAMP_SCN SCNi32
#define TSM_TIMESTAMP_PRI PRIi32
#define TSM_TIMESTAMP_PRECISION 1
typedef int32_t tsm_time;
#endif

// Access key definition -------------------------------------
// Typedef for key data
#define TSM_KEY_LENGTH		32
typedef uint8_t tsm_key_t[TSM_KEY_LENGTH];

// Typedef for a key slot
typedef struct {
	uint32_t flags;
	tsm_key_t key;
} tsm_key_info_t;

// Keys
typedef enum {
	tsmKey_Read = 0,
	tsmKey_Write,
	tsmKey_Max
} tsm_key_id_t;

// Size of key store
#define TSM_MAX_KEYS		((int)tsmKey_Max)
// -------------------------------------------------------

/* Data set metadata */
typedef struct {
	uint32_t	magic;				/*< Magic number - indicates TSM metadata file */
	uint32_t	version;			/*< Version identifier */
	uint64_t	dpsId;				/*< Node ID (should match filename) */
	uint32_t	nmetrics;			/*< Number of metrics in this data set */
	uint32_t	cacheDim;			/*< Number of seconds in the cache */
	uint32_t	minInterval;		/*< Interval in milli seconds between entries at the cache */
	uint32_t	decimation;			/*< Number of seconds for the downsampling  */
	uint32_t	flags[TSM_MAX_METRICS];	/*< Flags (for each metric) */
	tsm_key_info_t	key[TSM_MAX_KEYS];	/*< MAC keystore */

	uint32_t	DBmode;		/*< type of DB file management  */
	uint64_t	DBmaxFileLength;	/*< Max Dimension of DB file   */
	uint32_t	DBrecordDim;			/*< dimension of Record */
	uint64_t	DBmaxNumOfRecords;	/*< Max number of records in the file  */
	uint32_t	DBnSamples;			/*< Number of samples recorded */
	int64_t	DBstartTime;			/*< Timestamp of first entry in table (relative to epoch) */
	int64_t	DBendTime;			/*< Timestamp of last entry in table (relative to epoch) */
	uint64_t	DBfirstRecord;		/*< First Record in Circular Buffer   */
	uint64_t	DBlastRecord;			/*< Last Record in Circular Buffer   */
	uint32_t	DBstructType;	/*<Type of DB Structure > */
} tsm_metadata_t;


// Contex structure
#define TSM_MAX_FILEHANDLER	3
typedef struct {
	int  		meta_fd;					/*< File descriptor for metadata */
	FILE 		*table_fd[TSM_MAX_FILEHANDLER];				/*< File descriptors */
	tsm_metadata_t	*meta;					/*< Pointer to mmapped metadata */

	tsm_time	lastSampleTime;				/* Time of the last sample recorded */
	void		*cacheBufferPtr;			/* Pointer to the cache */
	uint32_t	cacheEntries;				/* Number of entries to the cache */
	uint32_t	sampleDim;					/* Dimension of one sample */
	uint32_t	firstEle;					/* Index of the first element in the cache */
	uint32_t	lastEle;					/* Index of the last element in the cache */

	tsm_time	startDownSampleTime;		/* The time stamp of the first downsampled sample */
	tsm_time	lastDownSampleTime;			/* The time stamp of the last downsampled sample */
	tsm_data_t	*sampleBuffer;				/* work buffer for decimation sample */
	tsm_data_t	*accumulatorBuffer;			/* work buffer for decimation totals */
	uint32_t	entries;					/* The number of downsampled samples */

	char	*recordIoBuffer;			/* Pointer to the I/O file buffer */
	struct timeval lastUse;				/* Time of the last take-contex */
	pthread_mutex_t	muIsDPoperated;			/*< Mutex for locking in multi-threaded applications */
} tsm_ctx_t;

// Context Manager
typedef struct {
	uint64_t	dpsId;				/*< Node ID (should match filename) */
	tsm_ctx_t  *ctx;					/* pointer to the opened Context */
} tsm_open_dp;



/*
 *  ===========  General Use Functions Prototypes ===============
 */
void *my_malloc(const char *caller, size_t  size);
void my_free(const char *caller, void *pointer);
void __dump_time(long param, char *mes);
tsm_time getNow(void);
char *tsmEpocToString(int64_t timestamp);
char *tsmEpocToStringShort(int32_t timestamp);

#endif /* STSCORE_H_ */
