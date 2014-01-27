/*
 * TSManager.h
 *
 *  Created on: 25/nov/2013
 *      Author: fap
 */

#ifndef TSMANAGER_H_
#define TSMANAGER_H_


#include <stdint.h>
#include <inttypes.h>

#include "cJSON.h"

//#include "SlightTSDB.h"
#include "STSAccess.h"

#define TSM_MAGIC_META		0x42445354 // TSM (little-endian)
#define TSM_VERSION		1

/* Flags to specify how to combine data points for downsampling */
typedef enum {
	tsmDownsample_Mean = 0,
	tsmDownsample_Median,
	tsmDownsample_Mode,
	tsmDownsample_Sum,
	tsmDownsample_Min,
	tsmDownsample_Max,
	tsmDownsample_Weight,
	tmsDownsample_None,
} tsm_downsample_mode_t;
#define TSM_DOWNSAMPLE_SHIFT	8
#define TSM_DOWNSAMPLE_MASK	15

/* Format for metadata filename ((uint64_t)node id) */
#define TSM_METADATA_FORMAT	"%016" PRIX64 ".tsm"

/* Format for table data filename ((uint64_t)node id, (unsigned int)layer) */
#define TSM_TABLE_FORMAT	"%016" PRIX64 "_%u_.dat"
#define TSM_TABLE_SWITCH_FORMAT	"%016" PRIX64 "_%u_%s.dat"

/* Max size for generated paths */
#define TSM_MAX_PATH		256

/* Store DB Mode */
typedef enum {
	tsmDBFile_Nolimit = 0,
	tsmDBFile_Circular,
	tsmDBFile_Switched
} tsm_storeDb_mode_t;

/* Size of db file  */
#define TSM_MAX_DBFILEMODE		((int)tsmDBFile_Switched)

#define TSM_END_OF_RECORD		0xAF // just a tappo now


// Name/value pairs for returning series
typedef struct {
	tsm_time	timestamp;
	tsm_data_t	value;
} tsm_series_point_t;

// Define that specify the compiled version type
#ifdef TSM_DOUBLE_TYPE
#define TSM_DBDOUBLE 1
#else
#define TSM_DBDOUBLE 0
#endif
#ifdef TSM_LONG_TIMESTAMP
#define TSM_DBLONGTIME 1
#else
#define TSM_DBLONGTIME 0
#endif
#define TSM_DBSTRUCTTYPE (TSM_DBDOUBLE*4 + TSM_DBLONGTIME*2 + 1)

#define isDBstructDouble(a) ( (a & 4) != 0 ? TRUE : FALSE)
#define isDBstructLongTime(a) ( (a & 2) != 0 ? TRUE : FALSE)
// -----------------


// --------------------   default setting --------------
// 1 second
#define  TS_DEFAULT_SAMPLEINTERVAL 1000
// 60 seconds
#define  TS_DEFAULT_CACHEDIM 60
#define  TS_DEFAULT_METRICS 1
// 15 seconds
#define  TS_DEFAULT_DECIMATION 15

// ------------------------------------------------------


/*
 *  ===========  Particular Functions Prototypes ===============
 */
/*!
 * \brief		Search for an opened DP file
 * \param dpsId	data Point to manage
 * \return		The pointer ox related context or NULL
 */
tsm_ctx_t * __tmsSearchContex(uint64_t dpsId);

/*!
 * \brief		Store a new active context into the table
 * \param 	dpsId	Data Point id to create
 * \param	ctx		The pointer to the context
 * \return		the index of table -1 if fails
 */
int __storeNewContext(uint64_t dpsId, tsm_ctx_t *ctx);

/*!
 * \brief		Remove a context from the table
 * \param 	dpsId	Data Point id to remove
 * \return
 */
void __removeContext(uint64_t dpsId);

/*!
 * \brief		Clean the Contexts table...
 * \param
 * \return
 */
void __tsmGarbageContex();

/*!
 * \brief		Get the number of opened Contexts
 * \param
 * \return		number of opened contexts
 */
int __tmsGetOpenedContexs();

/*!
 * \brief		Creates a new time series database
 * \param node_id	Node to create
 * \param minInterval  The period at maximum rate in cache (milliseconds)
 * \param cacheDim  Number of seconds of cache
 * \param nmetrics	Number of metrics to allocate for this node
 * \param ds_mode	Array per metric \see tsm_downsample_mode_t
 * \param decimation	Interval in seconds for the decimation
 * \param dbmode	Type of the DB file management
 * \param maxfilelength	Max DB file dimension
 * \return		0 or negative error code
 */
int tsmCreate(uint64_t dpsId,unsigned int minInterval,unsigned int cacheDim,unsigned int nmetrics,
		tsm_downsample_mode_t *ds_mode,unsigned int decimation,unsigned int dbmode,unsigned long maxfilelength);


/*!
 * \brief		Deletes an existing time series database
 * \param dpsId	Data Point Id to delete
 * \return		0 or negative error code
 */
int tsmDelete(uint64_t dpsId);

/*!
 * \brief 		Opens an existing time series database
 * \param dpsId	Data Point Id to open
 * \return		Pointer to context structure or null on error
 */
tsm_ctx_t* tsmOpen(uint64_t dpsId);

/*!
 * \brief 		Opens the DB file
 * \param ctx	Pointer to context structure
 * \return		0 or negative on error
 */
int __tsmOpenTableFile(tsm_ctx_t *ctx);

/*!
 * \brief 		Opens the Meta Data file
 * \param ctx	Pointer to context structure
 * \param dpsId	Data Point Id to delete
 * \return		0 or negative on error
 */
int __tsmOpenMetaData(tsm_ctx_t *ctx, uint64_t dpsId);

/*!
 * \brief 		Checks the consistency of Meta Data info
 * \param ctx	Pointer to context structure
 * \param dpsId	Data Point Id to delete
 * \return		0 or negative on error
 */
int __tsmCheckMetadata(tsm_ctx_t *ctx, uint64_t dpsId);

/*!
 * \brief 		Dumps in the Debug log all the metadata infos
 * \param ctx	Pointer to context structure
 */
void __tsmDumpMetaData(tsm_ctx_t *ctx);


/*!
 * \brief		Closes a time series database opened by a call to tsm_open
 * \param ctx		Pointer to context structure returned by tsm_open
 */
void *tsmClose(tsm_ctx_t *ctx);

/*!
 * \brief		Returns the UNIX timestamp of the latest time point
 * \param ctx		Pointer to context structure returned by tsm_open
 * \return		UNIX timestamp of the most recent time point or TSM_NO_TIMESTAMP if no data
 */
tsm_time tsmGetLatest(uint64_t dpsId);
tsm_time __tsmGetLatest(tsm_ctx_t *ctx);

/*!
 * \brief		Store the new n-upla of values for a DataPoint
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param timestamp	Pointer to variable with the UNIX timestamp.
 * \param values	Pointer to an array of values.
 * \return		0 on success or a negative error code
 */
int tsmStoreDataPoint(uint64_t dpsId, tsm_time *timestamp, tsm_data_t *values);
int __tsmStoreDataPoint(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values);

/*!
 * \brief		Execute the Downsampling and stores the vales into the DB
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param timestamp	Pointer to variable with the UNIX timestamp.
 * \param values	Pointer to an array of values.
 */
void __tsmDownSampleValue(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values);

/*!
 * \brief		Reset the buffer for the downsampling
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param timestamp	Pointer to variable with the UNIX timestamp.
 */
void __tsmResetDownSampleBuffer(tsm_ctx_t *ctx, tsm_time *timestamp);

/*!
 * \brief		Store one sample into the DB table
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param timestamp	Pointer to variable with the UNIX timestamp.
 * \param values	Pointer to an array of values.
 * \return		0 on success or a negative error code
 */
int __tsmStoreSampleIntoDB(tsm_ctx_t *ctx, tsm_time timestamp, tsm_data_t *value);


/*!
 * \brief		Check the cache circular buffer is full
 * \param ctx		Pointer to context structure returned by tsm_open
 * \return		TRUE or FALSE
 */
int __tsmIsCacheFull(tsm_ctx_t *ctx);

/*!
 * \brief		Check the cache circular buffer is empty
 * \param ctx		Pointer to context structure returned by tsm_open
 * \return		TRUE or FALSE
 */
int __tsmIsCacheEmpty(tsm_ctx_t *ctx);

/*!
 * \brief		Push one dp sample in the cache circular buffer
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param timestamp	Pointer to variable with the UNIX timestamp.
 * \param values	Pointer to an array of values.
 */
void __tsmCacheWrite(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values);

/*!
 * \brief		Pop one dp sample from cache circular buffer
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param timestamp	Pointer to variable with the UNIX timestamp.
 * \param values	Pointer to an array of values.
 */
void __tsmCacheRead(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values);

/*!
 * \brief		Search the first sample in the cache that have the timestamp less or equal the param,
 *              the returned pointer is valid ever, but the contents could vary if a push in the cache
 *              override the buffer !
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param timestamp	Variable with the UNIX timestamp.
 * \return values	Pointer to a Data sample structure (timestamp and array of values).
 * \return 	Index into the circular buffer .
 */
uint32_t __tsmSearchValueInCache(tsm_ctx_t *ctx, tsm_time timestamp, tsm_series_point_t **values);
uint32_t __tsmNextValueInCache(tsm_ctx_t *ctx, uint32_t index, tsm_series_point_t **ptrS);
uint32_t __tsmPreviousValueInCache(tsm_ctx_t *ctx, uint32_t index, tsm_series_point_t **ptrS);

/*!
 * \brief		Search the first sample in the DB table that have the timestamp less or equal the param,
 *              the returned pointer is the offset inside the DB. The function return olso the read record
 *              in the memory pointed by the buffer ppointer parma
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param timestamp	Pointer to variable with the UNIX timestamp.
 * \param buffer	Pointer to a memory space used as buffer to read form file.
 * \param bound		boundary spec. 1=left 2=right 0=match
 * \return 	Offset in records in the DB file, -1 on error.
 */
int64_t __tsmSearchValueInDB(tsm_ctx_t *ctx, tsm_time timestamp, void *buffer, int bound);

int64_t __tsmNextValueInDB(tsm_ctx_t *ctx, int64_t filePtr, void *buffer);
int64_t __tsmPreviuosValueInDB(tsm_ctx_t *ctx, int64_t filePtr, void *buffer);
int64_t __tsmSkipValueInDB(tsm_ctx_t *ctx, int64_t filePtr, void *buffer, int dir);


/*!
 * \brief		Returns the specified timestamped values for all metrics in the data set
 *
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param timestamp	Pointer to variable with the UNIX timestamp of the required time point.
 * 			May be modified with a value rounded up to the nearest sample.
 * \param values	Pointer to an array to be updated with the time point's values.
 * \return		0 on success or a negative error code
 */
char *tsmGetDPValue(uint64_t dpsId, tsm_time *timestamp, char **outBuffer, long maxsize);
int __tsmGetDPValue(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values);

/*!
 * \brief		Returns an array of values for one metric from one data point)
 *
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param metricId	ID of metric to return
 * \param startTime		UNIX timestamp for the start of the period of interest (or TSM_NO_TIMESTAMP)
 * \param endTime		UNIX timestamp for the end of the period of interest (or TSM_NO_TIMESTAMP)
 * \param maxEntries	Maximum number of values to output
 * \param series	Pointer to an array to be updated with the result set.
 * \return		Number of points returned on success or a negative error code
 */
char *tsmGetOneSerie(uint64_t dpsId, unsigned int metricId, tsm_time startTime, tsm_time endTime,
		unsigned int maxEntries, char **outBuffer, long maxsize);
int __tsmGetOneSerie(tsm_ctx_t *ctx, unsigned int metricId, tsm_time startTime, tsm_time endTime,
		unsigned int maxEntries, char **outBuffer,long maxsize);


/*!
 * \brief		Returns an array of values for one metric from one data point read from cache
 *
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param metricId	ID of metric to return
 * \param startTime		UNIX timestamp for the start of the period of interest (or TSM_NO_TIMESTAMP)
 * \param endTime		UNIX timestamp for the end of the period of interest (or TSM_NO_TIMESTAMP)
 * \param maxEntries	Maximum number of values to output
 * \param series	Pointer to an array to be updated with the result set.
 * \return		Number of points returned on success or a negative error code
 */
long __extractSerieFromCache(tsm_ctx_t *ctx,unsigned int metricId,tsm_time startTime, tsm_time endTime,
		unsigned int maxEntries,char **outBuffer,long *maxsize);


/*!
 * \brief		Returns an array of values for one metric from one data point read from DB
 *
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param metricId	ID of metric to return
 * \param startTime		UNIX timestamp for the start of the period of interest (or TSM_NO_TIMESTAMP)
 * \param endTime		UNIX timestamp for the end of the period of interest (or TSM_NO_TIMESTAMP)
 * \param maxEntries	Maximum number of values to output
 * \param series	Pointer to an array to be updated with the result set.
 * \return		Number of points returned on success or a negative error code
 */
long __extractSerieFromDB(tsm_ctx_t *ctx,unsigned int metricId,tsm_time startTime, tsm_time endTime,
		unsigned int maxEntries,char **outBuffer,long *maxsize);

/*!
 * \brief			Returns a key from the database metadata
 *
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param key_id	ID of key to return
 * \param key		Pointer to key to be populated
 *
 * \return			0 on success, -EINVAL on bad key ID, -ENOENT if no key defined
 */
int tsmGetKey(tsm_ctx_t *ctx, tsm_key_id_t key_id, tsm_key_t *key);

/*!
 * \brief			Writes a key to the database metadata
 *
 * \param ctx		Pointer to context structure returned by tsm_open
 * \param key_id	ID of key to set
 * \param key		Pointer to key data to be written to database or NULL to remove
 *
 * \return			0 on success, -EINVAL on bad key ID
 */
int tsmSetKey(tsm_ctx_t *ctx, tsm_key_id_t key_id, tsm_key_t *key);


int tsmGetDPList(char **dpList, long maxsize);
char *tsmDumpMetaData(uint64_t dpsId, char **outBuffer, long maxsize);
int __tsmCorrectDBStruct(tsm_ctx_t *ctx);


#endif /* TSMANAGER_H_ */
