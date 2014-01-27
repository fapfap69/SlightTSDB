/*
 * STSManager.c
 *
 *  Created on: 25/nov/2013
 *      Author: fap
 */


//#include "cJSON.h"
#include "profile.h"

#include "SlightTSDB.h"
#include "STSCore.h"
#include "STSLog.h"
#include "STSAccess.h"

#include "STSManager.h"

extern tsm_open_dp activeDp[];  // the array that manage the active dps

// -----------------------------------------------------
// Functions for the Context management
//

/*
 *  The  wrapper for the tsmClose function... a casting matter
 */
void *__theTerminator( void *param) {
	tsmClose((tsm_ctx_t *)param);
	return(NULL);
}

/*
* This analyze the context table for sleeping streams
* if there are try to close they, optimized use of resources
*/
void __tsmGarbageContex() {
	int i,ret;
	struct timeval now, lastused;
	gettimeofday(&now, NULL);
    pthread_t terminator;

	for(i = 0; i < TSM_MAX_ACTIVEDP; i++){  // scan the context table
		if(activeDp[i].dpsId != 0 && activeDp[i].ctx != NULL) { // is a good entry
			lastused.tv_sec = activeDp[i].ctx->lastUse.tv_sec;
			lastused.tv_usec = activeDp[i].ctx->lastUse.tv_usec;
			if(now.tv_sec > lastused.tv_sec + TSM_MAXUNUSEDTIME) { // this could be a candidate to auto close

				// executes the Close in a child thread in order to ensure the non blocking
		        pthread_create(&terminator, NULL, __theTerminator, (void *)activeDp[i].ctx);

		        ret = 0; // wait for max 100 msec (20x5)
		        while(ret++ < 20) {
		        	usleep(5000); // 5 msec
		        	if (pthread_kill(terminator, 0) == ESRCH) { // fake signal that check the running state
		        		// Ok it's terminated
		        		DEBUG("Slight Manager : The Context was Closed in %d millisec !\n", ret*5);
		        		ret = 999; // this flags the exit
		        	}
		        }
		        // Now if the thread is still running ...
		        if (ret < 99 && pthread_kill(terminator, 0) != ESRCH) { // fake signal that check the running state
	        		// GULP there are problems ! try to terminate the thread
	        		if( pthread_cancel(terminator) == ESRCH) { // nothing else to do SOB !
	        			ERROR("__tsmGarbageContex :: Failed to kill the tsmClose thread !\n");
	        		} else {
	        			ERROR("__tsmGarbageContex :: We kill the tsmClose thread after 100 msec !\n");
	        		}
			        // In all cases removes the entry from table, we leave a Zombie child
					activeDp[i].ctx = NULL;
					activeDp[i].dpsId = 0;
		        }
			}
		}
	}
	return;
}
/*
* Search for the context associated to a Data Point ID
* return the pointer, NULL if it not exists
*/
tsm_ctx_t * __tmsSearchContex(uint64_t dpsId) {
	int i;
	for(i = 0; i < TSM_MAX_ACTIVEDP; i++){
		if(activeDp[i].dpsId == dpsId) {
			DEBUG("Slight Manager : The Context %d founded !\n", dpsId);
			return(activeDp[i].ctx);
		}
	}
	DEBUG("Slight Manager : The Context %d not found !\n", dpsId);
	return(NULL);
}
/*
* Store the new context in the first free entry of the table
* return the position of -1 for error
*/
int __storeNewContext(uint64_t dpsId, tsm_ctx_t *ctx) {
	int i;
	for(i = 0; i < TSM_MAX_ACTIVEDP; i++){
		if(activeDp[i].dpsId == 0) {
			activeDp[i].ctx = ctx;
			activeDp[i].dpsId = dpsId;
			DEBUG("Slight Manager : The New Context %d was stored !\n", dpsId);
			return i;
		}
	}
	ERROR("Slight Manager : The New Context %d do NOT stored, no empty rooms in table !\n", dpsId);
	return(-1);
}
/*
* Remove the context from the table
*/
void __removeContext(uint64_t dpsId) {
	int i;
	for(i = 0; i < TSM_MAX_ACTIVEDP; i++){
		if(activeDp[i].dpsId == dpsId) {
			activeDp[i].ctx = NULL;
			activeDp[i].dpsId = 0;
			DEBUG("Slight Manager : The Context %d was removed !\n", dpsId);
			return;
		}
	}
	WARNING("Slight Manager : The Context %d don't exists for deletion !\n", dpsId);
	return;
}
/*
 *  Returns the number of the opened streams
 */
int __tmsGetOpenedContexs() {
	int i,c;
	for(i = 0, c=0; i < TSM_MAX_ACTIVEDP; i++)
		if(activeDp[i].dpsId != 0)
			c++;;
	DEBUG("Slight Manager : The Context Table contains %d opened elements !\n", c);
	return(c);
}
// -------------------------------------------------------


/* ================  DATA POINT Functions ===================== */

/*
* Create a Data Point stream structure
* returns 0 if is ok, otherwise error
*/
int tsmCreate(uint64_t dpsId,
				unsigned int minInterval,  // the period at maximum rate in cache (milliseconds)
				unsigned int cacheDim, // number of seconds of cache
				unsigned int nmetrics, //  number of measurement for dp
				tsm_downsample_mode_t *ds_mode, // types of downsamples
				unsigned int decimation, // number of seconds for the decimation
				unsigned int dbmode, // Type of DB file management
				unsigned long maxfilelength) // Maximum length of DB files
{
	FUNCTION_TRACE;

	tsm_metadata_t md;
	char path[TSM_MAX_PATH];
	int n, fd;

	// Create metadata file only if it does not already exist
	snprintf(path, TSM_MAX_PATH, TSM_METADATA_FORMAT, dpsId);
	DEBUG("DataPoint %016" PRIX64 " metadata path: %s\n", dpsId, path);
	fd = open(path, O_RDWR | O_EXCL | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) {
		ERROR("Error creating metadata %s: %s\n", path, strerror(errno));
		return -errno;
	}
	// Populate fresh metadata
	INFO("Empty metadata - populating new dataset for the dpId:%X \n", dpsId);
	memset(&md, 0, sizeof(md));
	md.magic = TSM_MAGIC_META;
	md.version = TSM_VERSION;
	md.dpsId = dpsId;
	md.nmetrics = (uint32_t)nmetrics;

	md.cacheDim = cacheDim;
	md.minInterval = (uint32_t)minInterval;
	for (n = 0; n < nmetrics; n++) {
		md.flags[n] = (((uint32_t)ds_mode[n] << TSM_DOWNSAMPLE_SHIFT));
	}
	md.decimation = decimation * TSM_TIMESTAMP_PRECISION;

	md.DBmode = dbmode;
	md.DBrecordDim = sizeof(tsm_time) + (sizeof(tsm_data_t) * nmetrics) + 1; // Time + values(0:n-1) + FlagByte
	md.DBmaxFileLength = maxfilelength;
	md.DBmaxNumOfRecords = floor(maxfilelength / md.DBrecordDim) -1; // in order to have a little empty tail
	md.DBstartTime = 0;
	md.DBendTime = 0;
	md.DBfirstRecord = 0;
	md.DBlastRecord = 0;
	md.DBnSamples = 0;
	md.DBstructType = TSM_DBSTRUCTTYPE;

	write(fd, &md, sizeof(tsm_metadata_t));
	DEBUG("The dataset for the dpId=%d is created !\n", dpsId);
	close(fd);

	return 0;
}

/*
* Delete the Data Point structure
* 0 if ok otherwise error
*/
int tsmDelete(uint64_t dpsId)
{
	FUNCTION_TRACE;

	char path[TSM_MAX_PATH];

	// First of all close the stream
	tsm_ctx_t *ctx = __tmsSearchContex(dpsId);
	if(ctx != NULL)	tsmClose(ctx);

	// Delete metadata file
	snprintf(path, TSM_MAX_PATH, TSM_METADATA_FORMAT, dpsId);
	DEBUG("Datapoint Id = %016" PRIX64 " metadata path is: %s\n", dpsId, path);
	if (unlink(path) < 0) {
		ERROR("Failed to unlink %s\n", path);
		return -errno;
	}

	// Delete layer data - stop on first failure
	snprintf(path, TSM_MAX_PATH, TSM_TABLE_FORMAT, dpsId, 0);
#ifdef TSM_REMOVE_DATAFILES
	if (unlink(path) < 0) {
		ERROR("tsmDelete :: Failed to unlink %s\n", path);
		return -errno;
	}
	DEBUG("Datapoint Id = %016" PRIX64 " layer %u Table File path: %s REMOVED !\n", dpsId, 0, path);

#endif
	INFO("Delete datapoint dpId:%X without removing Data File !\n", dpsId);
	return 0;
}


/*
 *   Open the Data Point stream context
 *   returns the context pointer to the opened stream, or NULL on error
 *
 *   Allocation : the context of the DP stream, the cache buffer
 *                the DB record IO buffer, the Down-sampling buffers
 */
tsm_ctx_t * tsmOpen(uint64_t dpsId)
{
	FUNCTION_TRACE;

	//	If the dp is closed open it or return the context
	tsm_ctx_t *ctx;
	ctx = __tmsSearchContex(dpsId);
	if(ctx != NULL) return(ctx);

	// allocate the context
	ctx = (tsm_ctx_t*)my_malloc(__FUNCTION__,sizeof(tsm_ctx_t));
	if (ctx == NULL) {
		CRITICAL("Allocating the context : out of memory !\n");
		return NULL;
	}

	// allocate the mutex
	if(pthread_mutex_init(&ctx->muIsDPoperated,NULL) != 0) {
		CRITICAL("Mutex creation failed ! \n");
		goto fail;
	}

	// Now opens the files ...
	if(__tsmOpenMetaData(ctx, dpsId) < 0) goto fail;
	if(__tsmCheckMetadata(ctx, dpsId) < 0) goto fail;
	if(__tsmOpenTableFile(ctx) < 0) goto fail;
	__tsmDumpMetaData(ctx);  // dumps only in DEBUG level

	// Allocate the cache buffer
	ctx->sampleDim = sizeof(tsm_time) + (sizeof(tsm_data_t) * ctx->meta->nmetrics); // dimension of each sample
	ctx->cacheEntries = (int)floor( (float)ctx->meta->cacheDim / ((float)ctx->meta->minInterval / 1000.0) ) + 2;
	ctx->cacheBufferPtr = my_malloc(__FUNCTION__,ctx->cacheEntries * ctx->sampleDim + 10);  // extra byte
	if (ctx->cacheBufferPtr == NULL) {
		CRITICAL("Allocating the cache buffer : out of memory !\n");
		goto fail;
	}
	ctx->firstEle = 0;
	ctx->lastEle = 0;

	// Set the DB interface
	ctx->recordIoBuffer = my_malloc(__FUNCTION__,ctx->meta->DBrecordDim + 2);  // extra byte
	if (ctx->recordIoBuffer == NULL) {
		CRITICAL("Allocating the record I/O buffer : out of memory !\n");
		goto fail;
	}

	// set the timing & synchronization context
	ctx->lastSampleTime = __tsmGetLatest(ctx);
	gettimeofday(&ctx->lastUse, NULL);

	// Set the decimation frame
	ctx->accumulatorBuffer = my_malloc(__FUNCTION__,sizeof(tsm_data_t) * ctx->meta->nmetrics);
	if (ctx->accumulatorBuffer == NULL) {
		CRITICAL("Allocating the decimation buffer : out of memory !\n");
		goto fail;
	}
	ctx->sampleBuffer = my_malloc(__FUNCTION__,sizeof(tsm_data_t) * ctx->meta->nmetrics);
	if (ctx->sampleBuffer == NULL) {
		CRITICAL("Allocating the sample buffer : out of memory !\n");
		goto fail;
	}
	tsm_time timestamp = time(NULL);
	__tsmResetDownSampleBuffer(ctx, &timestamp);

	// store the context to the main table
	if(__storeNewContext(dpsId, ctx) >= 0) {
		INFO("Open context for Datapoint dpId:%X \n", dpsId);
		return(ctx);
	}
	CRITICAL("No Context entries for new active Datapoint !\n");
	goto fail;


fail:
	DEBUG("Close the datapoint context due to critical error !\n");
	tsmClose(ctx);
	return NULL;
}

/*
 *  Opens the DB file related to the context
 *  returns 0 if OK, otherwise on error
 */
int __tsmOpenTableFile(tsm_ctx_t *ctx) {

	FUNCTION_TRACE;

	char path[TSM_MAX_PATH];

	snprintf(path, TSM_MAX_PATH, TSM_TABLE_FORMAT, ctx->meta->dpsId, 0);
	DEBUG("Datopoint =%016" PRIX64 " layer=%u  table path: %s\n", ctx->meta->dpsId, 0, path);

	// clear the files handler table
	int i;
	for ( i = 0; i < TSM_MAX_FILEHANDLER; i++) {
		ctx->table_fd[i] = 0;
	}

	if( access( path, F_OK ) == -1 ) { // file doesn't exists
		ctx->table_fd[0] = fopen(path, "wb");
		if (ctx->table_fd[0] <= 0) {
			ERROR("__tsmOpenTableFile :: Error creating table file for dp %016" PRIX64 " layer %d: %s\n",ctx->meta->dpsId, 0, strerror(errno));
			return(-errno);
		}
		fclose(ctx->table_fd[0]);
	}
	// Now try to pen in r/w mode
	ctx->table_fd[0] = fopen(path, "rb+");
	if (ctx->table_fd[0] <= 0) {
		ERROR("__tsmOpenTableFile :: Error opening table for dp %016" PRIX64 " layer %d: %s\n",ctx->meta->dpsId, 0, strerror(errno));
		return(-errno);
	}
	return(0);
}

/*
 *  Opens the DP MetaData file
 *  returns 0 if OK, otherwise on error
 *
 *  Allocation : the file is memory mapped !
 */
int __tsmOpenMetaData(tsm_ctx_t *ctx, uint64_t dpsId) {

	FUNCTION_TRACE;

	struct stat st;
	char path[TSM_MAX_PATH];

	snprintf(path, TSM_MAX_PATH, TSM_METADATA_FORMAT, dpsId);
	DEBUG("Datapoint %016" PRIX64 " metadata path: %s\n", dpsId, path);
	ctx->meta_fd = open(path, O_RDWR);
	if (ctx->meta_fd < 0) {
		ERROR("Error opening metadata %s: %s\n", path, strerror(errno));
		return(-errno);
	}
	fstat(ctx->meta_fd, &st);
	if (st.st_size && st.st_size != sizeof(tsm_metadata_t)) {
		ERROR("Corrupt metadata\n");
		return(-errno);
	}
	ctx->meta = mmap(NULL, sizeof(tsm_metadata_t), PROT_READ | PROT_WRITE, MAP_SHARED, ctx->meta_fd, 0);
	if (ctx->meta == MAP_FAILED) {
		ERROR("mmap failed on file %s: %s\n", path, strerror(errno));
		return(-errno);
	}
	return(0);
}

/*
 *  Dumps on the Debug output the contents of the Metadata file
 */
void __tsmDumpMetaData(tsm_ctx_t *ctx) {
	int n = 0;
	DEBUG("TS Version = %" PRIu32 "\n", ctx->meta->version);
	DEBUG("  magic = 0x%08" PRIX32 "\n", ctx->meta->magic);
	DEBUG("Data Point Id = 0x%016" PRIX64 "\n", ctx->meta->dpsId);
	DEBUG("Metrics = %" PRIu32 "\n", ctx->meta->nmetrics);
	DEBUG("Min. acquisition period = %" PRIu32 "\n", ctx->meta->minInterval);
	DEBUG("Cache dimension = %" PRIu32 "\n", ctx->meta->cacheDim);
	DEBUG("Down sampling (sec) = %" PRIu32 "\n", ctx->meta->decimation);
	for (n = 0; n < TSM_MAX_METRICS; n++)
		DEBUG("  flags[%d] = 0x%08" PRIX32 "\n", n, ctx->meta->flags[n]);
	DEBUG("DB Mode = %d \n", ctx->meta->DBmode);
	DEBUG(" Max file dimension = %l\n", ctx->meta->DBmaxFileLength);
	DEBUG(" Max number of records = %l\n", ctx->meta->DBmaxNumOfRecords);
	DEBUG(" Record dimension = %d\n", ctx->meta->DBrecordDim);
	DEBUG(" Number of samples = %l\n", ctx->meta->DBnSamples);
	DEBUG(" Start time = %14.3f\n", ctx->meta->DBstartTime / 1000.0);
	DEBUG(" End time = %14.3f\n", ctx->meta->DBendTime / 1000.0);
	DEBUG(" Index to first = %l\n", ctx->meta->DBfirstRecord);
	DEBUG(" Index to last = %l\n", ctx->meta->DBlastRecord);
	return;
}

/*
 *   Checks the content of the Meta Data File
 *   returns 0 if OK otherwise on error
 */
int __tsmCheckMetadata(tsm_ctx_t *ctx, uint64_t dpsId) {

	FUNCTION_TRACE;

	if (ctx->meta->magic != TSM_MAGIC_META) {
		ERROR("Bad magic number\n");
		return(-1);
	}
	if (ctx->meta->version != TSM_VERSION) {
		ERROR("Bad database version\n");
		return(-1);
	}
	if (ctx->meta->dpsId != dpsId) {
		ERROR("Incorrect dpsId - possible data corruption\n");
		return(-1);
	}
	if (ctx->meta->DBstructType != TSM_DBSTRUCTTYPE) {
		ERROR("__tsmCheckMetadata :: Incorrect DB Structure - use a different compiled version or convert the DB !\n");
#ifdef TSM_AUTOCORRECT_DB_STRUCT
		if(__tsmCorrectDBStruct(ctx) != 0) {
			ERROR("Incorrect DB Structure - Fails to convert the structure !\n");
			return(-1);
		}
#else
		return(-1);
#endif
	}

	/* FIXME: Add the check for DB infos ! */
	DEBUG("Metadata for datapoint dpId=%d are correct !\n", dpsId);
	return(0);
}

/*
 *  Correct the DB structure with the actual compiled type
 */
int __tsmCorrectDBStruct(tsm_ctx_t *ctx) {

	// open the files
	char pathOut[TSM_MAX_PATH];
	ctx->table_fd[1] = 0;
	snprintf(pathOut, TSM_MAX_PATH, TSM_TABLE_FORMAT, ctx->meta->dpsId, 1);
	ctx->table_fd[1] = fopen(pathOut, "wb");
	if (ctx->table_fd[1] <= 0) {
		ERROR("Error creating table output file for DB conversion. (%s)\n",strerror(errno));
		return -1;
	}
	char pathIn[TSM_MAX_PATH];
	ctx->table_fd[0] = 0;
	snprintf(pathIn, TSM_MAX_PATH, TSM_TABLE_FORMAT, ctx->meta->dpsId, 0);
	ctx->table_fd[0] = fopen(pathIn, "rb");
	if (ctx->table_fd[0] <= 0) {
		ERROR("__tsmOpenTableFile :: Error creating table file for dp %016" PRIX64 " layer %d: %s\n",ctx->meta->dpsId, 0, strerror(errno));
		fclose(ctx->table_fd[1]);
		return(-1);
	}

	// Set dimensions and allocate buffers
	char *ptrSour, *ptrDest;
	char *prBufIn = NULL;
	char *prBufOut = NULL;
	int nMetrics = ctx->meta->nmetrics;

	int isDouble = isDBstructDouble(ctx->meta->DBstructType) ;
	int isLongTime = isDBstructLongTime(ctx->meta->DBstructType) ;
	prBufIn = my_malloc(__FUNCTION__,ctx->meta->DBrecordDim + 4);
	if (prBufIn == NULL) {
		CRITICAL("Allocate the conversion input buffer out of memory\n");
		goto failProc;
	}

	int isNewDouble = isDBstructDouble(TSM_DBSTRUCTTYPE) ;
	int isNewLongTime = isDBstructLongTime(TSM_DBSTRUCTTYPE) ;
	int newRecordBuffer = (isNewLongTime ? sizeof(int64_t) : sizeof(int32_t) ) + ((isNewDouble ? sizeof(double) : sizeof(float)) * nMetrics ) +1;
	prBufOut = my_malloc(__FUNCTION__,ctx->meta->DBrecordDim + 4);
	if (prBufIn == NULL) {
		CRITICAL("Allocate the conversion output buffer out of memory\n");
		goto failProc;
	}

	// And now we loop throw the file ...
	int reby,wrby,i;
	reby = fread(prBufIn, 1, ctx->meta->DBrecordDim, ctx->table_fd[0]);
	while(!feof(ctx->table_fd[0])) {
		if(reby != ctx->meta->DBrecordDim) {
			ERROR("Error reading DB! \n");
			goto failProc;
		}
		ptrSour = prBufIn;
		ptrDest = prBufOut;
		// convert the time stamp
		if((isNewLongTime && isLongTime) || (!isNewLongTime && !isLongTime)) {
			*((tsm_time *)ptrDest) = *((tsm_time *)ptrSour);
			ptrDest += sizeof(tsm_time);
			ptrSour += sizeof(tsm_time);
		} else if(!isNewLongTime && isLongTime) {
			*((tsm_time *)ptrDest) = ( (int32_t)(((double)(*((int64_t *)ptrSour))) / 1000.0) );
			ptrDest += sizeof(tsm_time);
			ptrSour += sizeof(int64_t);
		} else if(isNewLongTime && !isLongTime) {
			*((tsm_time *)ptrDest) = ( (int64_t)(*((int32_t *)ptrSour)) * 1000);
			ptrDest += sizeof(tsm_time);
			ptrSour += sizeof(int32_t);
		}
		// convert the data fields
		for(i=0;i<nMetrics;i++){
			if((isNewDouble && isDouble) || (!isNewDouble && !isDouble)) {
				*((tsm_data_t *)ptrDest) = *((tsm_data_t *)ptrSour);
				ptrDest += sizeof(tsm_data_t);
				ptrSour += sizeof(tsm_data_t);
			} else if(!isNewDouble && isDouble) {
				*((tsm_data_t *)ptrDest) = ( (float)( *((double *)ptrSour) ) );
				ptrDest += sizeof(tsm_data_t);
				ptrSour += sizeof(double);
			} else if(isNewDouble && !isDouble) {
				*((tsm_data_t *)ptrDest) = ( (double)( *((float *)ptrSour) ) );
				ptrDest += sizeof(tsm_data_t);
				ptrSour += sizeof(float);
			}
		}
		// copy the terminator byte
		*ptrDest = *ptrSour;
		//  write the output buffer --
		wrby = fwrite(prBufOut, 1, newRecordBuffer, ctx->table_fd[1]);
		if(wrby != newRecordBuffer) {
			ERROR("Write a wrong number of bytes = %d !\n", wrby);
			goto failProc;
		}
		// A new read ...
		reby = fread(prBufIn, 1, ctx->meta->DBrecordDim, ctx->table_fd[0]);
	}

	// Close the files & free the memory
	my_free(__FUNCTION__,prBufOut);
	my_free(__FUNCTION__,prBufIn);
	fclose(ctx->table_fd[1]); ctx->table_fd[1] = 0;
	fclose(ctx->table_fd[0]); ctx->table_fd[0] = 0;

	// exchange the files names
	char pathOld[TSM_MAX_PATH];
	snprintf(pathOld, TSM_MAX_PATH, TSM_TABLE_FORMAT, ctx->meta->dpsId, 9);
	rename(pathIn, pathOld);
	rename(pathOut, pathIn);

	// adjust the meta data
	ctx->meta->DBstructType = TSM_DBSTRUCTTYPE;
	ctx->meta->DBrecordDim = newRecordBuffer;
	if(!isNewLongTime && isLongTime) {
		ctx->meta->decimation = ctx->meta->decimation / 1000;
	} else if(isNewLongTime && !isLongTime) {
		ctx->meta->decimation = ctx->meta->decimation * 1000;
	}
	msync(ctx->meta, sizeof(tsm_metadata_t), MS_ASYNC);

	// end
	return(0);


failProc:
	if(prBufOut != NULL) my_free(__FUNCTION__,prBufOut);
	if(prBufIn != NULL) my_free(__FUNCTION__,prBufIn);
	fclose(ctx->table_fd[1]); ctx->table_fd[1] = 0;
	fclose(ctx->table_fd[0]); ctx->table_fd[0] = 0;
	return(-1);

}


/*
 *  Closes the files and releases all the memory spaces allocated
 */
void *tsmClose(tsm_ctx_t *ctx)
{
	FUNCTION_TRACE;

	// resolve particular cases
	if(ctx == NULL) return(NULL);  // Close a ghost ... what we loose ?
	if(ctx->meta == NULL) {		// Release only the context... the metadata missing :(
		my_free(__FUNCTION__,ctx);
		DEBUG("The metadata field pointer for dpId=?? is NULL... suspect corruption \n");
		return(NULL);
	}

	// Now lock the stream in order to ensure the normal termination
	pthread_mutex_lock(&ctx->muIsDPoperated);

	// Store the content of the Downsampling buffer into the DB
	if(ctx->entries > 0) {  // We have values
		__tsmStoreSampleIntoDB(ctx, (tsm_time)( (int64_t)((int64_t)ctx->startDownSampleTime+(int64_t)ctx->lastSampleTime)/2), ctx->accumulatorBuffer);
	}

	// Start to close
	uint64_t dpsId =ctx->meta->dpsId;
	INFO("De-activation of dpId=%X\n", dpsId);
	// Close any open table files
	unsigned int handler;
	for (handler = 0; handler < TSM_MAX_FILEHANDLER; handler++) {
		 if (ctx->table_fd[handler] > 0) { fclose(ctx->table_fd[handler]); }
	}
	// Close the metadata file
	if (ctx->meta_fd > 0) { close(ctx->meta_fd); }
	if (ctx->meta != NULL) { munmap(ctx->meta, sizeof(tsm_metadata_t)); } // Unmap
	// Free all allocated buffers
	if (ctx->cacheBufferPtr != NULL) { my_free(__FUNCTION__,ctx->cacheBufferPtr); }
	if (ctx->accumulatorBuffer != NULL) { my_free(__FUNCTION__,ctx->accumulatorBuffer); }
	if (ctx->sampleBuffer != NULL) { my_free(__FUNCTION__,ctx->sampleBuffer); }
	if (ctx->recordIoBuffer != NULL) { my_free(__FUNCTION__,ctx->recordIoBuffer); }
	// free the mutex
	pthread_mutex_destroy(&ctx->muIsDPoperated);
	// Release context
	my_free(__FUNCTION__,ctx);
	// Remove the context from the active list
	__removeContext(dpsId);
	return(NULL);
}

/*
* Return the timestamp of the last sample recorded
* into the cache or in the DB  : TSM_NO_TIMESTAMP for null
*
*   Two function the wrapper and the internal one
*/
tsm_time tsmGetLatest(uint64_t dpsId)
{
	FUNCTION_TRACE;

	tsm_ctx_t *ctx = tsmOpen(dpsId);
	if(ctx == NULL)
		return(TSM_NO_TIMESTAMP);
	else
		return(__tsmGetLatest(ctx));
}

tsm_time __tsmGetLatest(tsm_ctx_t *ctx)
{
	FUNCTION_TRACE;

	if(ctx->lastSampleTime  == 0) { // The cache isn't init ...
		if(ctx->table_fd[0] == NULL) {
			DEBUG("Data Base for DatapPoint %X is empty\n", ctx->meta->dpsId);
			return TSM_NO_TIMESTAMP;
		} else {
			if(ctx->meta->DBendTime != 0) {
#ifdef TSM_LONG_TIMESTAMP
				return ctx->meta->DBendTime;
#else
				return ctx->meta->DBendTime / 1000;
#endif
				} else {
				DEBUG("Data Base for Datapoint %X has 0 time\n", ctx->meta->dpsId);
				return TSM_NO_TIMESTAMP;
			}
		}
	}
	DEBUG("Latest values at point %X (%" TSM_PRI " s)\n", ctx->meta->dpsId, ctx->lastSampleTime);
	return ctx->lastSampleTime;
}

/*
 * Stores a new data point set of values
 * param: the dpId, the timestamp and the n-pla of metrics values
 *
 * returns 0 if OK otherwise on error
 *
 *   Two function the wrapper and the internal one
 *
*/
int tsmStoreDataPoint(uint64_t dpsId, tsm_time *timestamp, tsm_data_t *values)
{

	FUNCTION_TRACE;

	tsm_ctx_t *ctx = tsmOpen(dpsId);
	if(ctx == NULL) return(-1);
	return(__tsmStoreDataPoint(ctx, timestamp, values));
}

int __tsmStoreDataPoint(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values)
{
	FUNCTION_TRACE;

#ifdef TSM_CACHEPRESERVE
	if(__tsmIsCacheFull(ctx))
		ERROR("The sample cache is full ! Sample rejected \n");
		return(-1);
	}
#endif

	// lock the resource
	pthread_mutex_lock(&ctx->muIsDPoperated);
	gettimeofday(&ctx->lastUse, NULL); // mark the last used time ...

	__tsmCacheWrite(ctx, timestamp, values); // write values into the cache
	DEBUG("Push sample into the cache %d \n",*timestamp);
	ctx->lastSampleTime = *timestamp;
	// Update the downsampling decimation
	__tsmDownSampleValue(ctx, timestamp, values);
	// unlock the resource
	pthread_mutex_unlock(&ctx->muIsDPoperated);
#ifdef TSM_SUPERSAFEMODE
	// fix meta-data changes
	msync(ctx->meta, sizeof(tsm_metadata_t), MS_ASYNC);
#endif
	return (0);
}

/*
 * Make the DownSampling of vales and store into the DB
*/
void __tsmDownSampleValue(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values) {

	FUNCTION_TRACE;

	if(ctx->meta->decimation == 0) {  // There is no downSample mode set
		__tsmStoreSampleIntoDB(ctx, *timestamp, values);
		ctx->startDownSampleTime = ctx->lastDownSampleTime = *timestamp;
		return;
	}

	if(ctx->entries == 0) {  // This means that is the first after an open
		ctx->startDownSampleTime = ctx->lastSampleTime = *timestamp;
	} else {
		if(ctx->startDownSampleTime + ctx->meta->decimation <= *timestamp) { // is time to store the previous sample
			DEBUG("Datapoint set timing %d + %d > %d \n",ctx->startDownSampleTime , ctx->meta->decimation , *timestamp);
			__tsmStoreSampleIntoDB(ctx, (tsm_time)( (int64_t)((int64_t)ctx->startDownSampleTime+(int64_t)ctx->lastSampleTime)/2), ctx->accumulatorBuffer);
			__tsmResetDownSampleBuffer(ctx, timestamp);
		}
	}
	// Downsample the actual value
	tsm_time interval = ctx->lastSampleTime - *timestamp;  // the sample interval
	int metric;	// just an index
	int nummetrics = ctx->meta->nmetrics;  // the number of the metrics
	int n = ctx->entries;	// the number of the entries sampled
	tsm_data_t *ptrVal = values;  // copy of values buffer pointer
	for (metric = 0; metric < nummetrics; metric++, ptrVal++) {   //for each metric
		if (isnan(*ptrVal)) { continue; } // Skip unknown values
		// Perform decimation according to the option for this metric
		switch ((tsm_downsample_mode_t)((ctx->meta->flags[metric] >> TSM_DOWNSAMPLE_SHIFT) & TSM_DOWNSAMPLE_MASK)) {
			case tsmDownsample_Weight:
				ctx->accumulatorBuffer[metric] = (( ctx->accumulatorBuffer[metric] * n) + (ctx->sampleBuffer[metric] * (tsm_data_t)interval) ) / (tsm_data_t)(n+interval);
				ctx->sampleBuffer[metric] = *ptrVal;
				ctx->entries = n + interval;
				break;
			case tsmDownsample_Mean:
				ctx->accumulatorBuffer[metric] = (( ctx->accumulatorBuffer[metric] * n) + *ptrVal) / (tsm_data_t)(n+1);
				ctx->entries = n + 1;
				break;
			case tsmDownsample_Sum:
				ctx->accumulatorBuffer[metric] += *ptrVal;
				ctx->entries = n + 1;
				break;
/* FIXME: could be never, we need all samples, high resources consuming processes */
			case tsmDownsample_Median:
				ERROR("FIXME: MEDIAN not implemented\n");
				ctx->entries = n + 1;
				break;
/* FIXME: could be never, ... */
				case tsmDownsample_Mode:
				ERROR("FIXME: MODE not implemented\n");
				ctx->entries = n + 1;
				break;
			case tsmDownsample_Min:
				if (*ptrVal < ctx->accumulatorBuffer[metric]) ctx->accumulatorBuffer[metric] = *ptrVal;
				ctx->entries = n + 1;
				break;
			case tsmDownsample_Max:
				if (*ptrVal > ctx->accumulatorBuffer[metric]) ctx->accumulatorBuffer[metric] = *ptrVal;
				ctx->entries = n + 1;
				break;
			case tmsDownsample_None:
				ctx->accumulatorBuffer[metric] = *ptrVal;
				ctx->entries = 1;
				break;
			default:
				ERROR("Bad downsampling mode\n");
				ctx->entries = n + 1;
				break;
		}
	}
	ctx->lastDownSampleTime = *timestamp;
	return;
}

/*
 * resets the DownSamling buffer and variables...
 */
void __tsmResetDownSampleBuffer(tsm_ctx_t *ctx, tsm_time *timestamp) {

	FUNCTION_TRACE;

	ctx->entries = 0;
	ctx->startDownSampleTime = *timestamp;
	ctx->lastDownSampleTime =  *timestamp;
	int n = ctx->meta->nmetrics;
	int metric;
	for(metric=0;metric<n;metric++) {
		ctx->sampleBuffer[metric] = 0.0;
		switch ((tsm_downsample_mode_t)((ctx->meta->flags[metric] >> TSM_DOWNSAMPLE_SHIFT) & TSM_DOWNSAMPLE_MASK)) {
			case tsmDownsample_Min:
				ctx->accumulatorBuffer[metric] = INFINITY;
				break;
			case tsmDownsample_Max:
				ctx->accumulatorBuffer[metric] = -INFINITY;
				break;
			default:
				ctx->accumulatorBuffer[metric] = 0.0;
				break;
		}
	}
	return;
}

/* ============================================================
* Functions to manage the circular cache buffer
============================================================= */

/*
 * Calculate the pointer in the cache buffer
 */
#define __tsmCachePtr(ele) (ctx->cacheBufferPtr + (ele * ctx->sampleDim))

/* Test of the circular buffer */
int __tsmIsCacheFull(tsm_ctx_t *ctx) {

	FUNCTION_TRACE;

    return ((ctx->lastEle + 1) % ctx->cacheEntries == ctx->firstEle);
}
int __tsmIsCacheEmpty(tsm_ctx_t *ctx) {

	FUNCTION_TRACE;

    return ctx->lastEle == ctx->firstEle;
}
/* Write into the cache */
void __tsmCacheWrite(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values) {

	FUNCTION_TRACE;

	void *lastPtr;
	tsm_data_t *valsPtr = values;
	int n = ctx->meta->nmetrics * sizeof(tsm_data_t); // this is the dimension of the values serie
	lastPtr =  __tsmCachePtr(ctx->lastEle);
	// put into the cache
	*((tsm_time *)lastPtr) = *timestamp;
	lastPtr += sizeof(tsm_time);
	memcpy((char *)lastPtr,(char *)valsPtr,n);
	// Update pointers
	ctx->lastEle  = (ctx->lastEle + 1) % ctx->cacheEntries;
	if(ctx->lastEle == ctx->firstEle)
        ctx->firstEle = (ctx->firstEle + 1) % ctx->cacheEntries; // full, overwrite
	DEBUG("CacheWrite :first=%d last=%d timestamp=%d value=%f ... \n",ctx->firstEle,ctx->lastEle,(*timestamp)/TSM_TIMESTAMP_PRECISION , values[0]);
	return;
}
/* read the cache */
void __tsmCacheRead(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values) {

	FUNCTION_TRACE;

	void *firstPtr;
	tsm_data_t *valsPtr = values;
	int n = ctx->meta->nmetrics * sizeof(tsm_data_t); // this is the dimension of the values serie
	firstPtr = __tsmCachePtr(ctx->firstEle);
	// get form cache
	*timestamp = *((tsm_time *)firstPtr);
	memcpy((char *)valsPtr,(char *)firstPtr,n);
	// update pointers
	ctx->firstEle = (ctx->firstEle + 1) % ctx->cacheEntries;
	return;
}
/*
 * Search a value into the cache given the timestamp
 * Returns:
 *    -the index to the buffer element GREITER or EQUAL to the searched or -1 in
 *        the case that the searched if major to the last inserted element
 *    - the pointer to the record in the buffer or NULL
 */
/* FIXME: this could be optimized with a bynary search... but in memory... */
uint32_t __tsmSearchValueInCache(tsm_ctx_t *ctx, tsm_time timestamp, tsm_series_point_t **values) {

	FUNCTION_TRACE;

	void *ptr;
	int i = ctx->firstEle;

	while(i != ctx->lastEle) {
		ptr = __tsmCachePtr(i);
		if(*((tsm_time *)ptr) >= timestamp) {
			*values = (tsm_series_point_t *)ptr;
			return(i);
		}
		i = (i + 1) % ctx->cacheEntries;
	}
	*values = NULL;
	return(-1);
}
/*
 *  Skip to the next element into the circular buffer
 *  Returns:
 *     - the index of the element, or -1 in case of end of buffer reached
 *     - the pointer to the record in the buffer or NULL
 */
uint32_t __tsmNextValueInCache(tsm_ctx_t *ctx, uint32_t index, tsm_series_point_t **ptrS) {

	FUNCTION_TRACE;

	index = (index + 1) % ctx->cacheEntries;
	if(index == ctx->lastEle) {
		*ptrS = NULL;
		return(-1);
	}
	*ptrS = __tsmCachePtr(index);
	return(index);
}
/*
 *  Skip to the previous element into the circular buffer
 *  Returns:
 *     - the index of the element, or -1 in case of start of buffer reached
 *     - the pointer to the record in the buffer or NULL
 */
uint32_t __tsmPreviousValueInCache(tsm_ctx_t *ctx, uint32_t index, tsm_series_point_t **ptrS) {

	FUNCTION_TRACE;

	if(index == ctx->firstEle) {
		*ptrS = NULL;
		return(-1);
	}
	index = (index -1) % ctx->cacheEntries;
	*ptrS = __tsmCachePtr(index);
	return(index);
}

/* ===================================================================
    Functions to manage the DB
 ==================================================================== */

/*
 *  Converts the logical pointer to the fis. circular buffer
*/
#define __logToFisCirPtr(logPtr) (logPtr + ctx->meta->DBfirstRecord % ctx->meta->DBmaxNumOfRecords)

/*
 * Stores one sample into the DB
 * Params : the timestamp, the n-pla of all metrics values
 *
 * returns 0 if OK, otherwise on error
 */
//
//
int __tsmStoreSampleIntoDB(tsm_ctx_t *ctx, tsm_time timestamp, tsm_data_t *value) {

	FUNCTION_TRACE;

	// Prepare the buffer and the copy the formatted data
	off_t DBPtr = -1;
	int dim = ctx->meta->DBrecordDim;

	memcpy(ctx->recordIoBuffer,&timestamp,sizeof(tsm_time));
	memcpy(ctx->recordIoBuffer+sizeof(tsm_time),value,sizeof(tsm_data_t) * ctx->meta->nmetrics);
	ctx->recordIoBuffer[dim-1] = TSM_END_OF_RECORD; // this is a flag byte reserved

	// Controls the Store Mode : NoLimit, Circular, Switched
	int wrby;
	switch(ctx->meta->DBmode) {
		case tsmDBFile_Nolimit:  // append data ever
			// move the pointer at the end
			DBPtr = fseek(ctx->table_fd[0], 0, SEEK_END);
			if (DBPtr == -1) {
				ERROR("Error seeking the DB !\n");
				return -errno;
			}
			wrby = fwrite(ctx->recordIoBuffer, 1, dim, ctx->table_fd[0]);
			if(wrby != dim) {
				ERROR("Write a wrong number of bytes = %d !\n", wrby);
				return -errno;
			}
			ctx->meta->DBlastRecord = (ctx->meta->DBlastRecord + 1);
			DEBUG("Write bytes = %d (append mode)\n", wrby);
			break;

		case tsmDBFile_Circular:
			DBPtr = fseek(ctx->table_fd[0], ctx->meta->DBlastRecord * dim , SEEK_SET);
			if (DBPtr == -1) {
				ERROR("Error seeking the DB !\n");
				return -errno;
			}
			int wrby = fwrite(ctx->recordIoBuffer, dim, 1, ctx->table_fd[0]);
			if(wrby != dim) {
				ERROR("Write a wrong number of bytes = %d !\n", wrby);
				return -errno;
			}
			ctx->meta->DBlastRecord = (ctx->meta->DBlastRecord + 1) % ctx->meta->DBmaxNumOfRecords;
			if(ctx->meta->DBlastRecord == ctx->meta->DBfirstRecord)
				ctx->meta->DBfirstRecord = (ctx->meta->DBfirstRecord + 1) % ctx->meta->DBmaxNumOfRecords; // full, overwrite
			DEBUG("Write bytes = %d (circular buf mode)\n", wrby);
			break;
/* FIXME : we need to implement a file switching mechanism... work around the overlapping of data
 *         among two files in order to insure the data availability ...
 */
		case tsmDBFile_Switched:
			if( DBPtr >= ctx->meta->DBmaxFileLength) {
				// We need to switch the file ....
				ERROR("File switching mode not yet implemented\n");
			}
			break;
	}

	// flush the buffer
	fflush(ctx->table_fd[0]);

	// update metadata
	ctx->meta->DBnSamples++;
#ifdef TSM_LONG_TIMESTAMP
	if(ctx->meta->DBstartTime == 0) { // is the first sample
		ctx->meta->DBstartTime = (int64_t)(timestamp);
	}
	ctx->meta->DBendTime = (int64_t)timestamp;
#else
	if(ctx->meta->DBstartTime == 0) { // is the first sample
		ctx->meta->DBstartTime = ((int64_t)timestamp * 1000);
	}
	ctx->meta->DBendTime = ((int64_t)timestamp * 1000);
#endif

#ifndef TSM_SUPERSAFEMODE
	msync(ctx->meta, sizeof(tsm_metadata_t), MS_ASYNC);
#endif

	return(0);
}

/*
 * Search into the DB the record
 *
 * Returns the position index into the file or -1 if no found
 *
 */
int64_t __tsmSearchValueInDB(tsm_ctx_t *ctx, tsm_time timestamp, void *buffer, int bound) {

	FUNCTION_TRACE;

	// Bound 1=left 2=right 0=match
	if(bound < 0 || bound > 2) bound = 1;

	// Prepare the buffer and the copy the formatted data
	int RecordDim = ctx->meta->DBrecordDim;
	int64_t Entries = ctx->meta->DBnSamples;
	if(Entries < 1) return(-1); 	// DB empty .... exit

	// vars for the binary search
	int64_t dow = 0;
	int64_t up = Entries-1;
	int64_t m, pm;
	int reby;

	// the search depends to the DB organization
	switch(ctx->meta->DBmode) {
		case tsmDBFile_Nolimit:  // just a stream: binary search
			while(dow <= up) {
				m = (dow + up) / 2;
				fseek(ctx->table_fd[0], m * RecordDim, SEEK_SET);
				reby = fread(buffer,1,RecordDim,ctx->table_fd[0]);
				if(reby != RecordDim) {
					ERROR("Error reading DB! \n");
					return(-1);
				}
				if(*((tsm_time *)buffer) == timestamp) {
					return(m);
				}
				if(*((tsm_time *)buffer) < timestamp)
					dow = m + 1;
				else
					up = m - 1;
			}
			if(bound==0) return(-1);
			if(bound==1 && dow < Entries-1 && dow > 0) dow--;
			// sync the buffer
			fseek(ctx->table_fd[0], dow * RecordDim, SEEK_SET);
			reby = fread(buffer,1,RecordDim,ctx->table_fd[0]);
			if(reby != RecordDim) {
				ERROR("Error reading DB! \n");
				return(-1);
			}
			return(dow);
			break;

/* FIXME: could be optimized ... */
		case tsmDBFile_Circular:
			while(dow <= up) {
				m = (dow + up) / 2;
				pm = __logToFisCirPtr(m);
				fseek(ctx->table_fd[0], pm * RecordDim, SEEK_SET);
				reby = fread(buffer,1, RecordDim,ctx->table_fd[0]);
				if(reby != RecordDim) {
					ERROR("Error reading DB! \n");
					return(-1);
				}
				if(*((tsm_time *)buffer) == timestamp) {
					return(m);
				}
				if(*((tsm_time *)buffer) < timestamp)
					dow = m + 1;
				else
					up = m - 1;
			}
			if(bound==0) return(-1);
			if(bound==1 && dow < Entries-1 && dow > 0) dow--;
			// sync the buffer
			fseek(ctx->table_fd[0], dow * RecordDim, SEEK_SET);
			reby = fread(buffer,1,RecordDim,ctx->table_fd[0]);
			if(reby != RecordDim) {
				ERROR("Error reading DB! \n");
				return(-1);
			}
			return(dow);
			break;

/* FIXME: not yet implemented, just a placeholder */
		case tsmDBFile_Switched:
			ERROR("File switching mode not yet implemented\n");
				//if( DBPtr >= ctx->meta->DBmaxFileLength) {
				// We need to switch the file ....
			return(-1);
			break;
	}
	return(-1);
}
/*
 *  Two wrappers in order to move Next/Previous with just a function
 */
int64_t __tsmNextValueInDB(tsm_ctx_t *ctx, int64_t filePtr, void *buffer){

	FUNCTION_TRACE;

	return(__tsmSkipValueInDB(ctx, filePtr, buffer, +1));
}
int64_t __tsmPreviuosValueInDB(tsm_ctx_t *ctx, int64_t filePtr, void *buffer){

	FUNCTION_TRACE;

	return(__tsmSkipValueInDB(ctx, filePtr, buffer, -1));
}
/*
 *  Skip to the next/previous element into the file
 *  Params : the actual Index, the direction
 *  Returns:
 *     - the Index to the record in the file or -1 on error
 *     - the Input buffer filled with the record data
 */
int64_t __tsmSkipValueInDB(tsm_ctx_t *ctx, int64_t filePtr, void *buffer, int dir){

	FUNCTION_TRACE;

	int64_t newFilePtr;
	int reby;

	if((filePtr + dir) < 0) return(-1);
	// the search depends to the DB organization
	switch(ctx->meta->DBmode) {
		case tsmDBFile_Nolimit:  // just a stream: binary search

			if(fseek(ctx->table_fd[0], (filePtr + dir) * ctx->meta->DBrecordDim, SEEK_SET) != 0) {
				ERROR("Error access the DBn\n");
				return(-1);
			}
			if((ftell(ctx->table_fd[0])) != (filePtr + dir) * ctx->meta->DBrecordDim) {
				DEBUG("End of File reached during Skip record operation\n");
				return(-1);
			}
			reby = fread(buffer, 1, ctx->meta->DBrecordDim, ctx->table_fd[0]);
			if(reby != ctx->meta->DBrecordDim) {
				DEBUG("Error reading DB! \n");
				return(-1);
			}
			return(filePtr+dir);
			break;

		case tsmDBFile_Circular:  // sequential search :(

			newFilePtr = (filePtr + dir) % ctx->meta->DBmaxNumOfRecords;
			if(newFilePtr == (dir == 1 ? ctx->meta->DBfirstRecord : ctx->meta->DBlastRecord) ) {
				DEBUG("End of buffer (circular buf mode)\n");
				return(-1);
			}
			if(fseek(ctx->table_fd[0], newFilePtr * ctx->meta->DBrecordDim, SEEK_SET) != 0) {
				DEBUG("Error access the DBn\n");
				return(-1);
			}
			if((ftell(ctx->table_fd[0])) != (newFilePtr * ctx->meta->DBrecordDim)) {
				DEBUG("End of File reached during Next record operation\n");
				return(-1);
			}
			reby = fread(buffer, 1, ctx->meta->DBrecordDim, ctx->table_fd[0]);
			if(reby != ctx->meta->DBrecordDim) {
				DEBUG("Error reading DB! \n");
				return(-1);
			}
			return(newFilePtr);

/* FIXME: not yet implemented, just a placeholder */
		case tsmDBFile_Switched:
			ERROR("File switching mode not yet implemented\n");
				//if( DBPtr >= ctx->meta->DBmaxFileLength) {
				// We need to switch the file ....
			return(-1);
			break;
	}
	return(-1);
}


/*
 * Get values for all metrics of a DP
 * Params : DP identifier, the timestamp, the pointer to the begin of the outBuffer and the max size allocated
 *
 * Returns : the pointer to the filled buffer
 *
 * NOTE: This is the JSON wrapper to the Original function
 *
 */
char *tsmGetDPValue(uint64_t dpsId, tsm_time *timestamp, char **outBuffer, long maxsize) {

	FUNCTION_TRACE;

	tsm_ctx_t *ctx = tsmOpen(dpsId);
	if(ctx == NULL)
		return(NULL);

	tsm_data_t *values = my_malloc(__FUNCTION__, sizeof(tsm_time) + ctx->meta->nmetrics * sizeof(tsm_data_t));
	if(values == NULL)
		return(NULL);

	char *buf_ptr = *outBuffer + strlen(*outBuffer);
	char sep[4];
	strcpy(sep,"");

	if(__tsmGetDPValue(ctx, timestamp, values) == 0) {
/* FIXME : with or without the  TAB & NEWLINE char ?? */
		int i;
		char *values_ptr = (char *)values;

		sprintf(buf_ptr, "\t\t\t\"dpId\":\t%llu,\n", dpsId);
		buf_ptr = *outBuffer + strlen(*outBuffer);

#ifdef TSM_LONG_TIMESTAMP
		sprintf(buf_ptr, "\t\t\t\"timestamp\":\t%10.3f,\n", ((double)(*timestamp))/TSM_TIMESTAMP_PRECISION);
#else
		sprintf(buf_ptr, "\t\t\t\"timestamp\":\t%" TSM_TIMESTAMP_PRI ",\n", (*timestamp));
#endif
		buf_ptr = *outBuffer + strlen(*outBuffer);

		strcat(buf_ptr, "\t\t\t\"values\":\t[");
		buf_ptr = *outBuffer + strlen(*outBuffer);

		for(i=0;i<ctx->meta->nmetrics;i++) {
			sprintf(buf_ptr, "%s %f", sep, *((tsm_data_t *)values_ptr) );
			buf_ptr = *outBuffer + strlen(*outBuffer);
			strcpy(sep,",");
			values_ptr += sizeof(tsm_data_t);
		}
		strcat(buf_ptr, "]\n");
	}
	my_free(__FUNCTION__,values);
	return(*outBuffer);
}

int __tsmGetDPValue(tsm_ctx_t *ctx, tsm_time *timestamp, tsm_data_t *values) {

	FUNCTION_TRACE;

	int result = 0;
	tsm_series_point_t *ptrValue;
	uint32_t	index;

	if(*timestamp > ctx->lastSampleTime) {
		ERROR("Asked a value in the future it has wrong timestamp ! \n");
		return -ENOENT;
	}

	// lock
	pthread_mutex_lock(&ctx->muIsDPoperated);
	gettimeofday(&ctx->lastUse, NULL);

	// verify the required time stamp
	if (*timestamp == TSM_NO_TIMESTAMP) {
		*timestamp  = ctx->lastSampleTime;
	}
	if(*timestamp <= ctx->lastSampleTime && ctx->entries != 0 && *timestamp >= *((tsm_time *)__tsmCachePtr(ctx->firstEle)) ) { //the sample is in cache
		index = __tsmSearchValueInCache(ctx, *timestamp, &ptrValue);
		if(ptrValue != NULL) {
			*timestamp = *(tsm_time *)ptrValue;
			ptrValue = ptrValue + sizeof(tsm_time);
			memcpy(values, (tsm_data_t *)ptrValue, ctx->meta->nmetrics * sizeof(tsm_data_t));
			result = 0;
		} else {
			ERROR("DB corrupted for dpId : %X ! \n",ctx->meta->dpsId);
			result = -ENOENT;
		}
	} else { // the sample could be into the DB
#ifdef TSM_LONG_TIMESTAMP
		if(*timestamp < ctx->meta->DBstartTime || *timestamp > ctx->meta->DBendTime) {
#else
		if(*timestamp * 1000 < ctx->meta->DBstartTime || *timestamp * 1000 > ctx->meta->DBendTime) {
#endif
			DEBUG("Requested value has timestamp outside DB ! \n");
			result = -ENOENT;
		} else if(__tsmSearchValueInDB(ctx, *timestamp, ctx->recordIoBuffer, 1) == -1) {
			DEBUG("Requested value not found into DB ! \n");
			result = -ENOENT;
		} else {
			// copy results
			*timestamp = *((tsm_time *)ctx->recordIoBuffer);
			memcpy( values, (tsm_data_t *)(ctx->recordIoBuffer + sizeof(tsm_time)), (sizeof(tsm_data_t) * ctx->meta->nmetrics));
			result = 0;
		}
	}
	// unlock
	pthread_mutex_unlock(&ctx->muIsDPoperated);
	return (result);
}

/*
 * Get a series of values for one metric give the time interval
 * Params : DP identifier, the metric id, the start/end interval timestamp, the maximum number
 *          of entries requested, the pointer to the begin of the outBuffer and the max size allocated
 *
 * Returns : the pointer to the filled buffer
 *
 * NOTE: This is the JSON wrapper to the Original function
 *
 */
char *tsmGetOneSerie(uint64_t dpsId, unsigned int metricId, tsm_time startTime, tsm_time endTime,
		unsigned int maxEntries, char **outBuffer, long maxsize) {

	FUNCTION_TRACE;

	tsm_ctx_t *ctx = tsmOpen(dpsId);
	if(ctx == NULL)
		return(NULL);

	char *buf_ptr = *outBuffer + strlen(*outBuffer);

	sprintf(buf_ptr, "\t\t\t\"dpId\":\t%llu,\n", dpsId);
	buf_ptr = *outBuffer + strlen(*outBuffer);

	sprintf(buf_ptr, "\t\t\t\"metric\":\t%d,\n", metricId);
	buf_ptr = *outBuffer + strlen(*outBuffer);

	if(__tsmGetOneSerie(ctx,metricId,startTime,endTime,maxEntries,outBuffer, maxsize) == 0) {
		// boh
	}
	return(*outBuffer);
}

int __tsmGetOneSerie(tsm_ctx_t *ctx, unsigned int metricId, tsm_time startTime, tsm_time endTime,
					unsigned int maxEntries, char **outBuffer,long maxsize)
{
	FUNCTION_TRACE;

	char *buf_ptr = *outBuffer + strlen(*outBuffer);

	// Verify the metric
	if (metricId >= ctx->meta->nmetrics) {
		strcat(buf_ptr, "[]\n");
		ERROR("Requested metric is out of range\n");
		return -ENOENT;
	}
	// and the time
	if (endTime < startTime) {
		strcat(buf_ptr, "[]\n");
		ERROR("End time must be later than start\n");
		return -EINVAL;
	}

	// lock the resource
	pthread_mutex_lock(&ctx->muIsDPoperated);
	gettimeofday(&ctx->lastUse, NULL);


	// Correct and verify the time
#ifdef TSM_LONG_TIMESTAMP
	if (startTime == TSM_NO_TIMESTAMP || startTime < 0) startTime = ctx->meta->DBstartTime;
	if (endTime == TSM_NO_TIMESTAMP || endTime < 0) endTime = ctx->lastSampleTime;
#else
	if (startTime == TSM_NO_TIMESTAMP || startTime < 0) startTime = ctx->meta->DBstartTime / 1000;
	if (endTime == TSM_NO_TIMESTAMP || endTime < 0) endTime = ctx->lastSampleTime;
#endif

	int numberOfEntriesRead;
	tsm_time firstOfCache = *((tsm_time *)__tsmCachePtr(ctx->firstEle));

	strcat(buf_ptr, "\t\t\t\"values\":\t[");

	__dump_time(0, "Start time for the query is"); // Produce INFOs ...
	if(startTime >= firstOfCache && ctx->entries > 0 ) { // the series is in the cache
		numberOfEntriesRead = __extractSerieFromCache(ctx,metricId,startTime,endTime,maxEntries, outBuffer, &maxsize);
	} else if(endTime < firstOfCache || ctx->entries == 0) { //the sample is in DB)
		numberOfEntriesRead = __extractSerieFromDB(ctx,metricId,startTime,endTime,maxEntries, outBuffer, &maxsize);
	} else {
		numberOfEntriesRead = __extractSerieFromDB(ctx,metricId,startTime, firstOfCache, maxEntries, outBuffer, &maxsize);
		maxEntries = maxEntries - numberOfEntriesRead;
		numberOfEntriesRead += __extractSerieFromCache(ctx,metricId,firstOfCache,endTime,maxEntries, outBuffer, &maxsize);
	}
	__dump_time(numberOfEntriesRead, "End query for elements=");

	//
	buf_ptr = *outBuffer + strlen(*outBuffer);
	if( *(buf_ptr -1) == ',' ) *(buf_ptr-1) = '\n';

	strcat(*outBuffer, "\t\t\t]\n");

	// unlock
	pthread_mutex_unlock(&ctx->muIsDPoperated);

	return numberOfEntriesRead;
}

// ----------------------------------------------
//
// extracts one metric for an interval from the cache
//
long __extractSerieFromCache(tsm_ctx_t *ctx,
		unsigned int metricId,
		tsm_time startTime, tsm_time endTime,
		unsigned int maxEntries,
		char **outBuffer,long *maxsize)
{
	FUNCTION_TRACE;

	if(maxEntries <= 0) return(0);
	char *ptrS;
	char *ptrD = *outBuffer + strlen(*outBuffer);

	tsm_series_point_t *ptrCache;
	long numVal = 0;
	uint32_t index;

	index = __tsmSearchValueInCache(ctx, startTime, &ptrCache);
	while(ptrCache != NULL) {
		ptrS = (char *)ptrCache;	
#ifdef TSM_LONG_TIMESTAMP
		sprintf(ptrD, "\n\t[%14.3f, %f],", ((double)*((tsm_time *)ptrS))/TSM_TIMESTAMP_PRECISION, *((tsm_data_t *)(ptrS + sizeof(tsm_time) + (sizeof(tsm_data_t) * metricId))) );
#else
		sprintf(ptrD, "\n\t[%" TSM_TIMESTAMP_PRI ", %f],", *((tsm_time *)ptrS), *((tsm_data_t *)(ptrS + sizeof(tsm_time) + (sizeof(tsm_data_t) * metricId))) );
#endif
		ptrD = *outBuffer + strlen(*outBuffer);

		if(strlen(*outBuffer) > *maxsize-200) { // the buffer is full
			*maxsize = *maxsize + 16384;
			*outBuffer = realloc(*outBuffer , *maxsize); //ad 16k bytes
		}
		// is the end
		numVal++;
		if(numVal < maxEntries) { // we have more entries ... search the next
			index = __tsmNextValueInCache(ctx, index, &ptrCache);  // Update source pointer
			if(ptrCache != NULL && endTime < (*(tsm_time *)ptrCache) ) ptrCache = NULL; // force the exit
		}
	}
	DEBUG("Find %d values in the time interval %u %u ! \n",numVal,startTime, endTime);
	return(numVal);
}

// ----------------------------------------------
//
// extracts one metric for an interval from the DB
//
long __extractSerieFromDB(tsm_ctx_t *ctx,
		unsigned int metricId,
		tsm_time startTime, tsm_time endTime,
		unsigned int maxEntries,
		char **outBuffer,long *maxsize)
{
	FUNCTION_TRACE;

	if(maxEntries <= 0) return(0);

	char *ptrD = *outBuffer + strlen(*outBuffer);
	long numVal = 0;
	int64_t filePtr;

	filePtr = __tsmSearchValueInDB(ctx, startTime, ctx->recordIoBuffer, 1 );
	while(filePtr >= 0) {
#ifdef TSM_LONG_TIMESTAMP
		sprintf(ptrD, "\n\t[%14.3f, %f],", ((double)*((tsm_time *)ctx->recordIoBuffer))/TSM_TIMESTAMP_PRECISION, *((tsm_data_t *)(ctx->recordIoBuffer+sizeof(tsm_time) + (sizeof(tsm_data_t) * metricId))) );
#else
		sprintf(ptrD, "\n\t[%" TSM_TIMESTAMP_PRI ", %f],", *((tsm_time *)ctx->recordIoBuffer), *((tsm_data_t *)(ctx->recordIoBuffer+sizeof(tsm_time) + (sizeof(tsm_data_t) * metricId))) );
#endif
		numVal++;
		ptrD = *outBuffer + strlen(*outBuffer);

		if(strlen(*outBuffer) > *maxsize-200) { // the buffer is full
			*maxsize = *maxsize + 16384;
			*outBuffer = realloc(*outBuffer , *maxsize); //ad 16k bytes
		}

		filePtr = __tsmNextValueInDB(ctx, filePtr, ctx->recordIoBuffer);  // Update source pointer
		// exit conditions
		if(filePtr != -1 && (numVal == maxEntries || endTime <= (*(tsm_time *)ctx->recordIoBuffer)) ) {
#ifdef TSM_LONG_TIMESTAMP
			sprintf(ptrD, "\n\t[%14.3f, %f],", ((double)*((tsm_time *)ctx->recordIoBuffer))/TSM_TIMESTAMP_PRECISION, *((tsm_data_t *)(ctx->recordIoBuffer+sizeof(tsm_time) + (sizeof(tsm_data_t) * metricId))) );
#else
			sprintf(ptrD, "\n\t[%" TSM_TIMESTAMP_PRI ", %f],", *((tsm_time *)ctx->recordIoBuffer), *((tsm_data_t *)(ctx->recordIoBuffer+sizeof(tsm_time) + (sizeof(tsm_data_t) * metricId))) );
#endif
			numVal++;
			filePtr = -1;
		}
	}
	DEBUG("Find %d values in the time interval %u %u ! \n",numVal,startTime, endTime);
	return(numVal);
}


/* FIXME : We need to consider a good security/encription system ... need investigation */
// ------------------------------------------------------------------------
// Functions for the key management
//  -----------------------------------------------------------------------
int tsmGetKey(tsm_ctx_t *ctx, tsm_key_id_t key_id, tsm_key_t *key) {

	FUNCTION_TRACE;

	if (key_id >= tsmKey_Max) {
		ERROR("Bad key ID\n");
		return -EINVAL;
	}
	DEBUG("Request for dpId %016" PRIX64 " key %d\n", ctx->meta->dpsId, (int)key_id);
	if (ctx->meta->key[(int)key_id].flags == 0) {
		INFO("tsmGetKey :: No key defined\n");
		return -ENOENT;
	}
	// Return stored key
	memcpy(key, &ctx->meta->key[(int)key_id].key, sizeof(tsm_key_t));
	return 0;
}

int tsmSetKey(tsm_ctx_t *ctx, tsm_key_id_t key_id, tsm_key_t *key) {

	FUNCTION_TRACE;

	if (key_id >= tsmKey_Max) {
		ERROR("Bad key ID\n");
		return -EINVAL;
	}
	DEBUG("Update to dpId %016" PRIX64 " key %d\n", ctx->meta->dpsId, (int)key_id);
	if (key) {	// Store new key
		ctx->meta->key[(int)key_id].flags = TSM_KEY_IN_USE;
		memcpy(&ctx->meta->key[(int)key_id].key, key, sizeof(tsm_key_t));
	} else { // Erase stored key
		memset(&ctx->meta->key[(int)key_id], 0, sizeof(tsm_key_info_t));
	}
	// Flush metadata
	msync(ctx->meta, sizeof(tsm_metadata_t), MS_ASYNC);
	return 0;
}

/* =========================================================
 *  Functions for the DB management
 ========================================================== */

/*
 *   Return the number and the list of the recorded DPs
 *   the parameter is an JSON object
 *   { "list_of_Dps" : [ dpId,..] }
 */
int tsmGetDPList(char **dpListResult, long maxsize) {

	FUNCTION_TRACE;

	int n=0; // number of items
	DIR *dp;
	struct dirent *ep;
	char sep[4];
	unsigned long long dpId = 0;
	char *buf_ptr = *dpListResult + strlen(*dpListResult);

	strcat(buf_ptr, " [ ");
	buf_ptr = *dpListResult + strlen(*dpListResult);
	strcpy(sep,"");

	if ((dp = opendir ("./")) != NULL) { // if the current directory is good
		while ((ep = readdir (dp))) {  // scan the directory for *.tsm files
			if(strstr(ep->d_name,".tsm") != NULL) {
				n++;
				sscanf(ep->d_name, TSM_METADATA_FORMAT, &dpId); // extract the dpId
				sprintf(buf_ptr, "%s %llu", sep, dpId);
				buf_ptr = *dpListResult + strlen(*dpListResult);
				strcpy(sep,",");
				if( strlen(*dpListResult) > maxsize -50) { // buffer full
					maxsize += 2048;
					*dpListResult = realloc(*dpListResult, maxsize );
				}
			}
		}
		(void) closedir (dp);
    } else {
    	ERROR("Couldn't open the directory");
    	n = -1;
	}
	strcat(*dpListResult, " ] ");
	return n;
}

/*
 *   Returns a array of objects key:value with the info about a
 *   DP
 */
char *tsmDumpMetaData(uint64_t dpsId, char **outBuffer, long maxsize) {

	FUNCTION_TRACE;

	tsm_ctx_t *ctx = tsmOpen(dpsId);
	if(ctx == NULL)	{
		return(NULL);
	}

	char *buf_ptr = *outBuffer + strlen(*outBuffer);

	strcat(buf_ptr, "\t{\n");
	buf_ptr = *outBuffer + strlen(*outBuffer);

	sprintf(buf_ptr, "\t\t\t\"STS_DB_Version\":\t%d,\n", ctx->meta->version);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Magic_number\":\t%d,\n", ctx->meta->magic);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Data_Point_Id\":\t%llu,\n", ctx->meta->dpsId);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Number_of_Metrics\":\t%d,\n", ctx->meta->nmetrics);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Minimum_aquisition_interval\":\t%d,\n", ctx->meta->minInterval);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Interval_Dimension_of_Cache\":\t%d,\n", ctx->meta->cacheDim);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Down_Sampling_Interval\":\t%d,\n", ctx->meta->decimation / TSM_TIMESTAMP_PRECISION);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Storing_DB_Mode\":\t%d,\n", ctx->meta->DBmode);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Maximum_File_Dimension\":\t%llu,\n", ctx->meta->DBmaxFileLength);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Maximum_Number_of_Records\":\t%llu,\n", ctx->meta->DBmaxNumOfRecords);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Record_Dimension\":\t%d,\n", ctx->meta->DBrecordDim);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"Number_of_Recorded_Records\":\t%d,\n", ctx->meta->DBnSamples);
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"TimeStamp_of_First_Record\":\t\"%s\",\n", tsmEpocToString( ctx->meta->DBstartTime));
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"TimeStamp_of_Last_Record\":\t\"%s\"\n", tsmEpocToString( ctx->meta->DBendTime));
	buf_ptr = *outBuffer + strlen(*outBuffer);
	sprintf(buf_ptr, "\t\t\t\"DB_Structure_Type\":\t\"%d\"\n", ctx->meta->DBstructType);
	buf_ptr = *outBuffer + strlen(*outBuffer);

	strcat(buf_ptr, "\t\t}\n");

	return(*outBuffer);

}

// ----------------------------------  EOF  ------------------------------------------
