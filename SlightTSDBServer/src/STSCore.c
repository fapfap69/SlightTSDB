/*
 * STSCore.c
 *
 *  Created on: 15/gen/2014
 *      Author: fap
 */

#include "SlightTSDB.h"
#include "STSCore.h"
#include "STSLog.h"


void *my_malloc(const char *caller, size_t  size) {

	void *poi;
	poi = malloc(size);
	if(poi != NULL) memset((char *)poi,0,size);

	char timestamp[TIMESTAMP_MAX];
	time_t now;
	now = time(NULL);
	strftime(timestamp, TIMESTAMP_MAX, TIMESTAMP_FORMAT, localtime(&now));
	DEBUG("%s: >  %s >-----> Malloc %d bytes at 0x%X\n", timestamp, caller, size, poi);
	return poi;
}
void my_free(const char *caller, void *pointer) {
	char timestamp[TIMESTAMP_MAX];
	time_t now;
	now = time(NULL);
	strftime(timestamp, TIMESTAMP_MAX, TIMESTAMP_FORMAT, localtime(&now));
	DEBUG("%s: < %s <----<< Free 0x%X\n", timestamp, caller, pointer);

	free(pointer);
	return;
}

/*!
 * \brief		Dump the clock time
 * \param param	A number
 * \param mes  a message
 */
void __dump_time(long param, char *mes) {
	struct tm *tmp;
	struct timeval now;
	gettimeofday(&now, NULL);
	char *timestr = my_malloc(__FUNCTION__,100);
	tmp = localtime(&now.tv_sec);
	strftime(timestr, 100, "%F %T", tmp);
	INFO("Time Series Server :: Bench mark : %s %d time = %s clock = %d.%03d \n", mes, param, timestr,now.tv_sec,now.tv_usec/1000);
	my_free(__FUNCTION__,timestr);
	return;
}


/*!
 * \brief		Returns the actual time according the Time format
 * \param 	void
 * \return 	the Linux Epoch in sec or millisec
 *
*/
tsm_time getNow(void) {
#ifdef TSM_LONG_TIMESTAMP
	struct timeval now;
	gettimeofday(&now, NULL);
	return((tsm_time)( now.tv_sec * 1000 + (now.tv_usec/1000.0) ) );
#else
	return((tsm_time)time(NULL));
#endif
}

/*!
 * \brief		Convert the Unix Epoch time into a string
 * \param timestamp Unix Epoch
 * \return the string DD/MM/YYYY HH:MM:SS  ( DD/MM/YYYY HH:MM:SS.mmm )
 *
 *   Returns a string that contains the Date Time
 *   two formats with or without millsec
 */
char *tsmEpocToString(int64_t timestamp) {
	struct tm * ts;
	static char timebuffer[30];
	time_t sec;
	double fsec;
	long millisec = (long)((modf(((double)timestamp)/1000.0, &fsec))*1000.0);
	sec = (time_t)fsec;
    ts = localtime(&sec);
    sprintf(timebuffer, "%02d/%02d/%04d %02d:%02d:%02d.%03d", ts->tm_mday, ts->tm_mon+1, ts->tm_year+1900, ts->tm_hour, ts->tm_min, ts->tm_sec, (int)millisec );
    return(timebuffer);
}

char *tsmEpocToStringShort(int32_t timestamp) {
	struct tm * ts;
	static char timebuffer[25];
	time_t t = (time_t)timestamp;
    ts = localtime(&t);
    strftime(timebuffer, sizeof(timebuffer), TS_DATETIMEFORMAT, ts);
    return(timebuffer);
}

