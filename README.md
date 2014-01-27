******************************************************************************
*                             SlightTSDB                                     *
*  Author : Antonio Franco                                                   *
*  ver : 0.1                                                                 *
*  date : 27/01/2014                                                         *
******************************************************************************

A slight Time Series Data Base

/*
  Copyleft 2014 Antonio FRANCO

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files, to deal in the Software
  without restriction, including without limitation the rights to use, copy, 
  modify, merge, publish, distribute, sublicense, and/or sell copies of the 
  Software, and to permit persons to whom the Software is furnished to do so,
  subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
*/

==   SlightTSDB software  ==

SlightTSDB aims to be a very light Time Series Data Base server, able to store series
of numeric data coming from more sources. The Client/Server protocol is based on 
TCP/IP - JSON.

Data Overview : Each data stream, named DATA_POINT (DP) can be composed by a certain
 number of float numeric values, named METRIC. The timed value for a Data Point is
 stored into the DB as an n-pla composed by a TIME_STAMP value and one float value
 for each Metric defined.
 
Data Recording : The Data Point values are recorded into a Cache (allocated in the 
 RAM memory) and after a DownSampling procedure, they are stored into the DB archive 
 (based on file). The cache dimension, the dawnsampling strategy and the DB 
 filedimension are parametric. The DATA_POINT is identified by an integer number (DPID).
 
 DB Operations : Here is a concise report of the actions will be done on the DB:	
 
 	- ServerStatus : retrives some information about the server, its setting and 
 	               status
 	- GetDPList : obtains a list of all DPs addressed by the server.
 	- GetDPInfo(DpId) : returns a table of informations about the specified Data
 	                    Point Id
 	- GetDPLast(DpId) : returns the last recorded value for the specified Data
 	                    Point Id. (All metrics will be returned)
 	                    
 	- GetDPVal(DpId,TimeStamp) : returns the recorded value for the specified Data
 	                    Point Id, that matchs the TimeStamp specified. (All metrics 
 	                    will be returned). If the values with the TimeStamp not
 	                    exists the previous one is returned.
 	                    
 	- GetDPSer(DPId, Metric, StartTime, EndTime) : returns a series of values of 
 						the specified Data Point Id Metric into the interval
 						] TimeStart, TimeEnd [.
 						
 	- SetDPVal(DPId, TimeStamp, [Val1,.. Valn]) : store the n-Values (all the metrics)
 						for the specified Data Point Id, with the TimeStamp specified.
 						
 	- SetDPVal(DPId, [Val1,.. Valn]) : store the n-Values (all the metrics) for the 
 						specified Data Point Id. The TimeStamp assigned is the value to
 						the Server local time at the received request time.
 	
 	- CreateDP(DPId, Metrics, CacheDim, Interval, DBMode, FileLenght, Decimation, DSModes) : create 
 						a new Data Point with the specified DPId, if it don't exists into
 						the DB.
 					DPId : <int> an integer number
 					Metrics : <int> number of metrics for this DP (max 32)
 					CacheDim : <int> dimension of the cache in seconds
 					Interval : <float> minimum period of samples update in seconds
					DBMode : <int> type of the DB management. 
								0 = infinite file dimension
								1 = finite dimension, circular buffer mode
								2 = finite dimension, switching file mode (No yet implemented)
					FileLenght : <long> maximum dinesion of DB file for the finite dinesion
								modes
					Decimation : <int> number of seconds of the time window used for the down
								sampling. 0 = no downsampling, all samples will be recorded
					DSModes : <array of int> a number thet specify the downsampling mode for 
								each metric.
								0 =  Mean. Calculate the Mean of values inside the time window
								1 = Median. Not implemented (this is time consuming ...)
								2 = Mode. Not implemented (this is time consuming ...)
								3 = Sum. Calculate the sum of values for the time window
								4 = Min. Set the Minimum value received for the time window
								5 = Max. Set the Maximum value received for the time window 
								6 = Weight. The Mean of values inside the time window,
								    weighted by the interval of duration of the sample 
								7 = None. Store the last value as rapresentative value

	- DeleteDP(DPId) : 	delete the specified DataPoint.
	
** BUILD INSTRUCTIONS **

	Change the working directory to the: SlightTSDBServer one, then run 'make' command

** RUN THE SERVER **

	In order to run the server from the shell:

	SlightTSDB/SlightTSDBServer$ ./SlightTSDB -h
		Slight Time Series DB version: 0.100000 
		A.Franco -2013- (L) 

		Usage: ./SlightTSDB [-s] [-v <level>] [-p <HTTP port>] [-D <data path>]

		        -s Don't daemonise - logs to stderr
		        -D Path to data tree 
		        -p HTTP listen port
		        -v Set verbosity level 0:none 1:critical, 2:error, 3:warning, 4:info, 5:debug, 6:trace
 

*** COMPILE SWITCH ***

	In order to tailor the Server is possible to change some compiler switch and costants in the file SlightTSDB.h:

	#define TSS_USEAUTHENTICATION	- Specify the use of autentication (ssh) Not Yet implemented ! 
	#define TSM_DOUBLE_TYPE		- Use DOUBLE format for store values : DOUBLE -> 8 bytes, FLOAT -> 4 bytes
	#define TSM_LONG_TIMESTAMP	- Use MILLISECONDS precision for the time stamp : MILLSEC -> 8 bytes, SEC -> 4 bytes
	#define TSM_PTHREAD_LOCKING	- Use Thread Locking strategy for some operations
	#define TSM_REMOVE_DATAFILES	- Remove the data files once delete the DataPoint
	#define TSM_JSON_OUTPUT		- Produce the output in JSON format
	#define TSM_AUTOCORRECT_DB_STRUCT	- Do the authomatic conversion of DB from LONG/SHORT formats
	#define TSM_CACHEPRESERVE	- Don't overwrite the circular buffer of the cache when full

	#define TSM_MAX_METRICS	32	// Maximum number of metrics per data set
	#define TSM_MAX_ACTIVEDP	225	// max number of active data points

	#define TSM_MAXUNUSEDTIME 60	// Time in seconds after that an unused DP stream is closed and the cache is purged

	#define TS_DATETIMEFORMAT "%d/%m/%Y %H:%M:%S"  - string format to display date/time values


**** Clients  ****

	Some Example clients are in the SlightDBClient directory

	...


****************************************************************************
 TO DO :
 	 * check the long data compilation mode & reconstruct
 	 * dump values with different time-printing format
 	 * use a dp in exclusive/public mode ... prevent multiple writes !
 	 * push publication mechanism !
 	 * adding "meta info" to DP and metrics

****************************************************************************

	CREDITS & TANKS 

 Forked included software :

 TimeStore (http://www.livesense.co.uk/timestore)
 Copyright (C) 2012, 2013 Mike Stirling

****************************************************************************

