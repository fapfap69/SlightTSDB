/*
 ============================================================================
 Name        : SlightTSDB.c
 Author      : A.Franco
 Version     : 0.1
 Copyright   :

 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.

 Description : Slight Time Series DB server


 TO DO :
 	 * check the long data compilation mode & reconstruct
 	 * dump values with different time-printing format
 	 * use a dp in exclusive/public mode ... prevent multiple writes !
 	 * push publication mechanism !
 	 * adding "meta info" to DP and metrics



 Forked included software :

 TimeStore (http://www.livesense.co.uk/timestore)
 Copyright (C) 2012, 2013 Mike Stirling


 ============================================================================
 */



// particular C includes
#include "profile.h"

// Project includes
#include "SlightTSDB.h"
#include "STSCore.h"
#include "STSAccess.h"
#include "STSjsonrpc.h"

// ------- declaration and definition in the Manager ----------
void __tsmGarbageContex();

// ----------  static variables  -----------------------------
static int bExitRun = 0;   // Terminate the loop
static tsm_key_t gAdminKey; // The Admin Key

tsm_open_dp activeDp[TSM_MAX_ACTIVEDP];  // the array that manage the active dps

/* Manage signals : no distinctions .... improve */
void tsSignalHandler(int signum) {

	if (signum == SIGINT || signum == SIGQUIT) {
		INFO("Slight Time Series Server :: exiting, signal received %d\n", signum);
		bExitRun = 1;
		return;
	} else {
		ERROR("Slight Time Series Server :: Abort program, signal received. %d\n", signum);
       	exit(EXIT_FAILURE);
	}
	return;
}
// -----------------------------------
static void tsUseScreen(const char *exeName) {
	fprintf(stderr, "Slight Time Series DB version: %f \n",TSS_VER);
	fprintf(stderr, "A.Franco -2013- (L) \n\n");
	fprintf(stderr, "Usage: %s [-s] [-v <level>] [-p <HTTP port>] [-D <data path>]\n\n", exeName);
	fprintf(stderr, "        -s Don't daemonise - logs to stderr\n");
	fprintf(stderr, "        -D Path to data tree \n");
	fprintf(stderr, "        -p HTTP listen port\n");
	fprintf(stderr, "        -v Set verbosity level\n");
	exit(EXIT_FAILURE);   // terminate
}

// Make the process a daemon !
static void tsDaemonise(void) {
	pid_t pid, sid;
	if (getppid() == 1) { // Already a daemon
		return;
	}

	// Fork the process
	pid = fork();
	if (pid < 0) {
		ERROR("Slight Time Series Server :: Fork failed\n");
		exit(EXIT_FAILURE);
	}
	if (pid > 0) { // Child process forked - terminate parent
		INFO("Slight Time Series Server :: Daemon started in process %d\n", pid);
		exit(EXIT_SUCCESS);
	}

	// Clean up child's resources
	umask(0);
	sid = setsid();
	if (sid < 0) {
		ERROR("Slight Time Series Server :: setting sid failed\n");
		exit(EXIT_FAILURE);
	}

	// Finally redirect the IO
	freopen("/dev/null", "r", stdin);
	freopen("/dev/null", "w", stdout);
	freopen(TSS_LOG_FILE, "a", stderr);
	return;
}
// ---------------------------------


// =================  Main ========================
int main(int argc, char **argv)
{
	// Run variables
	int iDebugSwitch = 0;  // Print Debug info
	int iLogLevel = TSS_LOG_LEVEL;  // The log level
	struct sigaction newSignalH, oldSignalH;  // The signal management vars
//	struct MHD_Daemon *httpDaemon;  // The Handler to the HTTP daemon histance

	// Environment variables
	unsigned short usNetPort = TSS_PORT;
	char *sDataPath = NULL;

	// Parse options of command line
	int opt;
	while ((opt = getopt(argc, argv, "sD:p:v:")) != -1) {
		switch (opt) {
			case 's':
				iDebugSwitch = 1;
				break;
			case 'D':
				sDataPath = strdup(optarg);
				break;
			case 'p':
				usNetPort = atoi(optarg);
				break;
			case 'v':
				iLogLevel = atoi(optarg);
				break;
			default:
				tsUseScreen(argv[0]);
				break;
		}
	}

	// Actualize parameters
	tsLogSetLevel(iLogLevel);
	if (!iDebugSwitch)
		tsDaemonise();
	INFO("SlightTSDB Server :: starting version %f \n",TSS_VER);

	if (sDataPath == NULL) sDataPath = strdup(TSS_DATA_PATH);
	INFO("SlightTSDB Changing working directory to: %s\n", sDataPath);
	if (chdir(sDataPath) < 0) {
		ERROR("Slight Time Series Server :: failed changing working directory to: %s\n", sDataPath);
		exit(EXIT_FAILURE);
	}

	// Install signal handler	newSignalH.sa_handler = tsSignalHandler;
	sigemptyset(&newSignalH.sa_mask);
	newSignalH.sa_flags = 0;
	sigaction(SIGINT, &newSignalH, &oldSignalH);

	// Context Manager Clean UP
	int i;
	for(i=0;i<TSM_MAX_ACTIVEDP;i++){
		activeDp[i].ctx = NULL;
		activeDp[i].dpsId = 0;
	}

	// Now Generate or Read the admin key for the sha2 requests
	tsaGenAdminKey(TSM_PERSISTENT_ADMINKEY, &gAdminKey);

	// Start the jsonrpc server
	pthread_t serverThread;
    int rc = pthread_create(&serverThread, NULL, STSjsonrpcserver, (void *)&usNetPort);
	if (rc){
		ERROR("Slight Time Series Server :: failed to create server thread \n");
		bExitRun = 1;
	 }

	// The main loop : we can put an heart beat signal ....
	while (!bExitRun) {
		__tsmGarbageContex();
		sleep(1);
	}

	// Here we finished the work
	INFO("Slight Time Series Server :: terminating \n");
//	tsHttpdDstroy(httpDaemon);
	sigaction(SIGINT, &oldSignalH, NULL);
	my_free(__FUNCTION__,sDataPath);

	return 0;
}
