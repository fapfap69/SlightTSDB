/*
 * STSjsonrpc.c
 *
 *	json rpc server
 *
 *  Created on: 17/dic/2013
 *      Author: fap
 */

#include "SlightTSDB.h"
#include "STSCore.h"
#include "STSLog.h"
#include "STSManager.h"

#include "STSjsonrpc.h"


// Global variable Server context
struct jrpc_server my_server;

// This is the main function that run into a separate thread
void *STSjsonrpcserver(void *usPortNumber)
{

	FUNCTION_TRACE;

   uint port;
   port = *(uint *)usPortNumber;

   // init the server
   INFO("Initializing server at port : %u \n", port);
   int socket = __STSjsonrpcserver_init(&my_server, port);
   if(socket == -1) {
	   CRITICAL("Error in the Jsonrpc server intialization: abort !");
	   pthread_exit(NULL);
	   return(NULL);
   }

   // Now registers all the method ...
   __STSjsonrpc_register_procedure(&my_server, server_stat, "serverstatus", NULL );
   __STSjsonrpc_register_procedure(&my_server, get_dplist, "getdplist", NULL );
   __STSjsonrpc_register_procedure(&my_server, create_dp, "createdp", NULL );
   __STSjsonrpc_register_procedure(&my_server, delete_dp, "deletedp", NULL );
   __STSjsonrpc_register_procedure(&my_server, get_dpinfo, "getdpinfo", NULL );
   __STSjsonrpc_register_procedure(&my_server, get_dpval, "getdpval", NULL );
   __STSjsonrpc_register_procedure(&my_server, get_dplast, "getdplast", NULL );
   __STSjsonrpc_register_procedure(&my_server, get_dpser, "getdpser", NULL );
   __STSjsonrpc_register_procedure(&my_server, set_dpval, "setdpval", NULL );
   DEBUG("Register CallBack functions \n");

   // Finally starts the server
   INFO("Starting the server...\n");
   __STSjsonrpcserver_start(&my_server, socket);

   // When the server stops
   /* FIXME : We need to consider the server destroy procedure ?*/
   //  __STSjsonrpcserver_destroy(&my_server);
   INFO("The server was destroyed ! Bye \n");

   pthread_exit(NULL);
   return(NULL);
}

// This function inserts a procedure in the functions/methods table
int __STSjsonrpc_register_procedure(struct jrpc_server *server,
		jrpc_function function_pointer, char *name, void * data) {
	int i = server->procedure_count++;
	if (!server->procedures)
		server->procedures = my_malloc(__FUNCTION__,sizeof(struct jrpc_procedure));
	else {
		struct jrpc_procedure * ptr = realloc(server->procedures,
				sizeof(struct jrpc_procedure) * server->procedure_count);
		if (!ptr)
			return -1;
		server->procedures = ptr;
	}
	if ((server->procedures[i].name = strdup(name)) == NULL)
		return -1;
	server->procedures[i].function = function_pointer;
	server->procedures[i].data = data;
	DEBUG("Procedure %s registered !\n", name);
	return 0;
}

/* ============================================================
 *
 *   The Call Back functions are wrapping to the DBManager
 *   functions with the decoding of JSON paramiters and the
 *   formatting of the Output JSON object-
 *
 ============================================================== */
// request some description about the server
char * server_stat(jrpc_context * jctx, cJSON * params, cJSON *id) {

	FUNCTION_TRACE;

	char *buffer = my_malloc(__FUNCTION__,TS_MINDIMBUFFER);
	if(buffer == NULL) exit(-1);
	char *buf_ptr;

	sprintf(buffer , "{\n\t\"result\":\t{\n");
	strcat(buffer, "\t\t\"name\":\t\"Slight Time Series Server\",\n");
	strcat(buffer, "\t\t\"version\":\t");
	buf_ptr = buffer + strlen(buffer);
	sprintf(buf_ptr, "%2.1f,\n", 1.0);
#ifdef TSS_USEAUTHENTICATION
	strcat(buffer, "\t\t\"use authentication\": \"yes\",\n");
#endif
#ifdef TSM_DOUBLE_TYPE
	strcat(buffer, "\t\t\"use double format for data \": \"yes\",\n");
#endif
#ifdef TSM_LONG_TIMESTAMP
	strcat(buffer, "\t\t\"use milliseconds precision for time stamps \": \"yes\",\n");
#endif
#ifdef TSM_PTHREAD_LOCKING
	strcat(buffer, "\t\t\"use thread locking mode \": \"yes\",\n");
#endif
#ifdef TSM_REMOVE_DATAFILES
	strcat(buffer, "\t\t\"remove data files on Data Point deletion \": \"yes\",\n");
#endif
#ifdef TSM_JSON_OUTPUT
	strcat(buffer, "\t\t\"is JSON output format \": \"yes\",\n");
#endif
#ifdef TSM_CACHEPRESERVE
	strcat(buffer, "\t\t\"use cache write forcing \": \"yes\",\n");
#endif
	strcat(buffer, "\t\t\"maximum number of metrics for DP \": ");
	buf_ptr = buffer + strlen(buffer);
	sprintf(buf_ptr, "%d,\n", TSM_MAX_METRICS);
	strcat(buffer, "\t\t\"maximum number of opened DP \": ");
	buf_ptr = buffer + strlen(buffer);
	sprintf(buf_ptr, "%d,\n", TSM_MAX_ACTIVEDP);
	strcat(buffer, "\t\t\"unused DP alive time (sec) \": ");
	buf_ptr = buffer + strlen(buffer);
	sprintf(buf_ptr, "%d,\n", TSM_MAXUNUSEDTIME);
	strcat(buffer, "\t\t\"state\":\t\"OK\",\n");
	strcat(buffer, "\t\t\"opened DPs\":\t");
	buf_ptr = buffer + strlen(buffer);
	sprintf(buf_ptr, "%3d\n\t}\n", __tmsGetOpenedContexs());
	jctx->error_code = 0;
	return buffer;

}

// request the list of the DPs recorded
char * get_dplist(jrpc_context * jctx, cJSON * params, cJSON *id) {

	FUNCTION_TRACE;

	char *buffer = my_malloc(__FUNCTION__,TS_MIDDIMBUFFER);
	if(buffer == NULL) exit(-1);
	char *buf_ptr = buffer;

	sprintf(buffer , "{\n\t\"result\":\t{\n");
	strcat(buffer, "\t\t\"dplist\":\t");
	buf_ptr = buffer;

	if(tsmGetDPList(&buf_ptr, TS_MIDDIMBUFFER) < 0) {
		jctx->error_code = JRPC_FILESYSTEM_ERROR;
		jctx->error_message = strdup("dpList fails to scan directory !");
	}
	strcat(buffer, "\n\t\t}\n");
	return buffer;
}


// request to create a new DP
char * create_dp(jrpc_context * jctx, cJSON * params, cJSON *id) {

	FUNCTION_TRACE;

	uint64_t dpsId = 0;
	unsigned int minInterval =  TS_DEFAULT_SAMPLEINTERVAL;
	unsigned int cacheDim = TS_DEFAULT_CACHEDIM;
	unsigned int nmetrics = TS_DEFAULT_METRICS;
	tsm_downsample_mode_t *ds_mode, *ds_mode_ptr;
	unsigned int decimation = TS_DEFAULT_DECIMATION;
	unsigned int dbmode = tsmDBFile_Nolimit;
	unsigned long maxfilelength = 0;

	jctx->error_code = 0;

	cJSON *param, *item;
	int n,i;

	// Parse the JSON object containing the parameters and extracts they...
	param = cJSON_GetObjectItem(params, "dpId");
    if (param != NULL && param->type == cJSON_Number) {
    	dpsId = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "cacheDim");
    if (param != NULL && param->type == cJSON_Number) {
    	minInterval = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "interval");
    if (param != NULL && param->type == cJSON_Number) {
    	cacheDim = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "metrics");
    if (param != NULL && param->type == cJSON_Number) {
    	nmetrics = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "decimation");
    if (param != NULL && param->type == cJSON_Number) {
    	decimation = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "dbMode");
    if (param != NULL && param->type == cJSON_Number) {
    	dbmode = param->valueint;
    	if(dbmode != tsmDBFile_Nolimit || dbmode != tsmDBFile_Circular)
    		dbmode = tsmDBFile_Nolimit;
    }
    param = cJSON_GetObjectItem(params, "fileLength");
    if (param != NULL && param->type == cJSON_Number) {
    	maxfilelength = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "dsModes");
    if (param != NULL && param->type == cJSON_Array) {
    	n = cJSON_GetArraySize(param);
    	ds_mode = my_malloc(__FUNCTION__,sizeof(tsm_downsample_mode_t) * n );
    	if(ds_mode == NULL) {
    		dpsId = -1;
    		ERROR("Error allocating memory buffer !\n");
    	}  else {
    		ds_mode_ptr = ds_mode;
    		for(i=0;i<n;i++) {
    			item = cJSON_GetArrayItem(param,i);
    			*ds_mode_ptr++ = item->valueint;
    		}
    	}
    }

    // Process ...
	char *buffer = my_malloc(__FUNCTION__,TS_MINDIMBUFFER);
	if(buffer == NULL) exit(-1);
	char *buf_ptr = buffer;

	sprintf(buffer , "{\n\t\"result\":\t{\n");
	strcat(buffer, "\t\t{\t\"createdp\":\t");
	buf_ptr = buffer;

    if(dpsId > 0) { // This is Ok
    	if(tsmCreate(dpsId,minInterval,cacheDim,nmetrics,ds_mode,
			decimation,dbmode,maxfilelength) != 0) {
    		jctx->error_code = JRPC_INVALID_PARAMS;
    		jctx->error_message = strdup("DP creation fails !");
    	} else {
    		buf_ptr = buffer + strlen(buffer);
    		sprintf(buf_ptr, "\"dp %llu created\"\n\t\t}\n", dpsId);
    	}
	} else {
		jctx->error_code = JRPC_INVALID_PARAMS;
		jctx->error_message = strdup("DP creation fails !");
	}

    if(ds_mode != NULL) my_free(__FUNCTION__,ds_mode);
	return (buffer);
}

// request to remove a DP
char * delete_dp(jrpc_context * jctx, cJSON * params, cJSON *id) {

	FUNCTION_TRACE;

	uint64_t dpsId = 0;
	jctx->error_code = 0;

	// Get parameter...
	cJSON *param;
	param = cJSON_GetObjectItem(params, "dpId");
    if (param != NULL && param->type == cJSON_Number) {
    	dpsId = param->valueint;
    }

	char *buffer = my_malloc(__FUNCTION__,TS_MINDIMBUFFER);
	if(buffer == NULL) exit(-1);
	char *buf_ptr = buffer;

	sprintf(buffer , "{\n\t\"result\":\t{\n");
	strcat(buffer, "\t\t{\t\"deletedp\":\t");
	buf_ptr = buffer;

    if( tsmDelete(dpsId) != 0) {
        jctx->error_code = JRPC_INVALID_PARAMS;
        jctx->error_message = strdup("DP deletion fails !");
	} else {
		buf_ptr = buffer + strlen(buffer);
		sprintf(buf_ptr, "\"dp %llu deleted\"\n\t\t}\n", dpsId);
	}

    return buffer;
}

// Dump a JSON object with the main info related to a DP
char * get_dpinfo(jrpc_context * jctx, cJSON * params, cJSON *id) {

	FUNCTION_TRACE;

	uint64_t dpsId = 0;
	jctx->error_code = 0;

	// Get parameter...
	cJSON *param;
	param = cJSON_GetObjectItem(params, "dpId");
    if (param != NULL && param->type == cJSON_Number) {
    	dpsId = param->valueint;
    }

	char *buffer = my_malloc(__FUNCTION__,TS_MIDDIMBUFFER);
	if(buffer == NULL) exit(-1);
	char *buf_ptr = buffer;

	sprintf(buffer , "{\n\t\"result\":\t{\n");
	strcat(buffer, "\t\t\"dpinfo\":");
	buf_ptr = buffer;

	if(tsmDumpMetaData(dpsId, &buf_ptr, TS_MIDDIMBUFFER) == NULL) {
		jctx->error_code = JRPC_INVALID_REQUEST;
		jctx->error_message = strdup("dpInfo fails to read DataPoint metadata !");
	}

	strcat(buffer, "\t}\n");
	return buffer;
}

// Return a JSON object containing the time:value couple
char * get_dpval(jrpc_context * jctx, cJSON * params, cJSON *id) {

	FUNCTION_TRACE;

	uint64_t dpsId = 0;
	jctx->error_code = 0;
	tsm_time timestamp = TSM_NO_TIMESTAMP;

	// Get parameter...
	cJSON *param;
	param = cJSON_GetObjectItem(params, "dpId");
    if (param != NULL && param->type == cJSON_Number) {
    	dpsId = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "timestamp");
    if (param != NULL && param->type == cJSON_Number) {
    	timestamp = param->valuedouble * TSM_TIMESTAMP_PRECISION;
    }

	char *buffer = my_malloc(__FUNCTION__,TS_MIDDIMBUFFER);
	if(buffer == NULL) exit(-1);
	char *buf_ptr = buffer;

	sprintf(buffer , "{\n\t\"result\":\t{\n");
	strcat(buffer, "\t\t\"dpval\":\t{\n");
	buf_ptr = buffer;

	if(tsmGetDPValue(dpsId, &timestamp, &buffer, TS_MIDDIMBUFFER) == NULL) {
		jctx->error_code = JRPC_INVALID_REQUEST;
		jctx->error_message = strdup("dpVal fails to read DataPoint value !");
	}

	strcat(buffer, "\t\t}\n\t}\n");
	return buffer;
}

// Return a JSON object containing the last time:value recorded couple
char * get_dplast(jrpc_context * jctx, cJSON * params, cJSON *id) {

	FUNCTION_TRACE;

	uint64_t dpsId = 0;
	jctx->error_code = 0;
	tsm_time timestamp = TSM_NO_TIMESTAMP;

	// Get parameter...
	cJSON *param;
	param = cJSON_GetObjectItem(params, "dpId");
    if (param != NULL && param->type == cJSON_Number) {
    	dpsId = param->valueint;
    }

	char *buffer = my_malloc(__FUNCTION__,TS_MIDDIMBUFFER);
	if(buffer == NULL) exit(-1);
	char *buf_ptr = buffer;

	sprintf(buffer , "{\n\t\"result\":\t{\n");
	strcat(buffer, "\t\t\"dplast\":\t{\n");
	buf_ptr = buffer;

    timestamp = tsmGetLatest(dpsId);
	if(tsmGetDPValue(dpsId, &timestamp, &buffer, TS_MIDDIMBUFFER) == NULL) {
		jctx->error_code = JRPC_INVALID_REQUEST;
		jctx->error_message = strdup("dpLast fails to read DataPoint value !");
	}

	strcat(buffer, "\t\t}\n\t}\n");
	return buffer;
}

// Return a JSON object containing an array of time:value couples related to a metric
char * get_dpser(jrpc_context * jctx, cJSON * params, cJSON *id) {

	FUNCTION_TRACE;

	uint64_t dpsId = 0;
	jctx->error_code = 0;
	tsm_time timestart = TSM_NO_TIMESTAMP;
	tsm_time timeend = TSM_NO_TIMESTAMP;
	int metric = 0;
	unsigned int maxvalues = 3000;

	// Get parameters...
	cJSON *param;
	param = cJSON_GetObjectItem(params, "dpId");
    if (param != NULL && param->type == cJSON_Number) {
    	dpsId = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "timestart");
    if (param != NULL && param->type == cJSON_Number) {
    	timestart = param->valuedouble * TSM_TIMESTAMP_PRECISION;
    }
    param = cJSON_GetObjectItem(params, "timeend");
    if (param != NULL && param->type == cJSON_Number) {
    	timeend = param->valuedouble * TSM_TIMESTAMP_PRECISION;
    }
    param = cJSON_GetObjectItem(params, "metric");
    if (param != NULL && param->type == cJSON_Number) {
    	metric = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "maxvalues");
    if (param != NULL && param->type == cJSON_Number) {
    	maxvalues = param->valueint;
    }

    // Process ...
	char *buffer = my_malloc(__FUNCTION__,TS_MAXDIMBUFFER);
	if(buffer == NULL) exit(-1);
	char *buf_ptr = buffer;

	sprintf(buffer , "{\n\t\"result\":\t{\n");
	strcat(buffer, "\t\t\"dpserie\":\t{\n");
	buf_ptr = buffer;

	if(tsmGetOneSerie(dpsId, metric, timestart, timeend,
			maxvalues, &buf_ptr, TS_MAXDIMBUFFER) == NULL) {
		jctx->error_code = JRPC_INVALID_REQUEST;
		jctx->error_message = strdup("dpSerie fails to read DataPoint values !");
	}
	strcat(buf_ptr, "\t\t}\n\t}\n");
	return buf_ptr;
}

// Records a Data Point into the DB
char * set_dpval(jrpc_context * jctx, cJSON * params, cJSON *id) {

	FUNCTION_TRACE;

	uint64_t dpsId = 0;
	jctx->error_code = 0;
	tsm_time timestamp = getNow();
	int i,n;
	char *values = NULL;
	tsm_data_t *values_ptr = NULL;

	// Get parameters...
	cJSON *param;
	cJSON *item;
	param = cJSON_GetObjectItem(params, "dpId");
    if (param != NULL && param->type == cJSON_Number) {
    	dpsId = param->valueint;
    }
    param = cJSON_GetObjectItem(params, "timestamp");
    if (param != NULL && param->type == cJSON_Number) {
    	timestamp = param->valuedouble * TSM_TIMESTAMP_PRECISION;
    }
    param = cJSON_GetObjectItem(params, "values");
    if (param != NULL && param->type == cJSON_Array) {
    	if((n = cJSON_GetArraySize(param)) > 0) {
    		values = my_malloc(__FUNCTION__,sizeof(tsm_data_t)*n+2);
    		if(values == NULL) {
    			ERROR("Error allocating the Values memory buffer !");
    			return(NULL);
    		}
    		values_ptr = (tsm_data_t *)values;
    		// move values in the input buffer
    		for(i=0;i<n;i++) {
    			item = cJSON_GetArrayItem(param,i);
    			*values_ptr++ = (tsm_data_t)item->valuedouble;
    		}
    		values_ptr = (tsm_data_t *)values;
    	} else {
        	INFO("No values given !");
        	return(NULL);
    	}
   	} else {
   		//Nothing to do
    	INFO("No values given !");
    	return(NULL);
   	}

    // allocate the output buffer
	char *buffer = my_malloc(__FUNCTION__,TS_MINDIMBUFFER);
	if(buffer == NULL) {
    	ERROR("Error allocating the Output memory buffer !");
    	my_free(__FUNCTION__,values);
    	return(NULL);
    }

    if(tsmStoreDataPoint(dpsId, &timestamp, values_ptr)) {
		jctx->error_code = JRPC_INVALID_REQUEST;
		jctx->error_message = strdup("dpSet fails to write DataPoint values !");
	} else {
		strcat(buffer , "{\n\t\"result\":\t{\"dpset\":\tOK}\n");
	}
    my_free(__FUNCTION__,values);
    return buffer;
}

/* =========================================================
 *                  JSON RPC SERVER FUNCTIONS
 * ========================================================= */

// Init the server
int __STSjsonrpcserver_init(struct jrpc_server *server, int port_number) {

	FUNCTION_TRACE;

	// Setup of the Inet environment
	memset(server, 0, sizeof(struct jrpc_server));
	server->port_number = port_number;

	char port[6];
	sprintf(port, "%d", server->port_number);

	struct addrinfo hints, *servinfo, *p;
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE; // use my IP

	int rv;
	if ((rv = getaddrinfo(NULL, port, &hints, &servinfo)) != 0) {
		ERROR("getaddrinfo: %s error\n", gai_strerror(rv));
		return -1;
	}

	int sockfd;
	int yes = 1;
	unsigned int len;
	struct sockaddr_in sockaddr;
	// loop through all the results and bind to the first we can
	for (p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
			ERROR("Socket error\n");
			continue;
		}
		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))	== -1) {
			CRITICAL("Setsockopt error\n");
			exit(EXIT_FAILURE);
		}
		if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			close(sockfd);
			ERROR("Error executing bind\n");
			continue;
		}
		len = sizeof(sockaddr);
		if (getsockname(sockfd, (struct sockaddr *) &sockaddr, &len) == -1) {
			close(sockfd);
			ERROR("Getsockname error\n");
			continue;
		}
		server->port_number = ntohs( sockaddr.sin_port );
		break;
	}

	if (p == NULL) {
		ERROR("Failed to bind\n");
		return -1;
	}

	freeaddrinfo(servinfo); // all done with this structure

	INFO("Socket init Done !\n");
	return (sockfd);

}

// Start the jRPC Server accept the incoming connections
static int __STSjsonrpcserver_start(struct jrpc_server *server, int sockfd) {

	FUNCTION_TRACE;

	if (listen(sockfd, 5) == -1) {
		CRITICAL("Error on listen !\n");
		exit(EXIT_FAILURE);
	}

	DEBUG("Waiting for connections...\n");
	char s[INET6_ADDRSTRLEN];
	struct sockaddr_storage their_addr; // connector's address information
	socklen_t sin_size;
	sin_size = sizeof their_addr;
	int conn_s; /*  connection socket         */
	int readbytes = 0;

	#define MAXSOCKETBUF 1024

	/*  Enter an infinite loop to respond
      to client requests and echo input  */

	char *buffer; // The file read buffer
	char *end_ptr; // Buffer mamagement pointers
	char *start_ptr;
	cJSON *root; // The main cJSON object
	char *str_result; // Pointer to the result

    while ( 1 ) {
    	conn_s = accept(sockfd, (struct sockaddr *) &their_addr,	&sin_size);
    	if (conn_s == -1) {
    		ERROR("Accept function error ...\n");
    	} else {
    		buffer = my_malloc(__FUNCTION__,MAXSOCKETBUF);
    		if(buffer == NULL) {
    			ERROR("Accept function error to allocate buffer!\n");
    			close(conn_s);
    			return 2;
    		}
    		inet_ntop(their_addr.ss_family,	__STSjsonrpc_get_in_addr((struct sockaddr *) &their_addr), s, sizeof(s) );
    		INFO("Accept function got connection from %s\n", s);

    		if ((readbytes = read(conn_s, buffer, MAXSOCKETBUF)) == -1) {
    			ERROR("Accept function : error reading stream ! \n");
			}
    		if (readbytes) {
    			start_ptr = buffer;
    			while ((root = cJSON_Parse_Stream(start_ptr, &end_ptr)) != NULL) {
    				str_result = cJSON_Print(root);
    				DEBUG("Accept function : Valid JSON Received:\n%s\n", str_result);
    				my_free(__FUNCTION__,str_result);
    				if (root->type == cJSON_Object) {
    					__STSjsonrpc_eval_request(server, conn_s, root);
    				}
    				//shift processed request, discarding it
    				start_ptr = end_ptr;
    				cJSON_Delete(root);
    			}
    		}
			my_free(__FUNCTION__,buffer);
			close(conn_s);
    	}
    }
	return 0;
}

// Evaluate the socket request
static int __STSjsonrpc_eval_request(struct jrpc_server *server, int conn, cJSON *root) {

	FUNCTION_TRACE;

	// Get the params : the Method indicate the function to perform
	cJSON *method, *params, *id;
	int res;
    method = cJSON_GetObjectItem(root, "method");
    if (method != NULL && method->type == cJSON_String) {
    	params = cJSON_GetObjectItem(root, "params");
    	if (params == NULL|| params->type == cJSON_Array || params->type == cJSON_Object) {
   			id = cJSON_GetObjectItem(root, "id");
   			if (id == NULL || id->type == cJSON_String || id->type == cJSON_Number) {
    			//We have to copy ID because using it on the reply and deleting the response Object will also delete ID
    			cJSON *id_copy = NULL;
    			if (id != NULL)	id_copy = (id->type == cJSON_String) ? cJSON_CreateString(id->valuestring) : cJSON_CreateNumber(id->valueint);
    			INFO("Method Invoked: %s\n", method->valuestring);
    			res = __STSjsonrpcinvoke_procedure(server, conn, method->valuestring, params, id_copy);
    			cJSON_Delete(id_copy);
    			return(res);
    		}
    	}
  	}

    // Send back an error result
    char *ptr = strdup("The JSON sent is not a valid Request object.");
    __STSjsonrpc_send_error(conn, JRPC_INVALID_REQUEST,ptr, NULL);
    my_free(__FUNCTION__,ptr);
   	return -1;
}

// Now invoke the function related to the Web request
static int __STSjsonrpcinvoke_procedure(struct jrpc_server *server,
		int conn, char *name, cJSON *params, cJSON *id) {

	FUNCTION_TRACE;

	int ret_val = 0;
	char *returned = NULL;
	int procedure_found = 0;
	jrpc_context jctx;
	jctx.error_code = 0;
	jctx.error_message = NULL;

	// Search the function and run it
	int i = server->procedure_count;
	while (i--) {
		if (!strcmp(server->procedures[i].name, name)) {
			procedure_found = 1;
			jctx.data = server->procedures[i].data;
			returned = server->procedures[i].function(&jctx, params, id);
			break;
		}
	}
	if (!procedure_found) {  // The specified method don't have a related function
		jctx.error_code = JRPC_METHOD_NOT_FOUND;
		jctx.error_message = strdup("invoke_procedure :: Method not found.");
		ret_val = __STSjsonrpc_send_error(conn, jctx.error_code, jctx.error_message, id);
		my_free(__FUNCTION__,jctx.error_message);
	} else { // The function was performed
		if (jctx.error_code)
			ret_val = __STSjsonrpc_send_error(conn, jctx.error_code, jctx.error_message, id);
		else
			ret_val = __STSjsonrpc_send_result(conn, returned, id);
		if(jctx.error_message != NULL) my_free(__FUNCTION__,jctx.error_message);
	}
	my_free(__FUNCTION__,returned);
	return(ret_val);
}

// get sockaddr, IPv4 or IPv6:
static void *__STSjsonrpc_get_in_addr(struct sockaddr *sa) {
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*) sa)->sin_addr);
	}
	return &(((struct sockaddr_in6*) sa)->sin6_addr);
}

// Send back a JSON response
static int __STSjsonrpc_send_response(int conn, char *response) {
	write(conn, response, strlen(response));
	write(conn, "\n", 1);
	return 0;
}

// Send back a JSON object that contains the error
static int __STSjsonrpc_send_error(int conn, int code, char* message, cJSON * id) {
	int return_value = 0;
	cJSON *result_root = cJSON_CreateObject();
	cJSON *error_root = cJSON_CreateObject();
	cJSON_AddNumberToObject(error_root, "code", code);
	cJSON_AddStringToObject(error_root, "message", message);
	cJSON_AddItemToObject(result_root, "error", error_root);
	if(id != NULL) cJSON_AddItemToObject(result_root, "id", id);
	char * str_result = cJSON_Print(result_root);
	return_value = __STSjsonrpc_send_response(conn, str_result);
	cJSON_Delete(result_root);
//	cJSON_Delete(error_root);
	my_free(__FUNCTION__,str_result);
	return return_value;
}

// Send back a JSON object containing the result
static int __STSjsonrpc_send_result(int conn, char * result, cJSON * id) {
	int return_value = 0;
	if(id != NULL) {
		strcat(result, ",\t\"id\":\t");
		char buf[20];
		if (id->type == cJSON_String) {
			sprintf(buf, "\"%s\"\n", id->valuestring);
			strcat(result, buf);
		} else {
			sprintf(buf, "%d\n", id->valueint);
			strcat( result, buf);
		}
	}
	strcat(result, "}");
	return_value = __STSjsonrpc_send_response(conn, result);
	return return_value;
}


// ------------------------------  EOF ---------------------------------------
