/*
 * STSjsonrpc.h
 *
 *  Created on: 17/dic/2013
 *      Author: fap
 */

#ifndef STSJSONRPC_H_
#define STSJSONRPC_H_


#define JRPC_PARSE_ERROR -32700
#define JRPC_INVALID_REQUEST -32600
#define JRPC_METHOD_NOT_FOUND -32601
#define JRPC_INVALID_PARAMS -32603
#define JRPC_INTERNAL_ERROR -32693
#define JRPC_FILESYSTEM_ERROR -32590

#include "cJSON.h"

typedef struct {
	void *data;
	int error_code;
	char * error_message;
} jrpc_context;

// typedef cJSON* (*jrpc_function)(jrpc_context *context, cJSON *params, cJSON* id);
typedef char* (*jrpc_function)(jrpc_context *context, cJSON *params, cJSON* id);

struct jrpc_procedure {
	char * name;
	jrpc_function function;
	void *data;
};

struct jrpc_server {
	int port_number;
	int procedure_count;
	struct jrpc_procedure *procedures;
	int debug_level;
};


// Functions prototipe definitions
void *STSjsonrpcserver(void *usPortNumber);
int __STSjsonrpcserver_init(struct jrpc_server *server, int port_number);
static int __STSjsonrpcserver_start(struct jrpc_server *server, int sockfd);
static void *__STSjsonrpc_get_in_addr(struct sockaddr *sa);
static int __STSjsonrpc_eval_request(struct jrpc_server *server, int conn, cJSON *root);
static int __STSjsonrpcinvoke_procedure(struct jrpc_server *server,	int conn, char *name, cJSON *params, cJSON *id);

static int __STSjsonrpc_send_response(int conn, char *response);
static int __STSjsonrpc_send_error(int conn, int code, char* message, cJSON * id) ;
//static int __STSjsonrpc_send_result(int conn, cJSON * result,	cJSON * id) ;
static int __STSjsonrpc_send_result(int conn, char * result, cJSON * id);
int __STSjsonrpc_register_procedure(struct jrpc_server *server,	jrpc_function function_pointer, char *name, void * data);


//
char * server_stat(jrpc_context * ctx, cJSON * params, cJSON *id) ;
char * get_dplist(jrpc_context * ctx, cJSON * params, cJSON *id) ;
char * create_dp(jrpc_context * ctx, cJSON * params, cJSON *id) ;
char * delete_dp(jrpc_context * ctx, cJSON * params, cJSON *id) ;
char * get_dpinfo(jrpc_context * ctx, cJSON * params, cJSON *id) ;
char * get_dpval(jrpc_context * ctx, cJSON * params, cJSON *id) ;
char * get_dplast(jrpc_context * ctx, cJSON * params, cJSON *id) ;
char * get_dpser(jrpc_context * ctx, cJSON * params, cJSON *id) ;
char * set_dpval(jrpc_context * ctx, cJSON * params, cJSON *id) ;

#endif /* STSJSONRPC_H_ */
