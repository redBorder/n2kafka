/*
** Copyright (C) 2015 Eneo Tecnologia S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU Affero General Public License as
** published by the Free Software Foundation, either version 3 of the
** License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU Affero General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "config.h"

#ifdef HAVE_LIBMICROHTTPD

#define HTTP_UNUSED __attribute__((unused))
#define POSTBUFFERSIZE (10*1024)

#define MODE_THREAD_PER_CONNECTION "thread_per_connection"
#define MODE_SELECT "select"
#define MODE_POLL "poll"
#define MODE_EPOLL "epoll"

#include "http.h"
#include "rb_addr.h"

#include "global_config.h"

#include <arpa/inet.h>
#include <assert.h>
#include <jansson.h>
#include <librd/rdlog.h>
#include <librd/rdmem.h>
#include <microhttpd.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

#define STRING_INITIAL_SIZE 2048

struct string {
	char *buf;
	size_t allocated,used;
};

#define HTTP_PRIVATE_MAGIC 0xC0B345FE
struct http_private{
#ifdef HTTP_PRIVATE_MAGIC
	uint64_t magic;
#endif
	struct MHD_Daemon *d;
	int redborder_uri;
    decoder_callback callback;
	void *callback_opaque;
};

static size_t smax(size_t n1, size_t n2) {
	return n1>n2?n1:n2;
}

static size_t smin(size_t n1, size_t n2) {
	return n1>n2?n2:n1;
}

static int init_string(struct string *s,size_t size) {
	s->buf = malloc(size);
	if(s->buf) {
		s->allocated = size;
		return 1;
	}
	return 0;
}

static size_t string_free_space(const struct string *str) {
	return str->allocated - str->used;
}

static size_t string_grow(struct string *str,size_t delta) {
	const size_t newsize = smax(str->allocated + delta,str->allocated*2);
	char *new_buf = realloc(str->buf,newsize);
	if(NULL != new_buf) {
		str->buf = new_buf;
		str->allocated = newsize;
	}
	return str->allocated;
}

struct conn_info {
	const char *topic;
	const char *client;
	const char *sensor_uuid;
	struct string str;
};

static void free_con_info(struct conn_info *con_info) {
	free(con_info->str.buf);
	con_info->str.buf = NULL;
	free(con_info);
}

static void prepare_decoder_params(struct conn_info *con_info,struct pair *mem,
        size_t memsiz,keyval_list_t *list) {
	assert(3==memsiz);
	memset(mem,0,sizeof(*mem)*3);

	mem[0].key   = "topic";
	mem[0].value = con_info->topic;
	mem[1].key   = "sensor_uuid";
	mem[1].value = con_info->sensor_uuid;
	mem[2].key   = "client_ip";
	mem[2].value = con_info->client;

	add_key_value_pair(list,&mem[0]);
	add_key_value_pair(list,&mem[1]);
	add_key_value_pair(list,&mem[2]);
}

static void request_completed (void *cls,
                               struct MHD_Connection *connection HTTP_UNUSED,
                               void **con_cls,
                               enum MHD_RequestTerminationCode toe)
{
	if( NULL == con_cls || NULL == *con_cls) {
		return; /* This point should never reached? */
	}

	if(toe != MHD_REQUEST_TERMINATED_COMPLETED_OK) {
		rdlog(LOG_ERR,"Connection terminated because %d",toe);
	}

	struct conn_info *con_info = *con_cls;
	struct http_private *h = cls;
#ifdef HTTP_PRIVATE_MAGIC
	assert(HTTP_PRIVATE_MAGIC == h->magic);
#endif

	keyval_list_t decoder_params = keyval_list_initializer(decoder_params);
	struct pair decoder_opts[3];

	prepare_decoder_params(con_info,decoder_opts,RD_ARRAY_SIZE(decoder_opts),
		&decoder_params);

	h->callback(con_info->str.buf,con_info->str.used,
		&decoder_params,h->callback_opaque,NULL);
	con_info->str.buf = NULL; /* librdkafka will free it */
	
	free_con_info(con_info);
	*con_cls = NULL;
}

static struct conn_info *create_connection_info(size_t string_size,const char *topic, 
                                            const char *client,const char *s_uuid) {
	/* First call, creating all needed structs */

	struct conn_info *con_info = NULL;

	rd_calloc_struct(&con_info,sizeof(*con_info),
		topic?-1:0,topic,&con_info->topic,
		client?-1:0,client,&con_info->client,
		s_uuid?-1:0,s_uuid,&con_info->sensor_uuid,
		RD_MEM_END_TOKEN);
	
	if( NULL == con_info ){
		rdlog(LOG_ERR,"Can't allocate conection context (out of memory?)");
		return NULL; /* Doesn't have resources */
	}

	if ( !init_string(&con_info->str,string_size) ) {
		rdlog(LOG_ERR,"Can't allocate connection string buffer (out of memory?)");
		free_con_info(con_info);
		return NULL; /* Doesn't have resources */
	}

	return con_info;
}

static int send_buffered_response(struct MHD_Connection *con,size_t sz,
                   char *buf,int buf_kind,unsigned int response_code,
                   int (*custom_response)(struct MHD_Response *)) {
	struct MHD_Response *http_response = MHD_create_response_from_buffer(
		sz,buf,buf_kind);

	if(NULL == http_response) {
		rdlog(LOG_CRIT,"Can't create HTTP response");
		return MHD_NO;
	}

	if(custom_response) {
		custom_response(http_response);
	}

	const int ret = MHD_queue_response(con,response_code,http_response);
	MHD_destroy_response(http_response);
	return ret;
}

static int send_http_ok(struct MHD_Connection *connection) {
	return send_buffered_response(connection,0,NULL,MHD_RESPMEM_PERSISTENT,
		MHD_HTTP_OK,NULL);
}

static int customize_method_not_allowed_response(struct MHD_Response *response) {
	return MHD_add_response_header(response, "Allow", "POST");
}


static int send_http_method_not_allowed(struct MHD_Connection *connection) {
	return send_buffered_response(connection,0,NULL,MHD_RESPMEM_PERSISTENT,
		MHD_HTTP_METHOD_NOT_ALLOWED,customize_method_not_allowed_response);
}

static int send_http_forbidden(struct MHD_Connection *connection) {
	return send_buffered_response(connection,0,NULL,MHD_RESPMEM_PERSISTENT,
		MHD_HTTP_FORBIDDEN,NULL);
}

static int send_http_unauthorized(struct MHD_Connection *connection) {
	return send_buffered_response(connection,0,NULL,MHD_RESPMEM_PERSISTENT,
		MHD_HTTP_UNAUTHORIZED,NULL);
}

static int send_http_bad_request(struct MHD_Connection *connection) {
	return send_buffered_response(connection,0,NULL,MHD_RESPMEM_PERSISTENT,
		MHD_HTTP_BAD_REQUEST,NULL);
}

static size_t append_http_data_to_connection_data(struct conn_info *con_info,
												  const char *upload_data,
												  size_t upload_data_size) {

	if( upload_data_size > string_free_space(&con_info->str) ) {
		/* TODO error handling */
		string_grow(&con_info->str,upload_data_size);
	}

	size_t ncopy = smin(upload_data_size,string_free_space(&con_info->str));
	strncpy(&con_info->str.buf[con_info->str.used],upload_data,ncopy);
	con_info->str.used += ncopy;
	return ncopy;
}

/* Return code: Valid prefix (i.e., /rbdata/)*/
static int extract_rb_url_info(const char *url,size_t url_len,char *dst,
								const char **uuid, const char **topic) {
	assert(url);
	assert(dst);
	assert(uuid);
	assert(topic);

	char *aux=NULL;
	memcpy(dst,url,url_len+1);

	const char *rbdata = strtok_r(dst,"/",&aux);
	const int invalid_rbdata = (NULL == rbdata) || strcmp(rbdata,"rbdata");
	if(!invalid_rbdata) {
		*uuid = strtok_r(NULL,"/",&aux);
		*topic = strtok_r(NULL,"/",&aux);
	}
	return !invalid_rbdata;
}

static const char *client_addr(char *buf, size_t buf_size,
                            struct MHD_Connection *con_info) {
	const union MHD_ConnectionInfo *cinfo = MHD_get_connection_info(con_info,
	                                    MHD_CONNECTION_INFO_CLIENT_ADDRESS);
	if(NULL == cinfo || NULL == cinfo->client_addr) {
		rdlog(LOG_WARNING,"Can't obtain client address info to print debug message.");
		return NULL;
	}

	return sockaddr2str(buf, buf_size, cinfo->client_addr);
}

static int rb_http2k_validation(struct MHD_Connection *con_info,const char *url,
							struct rb_database *rb_database, int *allok,
							char **ret_topic,char **ret_uuid,const char *source) {

	/*
	 * Need to validate that URL is valid:
	 * POST https://<host>/<sensor_uuid>/<topic>
	 */
	const char *uuid=NULL,*topic=NULL;
	const size_t url_len = strlen(url);
	char my_url[url_len+1];
	const int valid_prefix = extract_rb_url_info(url,url_len,my_url,&uuid,&topic);

	/// @TODO check uuid url/message equality
	if(!valid_prefix || NULL == uuid || NULL == topic) {
		if(!valid_prefix) {
			rdlog(LOG_WARNING,"Received no expected prefix url [%s] from %s. "
				"Closing connection.",url,source);
		}
		if(NULL == uuid) {
			rdlog(LOG_WARNING,"Received no uuid/topic in url [%s] from %s. "
				"Closing connection.",url,source);
		} else if(NULL == topic) {
			rdlog(LOG_WARNING,"Received no topic in url [%s] from %s. "
				"Closing connection.",url,source);
		}
		*allok = 0;
		return send_http_bad_request(con_info);
	}

	rdlog(LOG_DEBUG,"Receiving message with uuid '%s' and topic '%s' from "
		"client %s",uuid,topic,source);

	const int valid_uuid = rb_http2k_validate_uuid(rb_database,uuid);
	if(!valid_uuid) {
		rdlog(LOG_WARNING,"Received invalid uuid %s from %s. Closing connection.",uuid,
			source);
		*allok = 0;
		return send_http_unauthorized(con_info);
	}

	if(NULL != topic) {
		const int valid_topic = rb_http2k_validate_topic(&global_config.rb.database,topic);

		if(!valid_topic) {
			rdlog(LOG_WARNING,"Received topic %s from %s. Closing connection.",topic?topic:NULL,
				source);
			*allok = 0;
			return send_http_forbidden(con_info);
		}
	} else {
		rdlog(LOG_WARNING,"Received no topic in url [%s] from %s. Closing connection.",url,
			source);
		*allok = 0;
		return send_http_bad_request(con_info);
	}

	*allok = 1;

	if(ret_topic) {
		*ret_topic = strdup(topic);
	}
	if(ret_uuid) {
		*ret_uuid = strdup(uuid);
	}

	return MHD_YES;
}

static int post_handle(void *_cls,
						 struct MHD_Connection *connection,
						 const char *url,
						 const char *method,
						 const char *version HTTP_UNUSED,
						 const char *upload_data,
						 size_t *upload_data_size,
						 void **ptr) {
	struct http_private * cls = _cls;
#ifdef HTTP_PRIVATE_MAGIC
	assert(HTTP_PRIVATE_MAGIC == cls->magic);
#endif

	if (0 != strcmp(method, MHD_HTTP_METHOD_POST)) {
		rdlog(LOG_WARNING,"Received invalid method %s. "
			"Returning METHOD NOT ALLOWED.",method);
		return send_http_method_not_allowed(connection);
	}

	if ( NULL == ptr) {
		return MHD_NO;
	}

	if ( NULL == *ptr ) {
		char client_buf[BUFSIZ];
		const char *client = client_addr(client_buf,sizeof(client_buf),connection);
		if(NULL == client) {
			return MHD_NO;
		}
		/* First message of connection */
		char *topic = NULL,*uuid=NULL;
		if (cls->redborder_uri) {
			int aok = 1;
			const int rc = rb_http2k_validation(connection,url,
				&global_config.rb.database,&aok,&topic,&uuid,client);
			if(0 == aok) {
				free(topic);
				return rc;
			}
		}
		*ptr = create_connection_info(STRING_INITIAL_SIZE,topic,client,uuid);
		free(topic);
		free(uuid);
		return (NULL == *ptr) ? MHD_NO : MHD_YES;
	} else if ( *upload_data_size > 0 ) {
		/* middle calls, process string sent */
		struct conn_info *con_info = *ptr;
		const size_t rc = append_http_data_to_connection_data(con_info,
		                                upload_data,*upload_data_size);
		(*upload_data_size) -= rc;
		return (*upload_data_size != 0) ? MHD_NO : MHD_YES;

	} else {
		/* Send OK. Resources will be freed in request_completed */
		return send_http_ok(connection);
	}
}

struct http_loop_args {
	const char *mode;
	int port;
	int num_threads;
	struct{
		int connection_memory_limit;
		int connection_limit;
		int connection_timeout;
		int per_ip_connection_limit;
	}server_parameters;
	int redborder_uri;
};

static struct http_private *start_http_loop(const struct http_loop_args *args,
                                            decoder_callback callback,void *cb_opaque) {
	struct http_private *h = NULL;

	unsigned int flags = 0;
	if(args->mode == NULL || 0==strcmp(MODE_THREAD_PER_CONNECTION,
	                                   args->mode)) {
		flags |= MHD_USE_THREAD_PER_CONNECTION;
	} else if(0==strcmp(MODE_SELECT,args->mode)) {
		flags |= MHD_USE_SELECT_INTERNALLY;
	} else if(0==strcmp(MODE_POLL,args->mode)) {
		flags |= MHD_USE_POLL_INTERNALLY;
	} else if(0==strcmp(MODE_EPOLL,args->mode)) {
		flags |= MHD_USE_EPOLL_INTERNALLY_LINUX_ONLY;
	} else {
		rdlog(LOG_ERR,"Not a valid HTTP mode. Select one between("
		    MODE_THREAD_PER_CONNECTION "," MODE_SELECT "," MODE_POLL "," 
		    MODE_EPOLL ")");
		return NULL;
	}

	flags |= MHD_USE_DEBUG;

	h = calloc(1,sizeof(*h));
	if(!h) {
		rdlog(LOG_ERR,"Can't allocate LIBMICROHTTPD private"
		         " (out of memory?)");
		return NULL;
	}
#ifdef HTTP_PRIVATE_MAGIC
	h->magic = HTTP_PRIVATE_MAGIC;
#endif
	h->callback = callback;
	h->callback_opaque = cb_opaque;
	h->redborder_uri = args->redborder_uri;

	struct MHD_OptionItem opts[] = {
		{MHD_OPTION_NOTIFY_COMPLETED, (intptr_t)&request_completed, h},
		
		/* Digest-Authentication related. Setting to 0 saves some memory */
		{MHD_OPTION_NONCE_NC_SIZE, 0, NULL},

		/* Max number of concurrent onnections */
		{MHD_OPTION_CONNECTION_LIMIT, 
			args->server_parameters.connection_limit, NULL},

		/* Max number of connections per IP */
		{MHD_OPTION_PER_IP_CONNECTION_LIMIT, 
			args->server_parameters.per_ip_connection_limit, NULL},

		/* Connection timeout */
		{MHD_OPTION_CONNECTION_TIMEOUT, 
			args->server_parameters.connection_timeout, NULL},

		/* Memory limit per connection */
		{MHD_OPTION_CONNECTION_MEMORY_LIMIT, 
			args->server_parameters.connection_memory_limit, NULL},

		/* Thread pool size */
		{MHD_OPTION_THREAD_POOL_SIZE, args->num_threads, NULL},

		{ MHD_OPTION_END, 0, NULL }
	};

	h->d = MHD_start_daemon(flags,
		args->port,
		NULL, /* Auth callback */
		NULL, /* Auth callback parameter */
		post_handle, /* Request handler */
		h, /* Request handler parameter */
		MHD_OPTION_ARRAY, opts,
		MHD_OPTION_END);

	if(NULL == h->d) {
		rdlog(LOG_ERR,"Can't allocate LIBMICROHTTPD handler"
		         " (out of memory?)");
		free(h);
		return NULL;
	}

	return h;
}

static void reload_listener_http(json_t *new_config,
                decoder_listener_opaque_reload opaque_reload,
                void *cb_opaque, void *_private __attribute__((unused))) {
	if(opaque_reload){
		rdlog(LOG_INFO,"Reloading opaque");
		opaque_reload(new_config,cb_opaque);
	} else {
		rdlog(LOG_INFO,"Not reload opaque provided");
	}
}

static void break_http_loop(void *_h){
	struct http_private *h = _h;
	MHD_stop_daemon(h->d);
	free(h);
}

struct listener *create_http_listener(struct json_t *config,
        decoder_callback cb,void *cb_opaque) {

	json_error_t error;

	struct http_loop_args handler_args;
	memset(&handler_args,0,sizeof(handler_args));

	/* Default arguments */

	handler_args.num_threads = 1;
	handler_args.mode = MODE_SELECT;
	handler_args.server_parameters.connection_memory_limit = 128*1024;
	handler_args.server_parameters.connection_limit = 1024;
	handler_args.server_parameters.connection_timeout = 30;
	handler_args.server_parameters.per_ip_connection_limit = 0;

	/* Unpacking */

	const int unpack_rc = json_unpack_ex(config,&error,0,
		"{"
			"s:i," /* port */
			"s?s," /* mode */
			"s?i," /* num_threads */
			"s?b," /* redborder_uri */
			"s?i," /* connection_memory_limit */
			"s?i," /* connection_limit */
			"s?i," /* connection_timeout */
			"s?i"  /* per_ip_connection_limit */
		"}",
		"port",&handler_args.port,"mode",&handler_args.mode,
		"num_threads",&handler_args.num_threads,
		"redborder_uri",&handler_args.redborder_uri,
		"connection_memory_limit",
			&handler_args.server_parameters.connection_memory_limit,
		"connection_limit",
			&handler_args.server_parameters.connection_limit,
		"connection_timeout",
			&handler_args.server_parameters.connection_timeout,
		"per_ip_connection_limit",
			&handler_args.server_parameters.per_ip_connection_limit);

	if( unpack_rc != 0 /* Failure */ ) {
		rdlog(LOG_ERR,"Can't parse HTTP options: %s",error.text);
		return NULL;
	}

	struct http_private *priv = start_http_loop(&handler_args,cb,cb_opaque);
	if( NULL == priv ) {
		return NULL;
	}

	struct listener *listener = calloc(1,sizeof(*listener));
	if(!listener){
		rdlog(LOG_ERR,"Can't create http listener (out of memory?)");
		free(priv);
		return NULL;
	}

	rdlog(LOG_INFO,"Creating new HTTP listener on port %d",handler_args.port);

	listener->create       = create_http_listener;
	listener->cb.cb_opaque = cb_opaque;
	listener->cb.callback  = cb;
	listener->join         = break_http_loop;
	listener->private      = priv;
	listener->reload       = reload_listener_http;
	listener->port         = handler_args.port;

	return listener;
}

#endif
