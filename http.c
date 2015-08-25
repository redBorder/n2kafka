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

#include "global_config.h"

#include <assert.h>
#include <jansson.h>
#include <librd/rdlog.h>
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
    listener_callback callback;
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
	struct string str;
};

static void free_con_info(struct conn_info *con_info) {
	free(con_info->str.buf);
	con_info->str.buf = NULL;
	free(con_info);
}

static void request_completed (void *cls,
                               struct MHD_Connection *connection HTTP_UNUSED,
                               void **con_cls,
                               enum MHD_RequestTerminationCode toe HTTP_UNUSED)
{
	if( NULL == con_cls || NULL == *con_cls) {
		return; /* This point should never reached? */
	}

	struct conn_info *con_info = *con_cls;
	struct http_private *h = cls;
#ifdef HTTP_PRIVATE_MAGIC
	assert(HTTP_PRIVATE_MAGIC == h->magic);
#endif

	h->callback(con_info->str.buf,con_info->str.used,NULL,h->callback_opaque);
	con_info->str.buf = NULL; /* librdkafka will free it */
	
	free_con_info(con_info);
	*con_cls = NULL;
}

static struct conn_info *create_connection_info(size_t string_size) {
	/* First call, creating all needed structs */
	struct conn_info *con_info = calloc(1,sizeof(*con_info));
	if( NULL == con_info )
		return NULL; /* Doesn't have resources */

	if ( !init_string(&con_info->str,string_size) ) {
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

static void extract_rb_url_info(const char *url,size_t url_len,char *dst,
								const char **uuid, const char **topic) {
	assert(url);
	assert(dst);
	assert(uuid);
	assert(topic);

	char *aux=NULL;
	memcpy(dst,url,url_len+1);

	*uuid = strtok_r(dst,"/",&aux);
	*topic = strtok_r(NULL,"/",&aux);
}

static int rb_http2k_validation(struct MHD_Connection *con_info,const char *url,
							struct rb_database *rb_database, int *allok) {
	const char *uuid=NULL,*topic=NULL;
	const size_t url_len = strlen(url);
	char my_url[url_len+1];
	extract_rb_url_info(url,url_len,my_url,&uuid,&topic);

	rdlog(LOG_DEBUG,"Receiving message with uuid '%s' and topic '%s'",uuid,topic);

	/// @TODO check uuid url/message equality
	const int valid_uuid = rb_http2k_validate_uuid(rb_database,uuid);
	if(!valid_uuid) {
		rdlog(LOG_WARNING,"Received invalid uuid %s. Closing connection.",uuid);
		*allok = 0;
		return send_http_unauthorized(con_info);
	}

	const int valid_topic = rb_http2k_validate_topic(&global_config.rb.database,topic);
	if(!valid_topic) {
		rdlog(LOG_WARNING,"Received topic %s. Closing connection.",uuid);
		*allok = 0;
		return send_http_forbidden(con_info);
	}

	*allok = 1;

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
		if (cls->redborder_uri) {
			int aok = 1;
			const int rc = rb_http2k_validation(connection,url,
				&global_config.rb.database,&aok);
			if(0 == aok) {
				return rc;
			}
		}
		*ptr = create_connection_info(STRING_INITIAL_SIZE);
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
	unsigned int num_threads;
	int redborder_uri;
};

static struct http_private *start_http_loop(const struct http_loop_args *args,
                                            char *err,
                                            size_t errsize,
                                            listener_callback callback,void *cb_opaque) {
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
		snprintf(err,errsize,"Not a valid HTTP mode. Select one between("
		    MODE_THREAD_PER_CONNECTION "," MODE_SELECT "," MODE_POLL "," 
		    MODE_EPOLL ")");
		return NULL;
	}

	flags |= MHD_USE_DEBUG;

	h = calloc(1,sizeof(*h));
	if(!h) {
		snprintf(err,errsize,"Can't allocate LIBMICROHTTPD private"
		         " (out of memory?)");
		return NULL;
	}
#ifdef HTTP_PRIVATE_MAGIC
	h->magic = HTTP_PRIVATE_MAGIC;
#endif
	h->callback = callback;
	h->callback_opaque = cb_opaque;
	h->redborder_uri = args->redborder_uri;

	if(0 == strcmp(args->mode,MODE_THREAD_PER_CONNECTION)) {
		h->d = MHD_start_daemon(flags,
			args->port,
			NULL, /* Auth callback */
			NULL, /* Auth callback parameter */
			post_handle, /* Request handler */
			h, /* Request handler parameter */
			MHD_OPTION_NOTIFY_COMPLETED, &request_completed, h,
			/* Memory limit per connection */
			MHD_OPTION_CONNECTION_MEMORY_LIMIT, (size_t)(128*1024),
			/* Memory increment at read buffer growing */
			MHD_OPTION_CONNECTION_MEMORY_INCREMENT, (size_t)(4*1024),
			MHD_OPTION_END);
	} else {
		h->d = MHD_start_daemon(flags,
			args->port,
			NULL, /* Auth callback */
			NULL, /* Auth callback parameter */
			post_handle, /* Request handler */
			h, /* Request handler parameter */
			MHD_OPTION_NOTIFY_COMPLETED, &request_completed, h,
			MHD_OPTION_THREAD_POOL_SIZE, args->num_threads,
			MHD_OPTION_END);
	}

	if(NULL == h->d) {
		snprintf(err,errsize,"Can't allocate LIBMICROHTTPD handler"
		         " (out of memory?)");
		free(h);
		return NULL;
	}

	return h;
}

static void reload_listener_http(json_t *new_config,listener_opaque_reload opaque_reload,
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

struct listener *create_http_listener(struct json_t *config,listener_callback cb,void *cb_opaque,char *err,
	size_t errsize) {

	json_error_t error;

	struct http_loop_args handler_args;
	memset(&handler_args,0,sizeof(handler_args));
	handler_args.num_threads = 1;

	const int unpack_rc = json_unpack_ex(config,&error,0,"{s:i,s?s,s?i,s?b}",
		"port",&handler_args.port,"mode",&handler_args.mode,
		"num_threads",&handler_args.num_threads,
		"redborder_uri",&handler_args.redborder_uri);
	if( unpack_rc != 0 /* Failure */ ) {
		snprintf(err,errsize,"Can't parse HTTP options: %s",error.text);
		return NULL;
	}

	if(NULL==handler_args.mode)
		handler_args.mode = MODE_SELECT;

	struct http_private *priv = start_http_loop(&handler_args,err,errsize,cb,cb_opaque);
	if( NULL == priv ) {
		return NULL;
	}

	struct listener *listener = calloc(1,sizeof(*listener));
	if(!listener){
		snprintf(err,errsize,"Can't create http listener (out of memory?)");
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
