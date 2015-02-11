/*
** Copyright (C) 2014 Eneo Tecnologia S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU General Public License Version 2 as
** published by the Free Software Foundation.  You may not use, modify or
** distribute this program under any other version of the GNU General
** Public License.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program; if not, write to the Free Software
** Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*/

#include "config.h"

#ifdef HAVE_LIBMICROHTTPD

#define HTTP_UNUSED __attribute__((unused))
#define POSTBUFFERSIZE (10*1024)

#include "http.h"

#include "global_config.h"

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

struct http_private{
	struct MHD_Daemon *d;
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
	char *new_buf = realloc(&str->buf,newsize);
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

static void request_completed (void *cls HTTP_UNUSED, 
                               struct MHD_Connection *connection HTTP_UNUSED,
                               void **con_cls,
                               enum MHD_RequestTerminationCode toe HTTP_UNUSED)
{
	if( NULL == con_cls ) {
		return; /* This point should never reached? */
	}

	struct conn_info *con_info = *con_cls;

	if ( NULL == con_info ) 
		return;
	
	if( con_info->str.buf ) {
		send_to_kafka(con_info->str.buf,con_info->str.used,RD_KAFKA_MSG_F_FREE);
		con_info->str.buf = NULL; /* librdkafka will free it */
	}
	
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

static int send_http_ok(struct MHD_Connection *connection) {
	struct MHD_Response *http_response = MHD_create_response_from_buffer(
		0,NULL,MHD_RESPMEM_PERSISTENT);

	if(NULL == http_response) {
		rdlog(LOG_CRIT,"Can't create HTTP response");
	}

	const int ret = MHD_queue_response(connection,MHD_HTTP_OK,http_response);
	MHD_destroy_response(http_response);
	return ret;
}

static size_t append_http_data_to_connection_data(struct conn_info *con_info,
												  const char *upload_data,
												  size_t upload_data_size) {

	if( upload_data_size > string_free_space(&con_info->str) ) {
		/* TODO error handling */
		string_grow(&con_info->str,upload_data_size);
	}

	size_t ncopy = smin(upload_data_size,string_free_space(&con_info->str));
	strncpy(con_info->str.buf,upload_data,ncopy);
	con_info->str.used += ncopy;
	return ncopy;
}

static int post_handle(void *cls HTTP_UNUSED,
						 struct MHD_Connection *connection,
						 const char *url HTTP_UNUSED,
						 const char *method,
						 const char *version HTTP_UNUSED,
						 const char *upload_data,
						 size_t *upload_data_size,
						 void **ptr) {

	if (0 != strcmp(method, MHD_HTTP_METHOD_POST)) {
		return MHD_NO; /* unexpected method */
	}

	if ( NULL == ptr) {
		return MHD_NO;
	}

	if ( NULL == *ptr ) {
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

static struct http_private *start_http_loop(int port,char *err,
                                            size_t errsize) {
	struct http_private *h = calloc(1,sizeof(*h));

	if(!h) {
		snprintf(err,errsize,"Can't create http listener private data"
		                     " (out of memory?)");
		return NULL;
	}

	h->d = MHD_start_daemon(MHD_USE_THREAD_PER_CONNECTION,
		port,
		NULL, /* Auth callback */
		NULL, /* Auth callback parameter */
		post_handle, /* Request handler */
		NULL, /* Request handler parameter */
		MHD_OPTION_NOTIFY_COMPLETED, &request_completed, NULL,
		MHD_OPTION_END);

	if(NULL == h->d) {
		snprintf(err,errsize,"Can't allocate LIBMICROHTTPD handler"
		         " (out of memory?)");
		free(h);
		return NULL;
	}

	return h;
}

static void break_http_loop(void *_h){
	struct http_private *h = _h;
	MHD_stop_daemon(h->d);
}

struct listener *create_http_listener(struct json_t *config,char *err,
	size_t errsize) {

	json_error_t error;

	int port = 0;
	const int unpack_rc = json_unpack_ex(config,&error,0,"{s:i}","port",
		&port);
	if( unpack_rc != 0 /* Failure */ ) {
		snprintf(err,errsize,"Can't find server port: %s",error.text);
	}

	struct http_private *priv = start_http_loop(port,err,errsize);
	if( NULL == priv ) {
		return NULL;	
	}

	struct listener *listener = calloc(1,sizeof(*listener));
	if(!listener){
		snprintf(err,errsize,"Can't create http listener (out of memory?)");
		free(priv);
		return NULL;
	}

	listener->create  = create_http_listener;
	listener->join    = break_http_loop;
	listener->private = priv;
	return listener;


}


#endif