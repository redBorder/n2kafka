/*
**
** Copyright (c) 2014, Eneo Tecnologia
** Author: Eugenio Perez <eupm90@gmail.com>
** All rights reserved.
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

#pragma once

#include "rb_database.h"
#include "rb_http2k_sync_thread.h"
#include "rb_http2k_curl_handler.h"

#include "util/pair.h"

#include <librdkafka/rdkafka.h>
#include <librd/rdavl.h>
#include <librd/rdsysqueue.h>
#include <jansson.h>

#include <stdint.h>
#include <string.h>
#include <pthread.h>

#ifndef NDEBUG
/// MAGIC to check rb_config between void * conversions
#define RB_CONFIG_MAGIC 0xbc01a1cbc01a1cL
#endif

/* All functions are thread-safe here, excepting free_valid_mse_database */
struct json_t;
struct rb_config {
#ifdef RB_CONFIG_MAGIC
	/// This value always have to be RB_CONFIG_MAGIC
	uint64_t magic;
#endif

	/// @TODO protect all char * members!
	struct {
		/// Sync id to difference between others n2kafka and this one
		char *n2kafka_id;
		/// Timer to send organization stats to monitor
		rb_timer_t *timer;
		/// Timer to clean stats
		rb_timer_t *clean_timer;
		/// Topics to send organization stats.
		struct rkt_array topics;
		/// Consumer ctx
		sync_thread_t thread;
		/// http related
		struct {
			/// CURL handler to send PUT when done
			rb_http2k_curl_handler_t curl_handler;
			/// Url to send PUT request.
			char *url;
		} http;
	} organizations_sync;
	struct rb_database database;
};

#ifdef RB_CONFIG_MAGIC
/// Checks that rb_config magic field has the right value
#define assert_rb_config(cfg) do{ \
	assert(RB_CONFIG_MAGIC==(cfg)->magic);} while(0)
#else
#define assert_rb_config(cfg)
#endif

int parse_rb_config(void *_db,const struct json_t *rb_config);
/** Release all resources used */
void rb_decoder_done(void *rb_config);
/** Does nothing, since this decoder does not save anything related to
    listener
    */
int rb_decoder_reload(void *_db, const struct json_t *rb_config);

int rb_opaque_creator(struct json_t *config,void **opaque);
int rb_opaque_reload(struct json_t *config,void *opaque);
void rb_opaque_done(void *opaque);
void rb_decode(char *buffer,size_t buf_size,const keyval_list_t *props,
                void *listener_callback_opaque,void **decoder_sessionp);
