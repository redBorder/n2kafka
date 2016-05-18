/*
**
** Copyright (c) 2014, Eneo Tecnologia
** Author: Eugenio Perez <eupm90@gmail.com>
** All rights reserved.
**
** This program is free software; you can redistribute it and/or modify
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

#include "rb_http2k_curl_handler.h"

#include <util/util.h>

#include <librd/rdthread.h>
#include <librd/rdlog.h>

typedef struct rb_http2k_curl_handler_msg_s {
#ifndef NDEBUG
#define RB_HTTP2k_CURL_HANDLER_MSG 0xB112CA35B112CA35
	uint64_t magic;
#endif
	CURL *curl_handler;
} rb_http2k_curl_handler_msg_t;

static void assert_rb_http2k_curl_handler_ctx(
					rb_http2k_curl_handler_t *handler) {
#ifdef RB_HTTP2K_CURL_HANDLER_MAGIC
	assert(RB_HTTP2K_CURL_HANDLER_MAGIC == handler->magic);
#endif
}

/** Overwrite default write_callback, that prints out content by stdout */
static size_t write_callback(char *ptr, size_t size, size_t nmemb,
							void *userdata) {
	(void)ptr;
	(void)userdata;

	return size*nmemb;
}

/* Convenience function */
static CURL *my_rd_fifoq_pop_timedwait(rd_fifoq_t *fifoq, int timeout_ms) {
	rd_fifoq_elm_t *rfqe = rd_fifoq_pop_timedwait(fifoq, timeout_ms);
	CURL *ret = NULL;

	if (rfqe) {
		ret = rfqe->rfqe_ptr;
		rd_fifoq_elm_release(fifoq, rfqe);
	}

	return ret;
}

static void curl_handler_add_curl(rb_http2k_curl_handler_t *handler,
							CURL *curl_handler) {
	rdlog(LOG_DEBUG, "Adding HTTP PUT %p", curl_handler);
	curl_multi_add_handle(handler->curl_multi_handler, curl_handler);
}

/** Purge completed transfers
  @param handler Handler to purge
  @return Number of transfers cleaned
  */
static int curl_handler_purge_completed(rb_http2k_curl_handler_t *handler) {
	CURLMsg *msg;
	int cleaned = 0;
	int msgs_in_queue;
	while((msg = curl_multi_info_read(handler->curl_multi_handler,
							&msgs_in_queue))) {
		if(msg->msg == CURLMSG_DONE) {
			CURL *e = msg->easy_handle;
			rdlog(LOG_DEBUG, "HTTP PUT %p end", msg);
			curl_multi_remove_handle(handler->curl_multi_handler,
									e);
			curl_easy_cleanup(e);
			cleaned++;
		} else {
			rdlog(LOG_CRIT, "Unknown message returned!!");
		}
	}

	return cleaned;
}

static void curl_handler_entry_point0(rb_http2k_curl_handler_t *handler) {
	static int fifoq_pop_timeout_ms = 500;
	int my_still_running = 0;
	while(handler->run) {
		int still_running = 0;
		CURL *elm = my_rd_fifoq_pop_timedwait(&handler->msg_queue,
							fifoq_pop_timeout_ms);
		if (elm) {
			my_still_running++;
			curl_handler_add_curl(handler, elm);
		}

		curl_multi_perform(handler->curl_multi_handler,
							&still_running);

		if (still_running < my_still_running) {
			/* Some handlers require attention */
			const int cleaned = curl_handler_purge_completed(
								handler);
			my_still_running -= cleaned;
		}
	}

}

static void *curl_handler_entry_point(void *ctx) {
	rb_http2k_curl_handler_t *handler = ctx;
	assert_rb_http2k_curl_handler_ctx(handler);
	curl_handler_entry_point0(handler);
	return NULL;
}

int rb_http2k_curl_handler_init(rb_http2k_curl_handler_t *handler,
							int max_msgs_size) {
	char err[BUFSIZ];
#ifdef RB_HTTP2K_CURL_HANDLER_MAGIC
	handler->magic = RB_HTTP2K_CURL_HANDLER_MAGIC;
#endif
	handler->run = 1;

	rd_fifoq_init(&handler->msg_queue);
	rd_fifoq_set_max_size(&handler->msg_queue, max_msgs_size, 0);
	curl_global_init(CURL_GLOBAL_DEFAULT);
	handler->curl_multi_handler = curl_multi_init();
	if (NULL == handler->curl_multi_handler) {
		rdlog(LOG_ERR, "Couldn create curl_multi_handler.");
		return -1;
	}

	const int pthread_create_rc = pthread_create(&handler->thread, NULL,
		curl_handler_entry_point, handler);

	if (pthread_create_rc != 0) {
		rdlog(LOG_ERR, "Couldn't create thread: %s",
			mystrerror(errno, err, sizeof(err)));
		curl_multi_cleanup(handler->curl_multi_handler);
	}

	return pthread_create_rc;
}

void rb_http2k_curl_handler_done(rb_http2k_curl_handler_t *handler) {
	handler->run = 0;
	pthread_join(handler->thread, NULL);
	curl_multi_cleanup(handler->curl_multi_handler);
	curl_global_cleanup();
	rd_fifoq_destroy(&handler->msg_queue);
}

static void curl_easy_set_http_put_method(CURL *curl_handler,
							const char *url) {
	curl_easy_setopt(curl_handler, CURLOPT_URL, url);
	curl_easy_setopt(curl_handler, CURLOPT_UPLOAD, 1);
}

/// @TODO test full queue
void rb_http2k_curl_handler_put_empty(rb_http2k_curl_handler_t *handler,
							const char *url) {
	CURL *curl_handler = curl_easy_init(), *purged = NULL;
	curl_easy_set_http_put_method(curl_handler, url);
	curl_easy_setopt(curl_handler, CURLOPT_WRITEFUNCTION, write_callback);

	// Skip SSL verification
	curl_easy_setopt(curl_handler, CURLOPT_SSL_VERIFYPEER, 0L);

	rdlog(LOG_DEBUG, "Adding curl handler %p to URL %s", curl_handler,
									url);

	rd_fifoq_add_purge(&handler->msg_queue,curl_handler,&purged);
	if (purged) {
		char *purged_url = NULL;
		curl_easy_getinfo(purged, CURLINFO_EFFECTIVE_URL, &purged_url);
		rdlog(LOG_ERR, "Too much request queued. Freeing %s",
			purged_url ? purged_url : "older");
		curl_easy_cleanup(purged);
	}
}
