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

#pragma once

#include "rb_http2k_organizations_database.h"

#include <librdkafka/rdkafka.h>
#include <stdint.h>
#include <pthread.h>
#include <semaphore.h>

/// Information shared between main thread and consumer thread
typedef struct sync_thread_s {
#ifndef NDEBUG
#define SYNC_THREAD_MAGIC 0x5c3ada1c5c3ada1c
	/// Magic to assure coherence
	uint64_t magic;
#endif
	/// Thread creation return code
	int creation_rc;
	/// Say when a thread should stop
	volatile int run;
	/** Clean interval to know how old can we accept reports. Sync interval
	    are delimited by timestamp T that match:
	    T%interval_s + offset == 0. So, if we receive a message that is not
	    in out own interval, we drop it.
	    */
	struct {
		/// Mutex to protect fields
		pthread_mutex_t mutex;
		/// Clean interval
		time_t interval_s;
		/// Offset on clean.
		time_t offset_s;
	} clean_interval;
	/** Consumer handler */
	rd_kafka_t *rk;
	/** Consumer handler base config */
	rd_kafka_conf_t *rk_conf;
	/// Thread to consume sync messages
	pthread_t thread;
	/// Organization's database we need to act over
	organizations_db_t *org_db;
	/// Semaphore to signal right status. Do not use out of
	/// sync_thread_init
	sem_t sem;
} sync_thread_t;

/** Initializes a consumer topic context
  @param rk Consumer handler
  @param thread Context to initialize
  @return 0 if success, !0 in other case
  */
int sync_thread_init(sync_thread_t *thread, rd_kafka_conf_t *rk,
						organizations_db_t *org_db);

/** De-initialize a consumer topic context
  @param thread Context to free
  */
void sync_thread_done(sync_thread_t *thread);

/** Send an async request to consumer thread to update consumer topic. If
    topic is the same as current, no signal is given */
int update_sync_topic(sync_thread_t *thread, rd_kafka_topic_t *topic);

/** Update clean_interval. Thread will not accept messages that are in the
  previous interval, i.e., that are supposed to be already cleaned.
  @param thread Thread to update
  @param interval_s Length of an interval
  @param offset_s Offset from 0 that intervals start
  */
void update_sync_thread_clean_interval(sync_thread_t *thread,
					time_t interval_s, time_t offset_s);
