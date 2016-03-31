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

#include "util/topic_database.h"

#include <pthread.h>
#include <jansson.h>

struct rb_database {
	/* UUID enrichment read-only database */
	pthread_rwlock_t rwlock;
	/* UUID enrichment refcnt */
	pthread_mutex_t uuid_enrichment_mutex;
	json_t *uuid_enrichment;
	struct topics_db *topics_db;

	void *topics_memory;
};

void init_rb_database(struct rb_database *db);
void free_valid_rb_database(struct rb_database *db);

/**
	Get client enrichment and topic of an specific database.

	@param db Database to extract client and topic handler from
	@param topic Topic to search for
	@param sensor_uuid Sensor uuid to search for
	@param topic_handler Returned topic handler. Need to be freed with
	topic_decref
	@param client_enrichment Returned client enrichment. Need to be freed
	with rb_http2k_database_client_decref
	@return 0 if OK, !=0 in other case
	*/
int rb_http2k_database_get_topic_client(struct rb_database *db,
	const char *topic, const char *sensor_uuid,
	struct topic_s **topic_handler, json_t **client_enrichment);

static void rb_http2k_database_client_decref(struct rb_database *db,
	                        json_t *client) __attribute__((unused));
static void rb_http2k_database_client_decref(struct rb_database *db,
	                        json_t *client) {
	pthread_mutex_lock(&db->uuid_enrichment_mutex);
	json_decref(client);
	pthread_mutex_unlock(&db->uuid_enrichment_mutex);
}

int rb_http2k_validate_uuid(struct rb_database *db,const char *uuid);
int rb_http2k_validate_topic(struct rb_database *db,const char *topic);
