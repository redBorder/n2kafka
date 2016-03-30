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

#include "rb_database.h"
#include "util/topic_database.h"

#include <librd/rdlog.h>

#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <jansson.h>
#include <errno.h>

int init_rb_database(struct rb_database *db) {
	char errbuf[BUFSIZ];

	memset(db, 0, sizeof(*db));
	const int rc = pthread_rwlock_init(&db->rwlock, 0);

	if (rc != 0) {
		strerror_r(errno, errbuf, sizeof(errbuf));
		rdlog(LOG_ERR, "Can't start rwlock: %s", errbuf);
	}

	return rc;
}

void free_valid_rb_database(struct rb_database *db) {
	if (db) {
		if (db->sensors_db) {
			sensors_db_destroy(db->sensors_db);
		}

		if (db->topics_db) {
			topics_db_done(db->topics_db);
		}

		pthread_rwlock_destroy(&db->rwlock);
	}
}

int rb_http2k_database_get_topic_client(struct rb_database *db,
		const char *topic, const char *sensor_uuid,
		struct topic_s **topic_handler,
		sensor_db_entry_t **client_enrichment) {
	assert(db);
	assert(topic_handler);
	assert(client_enrichment);

	pthread_rwlock_rdlock(&db->rwlock);
	*topic_handler = topics_db_get_topic(db->topics_db, topic);

	if(*topic_handler) {
		*client_enrichment = sensors_db_get(db->sensors_db, sensor_uuid);
	}
	pthread_rwlock_unlock(&db->rwlock);

	return NULL != *topic_handler && NULL != *client_enrichment;
}

int rb_http2k_validate_uuid(struct rb_database *db, const char *sensor_uuid) {
	pthread_rwlock_rdlock(&db->rwlock);
	const int ret = sensors_db_exists(db->sensors_db, sensor_uuid);
	pthread_rwlock_unlock(&db->rwlock);

	return ret;
}

int rb_http2k_validate_topic(struct rb_database *db, const char *topic) {
       pthread_rwlock_rdlock(&db->rwlock);
       const int ret = topics_db_topic_exists(db->topics_db,topic);
       pthread_rwlock_unlock(&db->rwlock);

       return ret;
}
