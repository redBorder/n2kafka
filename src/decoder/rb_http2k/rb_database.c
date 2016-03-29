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
#include "topic_database.h"

#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <jansson.h>

void init_rb_database(struct rb_database *db) {
	memset(db, 0, sizeof(*db));
	pthread_rwlock_init(&db->rwlock, 0);
}

void free_valid_rb_database(struct rb_database *db) {
	if (db) {
		if (db->uuid_enrichment) {
			json_decref(db->uuid_enrichment);
		}

		if (db->topics_db) {
			topics_db_done(db->topics_db);
		}

		pthread_rwlock_destroy(&db->rwlock);
	}
}

int rb_http2k_validate_uuid(struct rb_database *db, const char *uuid) {
	pthread_rwlock_rdlock(&db->rwlock);
	const int ret = NULL != json_object_get(db->uuid_enrichment, uuid);
	pthread_rwlock_unlock(&db->rwlock);

	return ret;
}

int rb_http2k_validate_topic(struct rb_database *db, const char *topic) {
       pthread_rwlock_rdlock(&db->rwlock);
       const int ret = topics_db_topic_exists(db->topics_db,topic);
       pthread_rwlock_unlock(&db->rwlock);

       return ret;
}
