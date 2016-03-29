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

#include <pthread.h>

struct rb_database {
	/* UUID enrichment read-only database */
	pthread_rwlock_t rwlock;
	/* UUID enrichment refcnt */
	pthread_mutex_t uuid_enrichment_mutex;
	struct json_t *uuid_enrichment;
	/// @TODO this should be another kind to save "unknown values"
	struct json_t *dangerous_values;
	struct topics_db *topics_db;

	void *topics_memory;
};

void init_rb_database(struct rb_database *db);
void free_valid_rb_database(struct rb_database *db);

int rb_http2k_validate_uuid(struct rb_database *db,const char *uuid);
int rb_http2k_validate_topic(struct rb_database *db,const char *topic);
