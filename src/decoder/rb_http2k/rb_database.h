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
#include "rb_http2k_sensors_database.h"
#include "rb_http2k_organizations_database.h"

#include <pthread.h>
#include <jansson.h>

struct rb_database {
	/* UUID enrichment read-only database */
	pthread_rwlock_t rwlock;
	/// sensors UUID database.
	sensors_db_t *sensors_db;
	/// Organizations database
	organizations_db_t organizations_db;
	struct topics_db *topics_db;

	void *topics_memory;
};

/** Initialized a rb_database
  @param db database to init
  @return 0 if success, !0 in other case
  */
int init_rb_database(struct rb_database *db);
void free_valid_rb_database(struct rb_database *db);

/**
	Get sensor enrichment and topic of an specific database.

	@param db Database to extract sensor and topic handler from
	@param topic Topic to search for
	@param sensor_uuid Sensor uuid to search for
	@param topic_handler Returned topic handler. Need to be freed with
	topic_decref
	@param sensor_info Returned sensor information. Need to be freed
	with sensor_db_entry_decref
	@return 0 if OK, !=0 in other case
	*/
int rb_http2k_database_get_topic_client(struct rb_database *db,
	const char *topic, const char *sensor_uuid,
	struct topic_s **topic_handler, sensor_db_entry_t **sensor_info);

int rb_http2k_validate_uuid(struct rb_database *db,const char *uuid);
int rb_http2k_validate_topic(struct rb_database *db,const char *topic);
