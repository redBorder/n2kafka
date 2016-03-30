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

#include <jansson.h>
#include "tommyds/tommyhashtbl.h"

#define sensors_hashtable tommy_hashtable
#define sensors_hashtable_node tommy_hashtable_node

/// Sensors database entry
typedef struct sensor_db_entry {

#ifndef NDEBUG
	/* Private data - do not access directly */

/// Magic to assert coherency.
#define SENSOR_DB_ENTRY_MAGIC 0x05350DB305350DB3L
	/// Magic to assert coherency.
	uint64_t magic;
#endif

	/// Client uuid
	const char *sensor_uuid;

	/// Enrichment data
	json_t *enrichment;

	/// hashtable node
	sensors_hashtable_node node;

	/// Reference counter
	uint64_t refcnt;
} sensor_db_entry_t;

/** Obtains sensor uuid */
#define sensor_db_entry_get_uuid(e) ((e)->sensor_uuid)

/** Obtains sensor_db_entry enrichment information */
#define sensor_db_entry_json_enrichment(e) ((e)->enrichment)

/** Decrements uuid entry, signaling that we are not going to use it anymore */
void sensor_db_entry_decref(sensor_db_entry_t *entry);

/** Sensors uuid database */
typedef struct sensors_db_s {
	/* Private data - do not access directly */
	/// hashtable to search for uuids
	sensors_hashtable hashtable;
} sensors_db_t;

/** Creates a new database
  @param uuids_config Configurations for each sensor
  @returns new database
  */
sensors_db_t *sensors_db_new(json_t *sensors_config);

/** Get an entry from sensor database.
  @note Obtained entry need to be freed with sensor_db_entry_decref
  @param db database
  @param uuid sensor uuid
  @returns sensor entry
  */
sensor_db_entry_t *sensors_db_get(sensors_db_t *db, const char *sensor_uuid);

/** Checks if an entry exists in uuid database.
  @param db database
  @param uuid sensor uuid
  @returns 1 if sensor found, 0 ioc
  */
int sensors_db_exists(sensors_db_t *db, const char *sensor_uuid);

/** Destroy a sensor database
  @param db Database to destroy
  */
void sensors_db_destroy(sensors_db_t *db);
