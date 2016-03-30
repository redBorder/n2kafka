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

#include "config.h"

#include "rb_http2k_sensors_database.h"

#include <librd/rdlog.h>
#include <librd/rdmem.h>

/*
 * PURE HASHTABLE
 */

/// Seed to feed hashtable hash
/// @TODO make random!
static const uint64_t hashtable_seed = 0;

/** Assert that argument is a valid sensor */
static void sensor_db_entry_assert(const sensor_db_entry_t *sensor_entry) {
#ifdef SENSOR_DB_ENTRY_MAGIC
	assert(SENSOR_DB_ENTRY_MAGIC == sensor_entry->magic);
#endif
}

/** Hash to apply to string
  @param key Key to hash
  @return hash
  */
#define hash_str(key) tommy_hash_u32(hashtable_seed, key, strlen(key))

/** Initialice a clients uuid hashtable
  @param h hashtable.
  @param num size of hashtable
  */
#define sensors_hashtable_init(h,num) tommy_hashtable_init(h,num)

/** Inserts an element in uuid hashtable
  @param h hashtable
  @param n uuid_entry to insert
  */
#define sensors_hashtable_insert(h,n) \
	tommy_hashtable_insert(h, &((n)->node), n, hash_str((n)->sensor_uuid))

/** Calls a function foreach element in hashtable */
#define sensors_hashtable_foreach tommy_hashtable_foreach

#define sensors_hashtable_done tommy_hashtable_done

/** Check if an uuid_entry has an uuid
  @param arg uuid in const char * format
  @param obj uuid_entry
  @return 0 if equal, 1 ioc
  */
static int sensor_cmp(const void* arg, const void* obj) {
	const char *uuid = arg;
	const sensor_db_entry_t *sensor = obj;
	sensor_db_entry_assert(sensor);

	return strcmp(uuid,sensor->sensor_uuid);
}

/** Search an element in the hashtable
  @param db database
  @param uuid uuid to search
  @return element if found, NULL in other case
  */
static sensor_db_entry_t *sensors_hashtable_search(sensors_db_t *db,
							const char *uuid) {
	sensor_db_entry_t *ret = tommy_hashtable_search(&db->hashtable,
		sensor_cmp, uuid, hash_str(uuid));
	if (ret) {
		sensor_db_entry_assert(ret);
	}
	return ret;
}

/*
 * DATABASE
 */

void sensor_db_entry_decref(sensor_db_entry_t *entry) {
	if (0 == ATOMIC_OP(sub,fetch,&entry->refcnt,1)) {
		if (entry->enrichment) {
			json_decref(entry->enrichment);
		}

		free(entry);
	}
}

/** Extracts client information in a uuid_entry
  @param sensor_uuid client UUID
  @param sensor_config Client config
  @return Generated uuid entry
  */
static sensor_db_entry_t *create_sensor_db_entry(const char *sensor_uuid,
		json_t *sensor_config) {
	assert(sensor_uuid);
	assert(sensor_config);

	sensor_db_entry_t *entry = NULL;
	json_error_t jerr;

	rd_calloc_struct(&entry, sizeof(*entry),
		-1, sensor_uuid, &entry->sensor_uuid,
		RD_MEM_END_TOKEN);

	if (NULL == entry) {
		rdlog(LOG_ERR,
			"Couldn't create uuid %s entry (out of memory?).",
			sensor_uuid);
		goto err;
	}

#ifdef SENSOR_DB_ENTRY_MAGIC
	entry->magic = SENSOR_DB_ENTRY_MAGIC;
#endif

	entry->refcnt = 1;
	const int unpack_rc = json_unpack_ex(sensor_config, &jerr, JSON_STRICT,
		"{s?O}",
		"enrichment",&entry->enrichment);

	if (0 != unpack_rc) {
		rdlog(LOG_ERR,"Couldn't unpack client %s limits: %s",
			sensor_uuid, jerr.text);
		goto err_unpack;
	}

	return entry;

err_unpack:
	sensor_db_entry_decref(entry);
	entry = NULL;

err:
	return entry;
}

sensors_db_t *sensors_db_new(json_t *sensors_config) {
	const char *sensor_uuid;
	json_t *client_config;
	const size_t n_sensors = json_object_size(sensors_config);

	sensors_db_t *ret = calloc(1,sizeof(*ret));
	if (NULL == ret) {
		rdlog(LOG_ERR, "Couldn't create uuid database (out of memory?");
		goto err;
	}

	sensors_hashtable_init(&ret->hashtable, n_sensors*2);

	json_object_foreach(sensors_config, sensor_uuid, client_config) {
		sensor_db_entry_t *entry = create_sensor_db_entry(
			sensor_uuid, client_config);
		if (NULL != entry) {
			sensors_hashtable_insert(&ret->hashtable,entry);
		} else {
			/* create_sensor_db_entry() has already log */
			goto err_entry;
		}

	}

	return ret;

err_entry:
	sensors_db_destroy(ret);
err:
	return NULL;
}

/** Obtains an entry from database, but does not increments reference counting
  @param db Database
  @param uuid UUID
  @return uuid_entry
  */
static sensor_db_entry_t *sensors_db_get0(sensors_db_t *db, const char *uuid) {
	return sensors_hashtable_search(db,uuid);
}

sensor_db_entry_t *sensors_db_get(sensors_db_t *db, const char *uuid) {
	sensor_db_entry_t *ret = sensors_db_get0(db,uuid);
	if (ret) {
		ATOMIC_OP(add,fetch,&ret->refcnt,1);
	}
	return ret;
}

int sensors_db_exists(sensors_db_t *db, const char *uuid) {
	return NULL != sensors_db_get0(db,uuid);
}

/** Frees an element that is suppose to be an uuid_entry
  @param uuid_entry UUID entry
  */
static void void_sensor_db_entry_decref(void *ventry) {
	sensor_db_entry_t *entry = ventry;
	sensor_db_entry_assert(entry);
	sensor_db_entry_decref(entry);
}

void sensors_db_destroy(sensors_db_t *db) {
	sensors_hashtable_foreach(&db->hashtable,void_sensor_db_entry_decref);
	sensors_hashtable_done(&db->hashtable);
	free(db);
}
