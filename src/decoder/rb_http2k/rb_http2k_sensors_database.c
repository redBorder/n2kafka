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

#include "util/rb_json.h"
#include "rb_http2k_sensors_database.h"

#include <librd/rdlog.h>
#include <librd/rdmem.h>

struct sensors_db_s {
		/* Private data - do not access directly */
		/// database to search for uuids
		uuid_db_t uuid_db;
};

/** Assert that argument is a valid sensor */
static void sensor_db_entry_assert(const sensor_db_entry_t *sensor_entry) {
#ifdef SENSOR_DB_ENTRY_MAGIC
	assert(SENSOR_DB_ENTRY_MAGIC == sensor_entry->magic);
#endif
}

void sensor_db_entry_decref(sensor_db_entry_t *entry) {
	if (0 == ATOMIC_OP(sub,fetch,&entry->refcnt,1)) {
		if (entry->organization) {
			organizations_db_entry_decref(entry->organization);
		}

		if (entry->enrichment) {
			json_decref(entry->enrichment);
		}

		free(entry);
	}
}

static int update_sensor_with_org(sensor_db_entry_t *sensor,
					const char *organization_uuid,
					organizations_db_t *organizations_db) {
	const char *sensor_uuid = sensor_db_entry_get_uuid(sensor);

	int rc = 0;
	sensor->organization = organizations_db_get(organizations_db,
						organization_uuid);

	if (NULL == sensor->organization) {
		rdlog(LOG_ERR,
			"Couldn't find sensor %s organization %s",
			sensor_uuid, organization_uuid);

		return -1;
	}

	json_t *org_enrichment
		= organization_get_enrichment(sensor->organization);
	if (NULL != org_enrichment) {
		if (NULL == sensor->enrichment) {
			sensor->enrichment = json_deep_copy(org_enrichment);
			rc = (NULL == sensor->enrichment);
		} else {
			rc = json_object_update_missing_copy(
				sensor->enrichment, org_enrichment);
		}

		if (0 != rc) {
			rdlog(LOG_ERR,
				"Couldn't update sensor %s organization %s"
				"enrichment (out of memory?)",
				sensor_uuid, organization_uuid);
		}
	}

	return rc;
}

/** Extracts client information in a uuid_entry
  @param sensor_uuid client UUID
  @param sensor_config Client config
  @return Generated uuid entry
  */
static sensor_db_entry_t *create_sensor_db_entry(const char *sensor_uuid,
		organizations_db_t *organizations_db, json_t *sensor_config) {
	assert(sensor_uuid);
	assert(sensor_config);

	const char *organization_uuid = NULL;
	sensor_db_entry_t *entry = NULL;
	json_error_t jerr;

	rd_calloc_struct(&entry, sizeof(*entry),
		-1, sensor_uuid, &entry->uuid_entry.uuid,
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
	uuid_entry_init(&entry->uuid_entry);

	entry->refcnt = 1;
	entry->uuid_entry.data = entry;
	entry->enrichment = json_object_get(sensor_config,"enrichment");
	const int unpack_rc = json_unpack_ex(sensor_config, &jerr, JSON_STRICT,
		"{s?O,s?s}",
		"enrichment",&entry->enrichment,
		"organization_uuid", &organization_uuid);

	if (0 != unpack_rc) {
		rdlog(LOG_ERR,"Couldn't unpack client %s limits: %s",
			sensor_uuid, jerr.text);
		goto err_unpack;
	}

	if (organization_uuid) {
		const int rc = update_sensor_with_org(entry,
					organization_uuid, organizations_db);
		if (rc != 0) {
			goto err_organization_db;
		}
	}

	return entry;

err_organization_db:
	if (entry->enrichment) {
		json_decref(entry->enrichment);
	}

err_unpack:
	sensor_db_entry_decref(entry);
	entry = NULL;

err:
	return entry;
}

sensors_db_t *sensors_db_new(json_t *sensors_config,
					organizations_db_t *organizations_db) {
	const char *sensor_uuid;
	json_t *client_config;

	sensors_db_t *ret = calloc(1,sizeof(*ret));
	if (NULL == ret) {
		rdlog(LOG_ERR,
			"Couldn't create sensor uuid database (out of memory?");
		goto err;
	}

	uuid_db_init(&ret->uuid_db);

	json_object_foreach(sensors_config, sensor_uuid, client_config) {
		sensor_db_entry_t *entry = create_sensor_db_entry(
			sensor_uuid, organizations_db, client_config);
		if (NULL != entry) {
			uuid_db_insert(&ret->uuid_db, &entry->uuid_entry);
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
  @return sensor from db
  */
static sensor_db_entry_t *sensors_db_get0(sensors_db_t *db, const char *uuid) {
	uuid_entry_t *ret_entry = uuid_db_search(&db->uuid_db, uuid);
	if (ret_entry) {
		sensor_db_entry_t *ret = ret_entry->data;
		assert(ret);
		sensor_db_entry_assert(ret);
		return ret;
	}
	return NULL;
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
static void void_sensor_db_entry_decref(void *vuuid_entry) {
	uuid_entry_t *uuid_entry = vuuid_entry;
	sensor_db_entry_t *entry = uuid_entry->data;
	sensor_db_entry_assert(entry);
	sensor_db_entry_decref(entry);
}

void sensors_db_destroy(sensors_db_t *db) {
	uuid_db_foreach(&db->uuid_db,void_sensor_db_entry_decref);
	uuid_db_done(&db->uuid_db);
	free(db);
}
