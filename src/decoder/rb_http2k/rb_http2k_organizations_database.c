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

#include "rb_http2k_organizations_database.h"
#include "util/util.h"

#include <librd/rdlog.h>
#include <librd/rdmem.h>

/** Assert that argument is a valid organization */
static void organizations_db_entry_assert(
			const organization_db_entry_t *organization_entry) {
#ifdef ORGANIZATION_DB_ENTRY_MAGIC
	assert(ORGANIZATION_DB_ENTRY_MAGIC == organization_entry->magic);
#endif
}

void organizations_db_entry_decref(organization_db_entry_t *entry) {
	if (0 == ATOMIC_OP(sub,fetch,&entry->refcnt,1)) {
		if (entry->enrichment) {
			json_decref(entry->enrichment);
		}

		free(entry);
	}
}

/** Update database entry with new information (if needed)
  @param entry Entry to be updated
  @param new_config New config
  @return 0 if success, !0 in other case (reason printed)
  */
static int update_organization(organization_db_entry_t *entry,
				json_t *new_config) {
	int rc = 0;
	json_error_t jerr;
	json_int_t bytes_limit = 0;
	json_t *aux_enrichment = NULL;

	const int unpack_rc = json_unpack_ex(new_config, &jerr,
		JSON_STRICT,
		"{s?O,s?{s?I}}",
		"enrichment",&aux_enrichment,
		"limits","bytes",&bytes_limit);

	if (0 != unpack_rc) {
		const char *organization_uuid =
			organization_db_entry_get_uuid(entry);
		rdlog(LOG_ERR,"Couldn't unpack organization %s limits: %s",
						organization_uuid, jerr.text);
		rc = -1;
		goto unpack_err;
	}

	swap_ptrs(entry->enrichment, aux_enrichment);
	entry->bytes_limit.max = (uint64_t)bytes_limit;

unpack_err:
	if (aux_enrichment) {
		json_decref(aux_enrichment);
	}

	return rc;
}

/** Extracts client information in a uuid_entry
  @param sensor_uuid client UUID
  @param sensor_config Client config
  @return Generated uuid entry
  */
static organization_db_entry_t *create_organization_db_entry(
		const char *organization_uuid, json_t *organization_config) {
	assert(organization_uuid);
	assert(organization_config);

	organization_db_entry_t *entry = NULL;

	rd_calloc_struct(&entry, sizeof(*entry),
		-1, organization_uuid, &entry->uuid_entry.uuid,
		RD_MEM_END_TOKEN);

	if (NULL == entry) {
		rdlog(LOG_ERR,
			"Couldn't create uuid %s entry (out of memory?).",
			organization_uuid);
		goto err;
	}

#ifdef ORGANIZATION_DB_ENTRY_MAGIC
	entry->magic = ORGANIZATION_DB_ENTRY_MAGIC;
#endif
	uuid_entry_init(&entry->uuid_entry);

	entry->refcnt = 1;
	entry->uuid_entry.data = entry;

	const int rc = update_organization(entry,organization_config);
	if (rc != 0) {
		goto err_update;
	}

	return entry;

err_update:
	organizations_db_entry_decref(entry);
	entry = NULL;

err:
	return entry;
}

int organizations_db_init(organizations_db_t *organizations_db) {
	memset(organizations_db, 0, sizeof(*organizations_db));
	uuid_db_init(&organizations_db->uuid_db);
	return 0;
}

/** Obtains an entry from database, but does not increments reference counting
  @param db Database
  @param uuid UUID
  @return sensor from db
  */
static organization_db_entry_t *organizations_db_get0(organizations_db_t *db,
							const char *uuid) {
	uuid_entry_t *ret_entry = uuid_db_search(&db->uuid_db, uuid);
	if (ret_entry) {
		organization_db_entry_t *ret = ret_entry->data;
		assert(ret);
		organizations_db_entry_assert(ret);
		return ret;
	}
	return NULL;
}

/** Purge an organization that does not exists in the new configuration.
  @param new_orgs New organizations.
  @param entry Entry to check
  */
static void check_and_purge_organization(const json_t *new_orgs,
					organization_db_entry_t *entry) {
	const char *uuid = organization_db_entry_get_uuid(entry);
	if (NULL == json_object_get(new_orgs,uuid)) {
		/* This entry is not in the new db, we need to delete it */
		uuid_db_remove(&entry->db->uuid_db, &entry->uuid_entry);
		organizations_db_entry_decref(entry);
	}
}

/** Convenience function.
  @see check_and_purge_organization
  */
static void void_check_and_purge_organization(void *vnew_orgs, void *ventry) {
	const json_t *new_orgs = vnew_orgs;
	uuid_entry_t *uuid_entry = ventry;
	organization_db_entry_t *org = uuid_entry->data;
	organizations_db_entry_assert(org);
	check_and_purge_organization(new_orgs, org);
}

/** Clean all functions that are not in the new configuration
  @param db Database
  @param organizations New organizations config
  */
static void purge_old_organizations(organizations_db_t *db,
						json_t *organizations) {
	tommy_hashdyn_foreach_arg(&db->uuid_db,
		void_check_and_purge_organization, organizations);
}

void organizations_db_reload(organizations_db_t *db, json_t *organizations) {
	const char *organization_uuid;
	json_t *organization;

	/* 1st step: Delete all organziations that are not in the new config */
	purge_old_organizations(db,organizations);

	/* 2nd step: Update/create new ones */
	json_object_foreach(organizations, organization_uuid, organization) {
		organization_db_entry_t *entry = organizations_db_get0(db,
							organization_uuid);
		if(NULL == entry) {
			entry = create_organization_db_entry(organization_uuid,
								organization);
			if (NULL != entry) {
				entry->db = db;
				uuid_db_insert(&db->uuid_db,
							&entry->uuid_entry);
			}
		} else {
			update_organization(entry,organization);
		}

	}
}

organization_db_entry_t *organizations_db_get(organizations_db_t *db, const char *uuid) {
	organization_db_entry_t *ret = organizations_db_get0(db,uuid);
	if (ret) {
		ATOMIC_OP(add,fetch,&ret->refcnt,1);
	}
	return ret;
}

int organizations_db_exists(organizations_db_t *db, const char *uuid) {
	return NULL != organizations_db_get0(db,uuid);
}

/** Frees an element that is suppose to be an uuid_entry
  @param uuid_entry UUID entry
  */
static void void_organizations_db_entry_decref(void *vuuid_entry) {
	uuid_entry_t *uuid_entry = vuuid_entry;
	organization_db_entry_t *entry = uuid_entry->data;
	organizations_db_entry_assert(entry);
	organizations_db_entry_decref(entry);
}

void organizations_db_done(organizations_db_t *db) {
	uuid_db_foreach(&db->uuid_db,void_organizations_db_entry_decref);
	uuid_db_done(&db->uuid_db);
}
