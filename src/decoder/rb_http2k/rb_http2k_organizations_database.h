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

#include "config.h"

#include <jansson.h>
#include "uuid_database.h"
#include "util/kafka.h"

#include <time.h>

/* FW declaration */
struct organizations_db_s;

/// organization resource limit
struct organization_limit {
	/// Max allowed
	volatile uint64_t max;
	/// Consumed at this point
	volatile uint64_t consumed;
	/// Last reported bytes
	uint64_t reported;
        /// Boolean that say if we have sent the waning about limit reached
        int warning_given;
};

/// Organization database entry
typedef struct organization_db_entry_s {

#ifndef NDEBUG
	/* Private data - do not access directly */

/// Magic to assert coherency.
#define ORGANIZATION_DB_ENTRY_MAGIC 0x0A1A10DB0A1A10DBL
	/// Magic to assert coherency.
	uint64_t magic;
#endif

	/// Organization enrichment
	json_t *enrichment;

	/// Organization byte limit
	struct organization_limit bytes_limit;

	/// Registered organization database
	struct organizations_db_s *db;

	/// Entry of uuid database
	uuid_entry_t uuid_entry;

	/// Reference counter
	uint64_t refcnt;
} organization_db_entry_t;

/** Obtains organization uuid */
#define organization_db_entry_get_uuid(e) ((e)->uuid_entry.uuid)

/** Obtains organization enrichment */
#define organization_get_enrichment(e) ((e)->enrichment)

/** Obtains organization max bytes */
#define organization_get_max_bytes(org) ((org)->bytes_limit.max)

/** Add consumed bytes to an organization
  @param org Organization
  @param bytes Bytes to add
  @return updated consumed bytes
  */
#define organization_add_consumed_bytes(org, bytes) \
        ATOMIC_OP(add, fetch, &(org)->bytes_limit.consumed, bytes)

/** Adds another n2kafka consumed bytes to this organization
  @param org Organization
  @param n2kafka_id n2kafka id that consumed this information
  @param bytes Bytes that n2kafka reports to consume
  */
void organization_add_other_consumed_bytes(organization_db_entry_t *org,
	const char *n2kafka_id, uint64_t bytes);

/** Get's organization's consumed bytes
  @param org Organization
  @return organization's consumed bytes
  */
#define organization_consumed_bytes(org) ((org)->bytes_limit.consumed)

/** Set organization's warning given, and return previous value of the
  variable
  @param org organization
  @return previous value of the variable
  */
#define organization_fetch_set_warning_given(org) \
	ATOMIC_OP(fetch, or, &(org)->bytes_limit.warning_given, 1)

/** Checks if organization byte limit has been reached
  @param org Organization
  @param lock If you need to lock database
  @return 1 if limit has been recahed, 0 in other case
  */
#define organization_limit_reached(org) ({ \
	uint64_t _max = organization_get_max_bytes(org); \
	uint64_t _consumed = organization_consumed_bytes(org); \
	_max != 0 && _consumed > _max; })

/** Decrements uuid entry, signaling that we are not going to use it anymore */
void organizations_db_entry_decref(organization_db_entry_t *entry);

/** Organization uuid database */
typedef struct organizations_db_s {
	/* Private data - do not access directly */
	/** Mutex that protects against concurrent modification of reported
	limits */
	pthread_mutex_t reports_mutex;
	/// database to search for uuids
	uuid_db_t uuid_db;
} organizations_db_t;

/** Initialize a new database
  @param db db to initialize
  @returns new database
  */
int organizations_db_init(organizations_db_t *db);

/** Adds organizations uuid to database, and delete previous ones.
  @note bytes consumed and last byte consumed warning will not be reloaded
  in entries that are mantained throgh reload
  @note Entries that exists previous reload and does not exists after reload
  will be marked with byte_limit = 1 and will be decref()
  @param db db to update
  @param db organizations
  */
void organizations_db_reload(organizations_db_t *db, json_t *organizations);

/** Get an entry from organization database.
  @note Obtained entry need to be freed with organization_db_entry_decref
  @param db database
  @param uuid organization uuid
  @returns organization entry
  */
organization_db_entry_t *organizations_db_get(organizations_db_t *db,
					const char *organization_uuid);

/** Get a bytes consumed report for each organization
  @param db Database
  @param now Report's timestamp
  @param n2kafka_id N2kafka id
  @param clean Clean counters
  @return Reports (if clean==1, before the clean)
  */
struct kafka_message_array *organization_db_interval_consumed0(
				organizations_db_t *db, time_t now,
				const struct itimerspec *interval,
				const struct itimerspec *clean_interval,
				const char *n2kafka_id, int clean);

#define organization_db_interval_consumed(db, now, interval, clean_interval, \
							 n2kafka_id) \
	organization_db_interval_consumed0(db, now, interval, clean_interval, \
								 n2kafka_id, 0)

#define organization_db_clean_consumed(db, now, interval, clean_interval, \
							 n2kafka_id) \
	organization_db_interval_consumed0(db, now, interval, clean_interval, \
								 n2kafka_id, 1)

/** Checks if an entry exists in uuid database.
  @param db database
  @param uuid organization uuid
  @returns 1 if organization found, 0 ioc
  */
int organizations_db_exists(organizations_db_t *db,
						const char *organization_uuid);

/** Destroy a organization database
  @param db Database to destroy
  */
void organizations_db_done(organizations_db_t *db);
