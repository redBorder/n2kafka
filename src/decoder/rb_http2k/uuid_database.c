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

#include "uuid_database.h"

#include <assert.h>
#include <string.h>

static const uint64_t hashtable_seed = 0;
#define hash_str(key) tommy_hash_u32(hashtable_seed, key, strlen(key))

/** Asserts that we're using a valid uuid_entry */
static void uuid_entry_assert(const uuid_entry_t *uuid_entry) {
	assert(UUID_ENTRY_MAGIC == uuid_entry->magic);
}

/** Check if an uuid_entry has an uuid
  @param arg uuid in const char * format
  @param obj uuid_entry
  @return 0 if equal, 1 ioc
  */
static int uuid_entry_cmp(const void* arg, const void* obj) {
	const char *uuid = arg;
	const uuid_entry_t *uuid_entry = obj;
	uuid_entry_assert(uuid_entry);

	return strcmp(uuid,uuid_entry->uuid);
}

/** Inserts an uuid element in uuid hashtable
  @param h hashtable
  @param n uuid_entry to insert
  */
void uuid_db_insert(uuid_db_t *db, uuid_entry_t *entry) {
	tommy_hashdyn_insert(db, &entry->node, entry, hash_str(entry->uuid));
}

/** Get an entry from uuid database.
  @note Obtained entry need to be freed with uuid_entry_decref
  @param db database
  @param uuid uuid
  @returns uuid entry
  */
uuid_entry_t *uuid_db_search(uuid_db_t *db, const char *uuid) {
	return tommy_hashdyn_search(db, uuid_entry_cmp, uuid,
							hash_str(uuid));
}
