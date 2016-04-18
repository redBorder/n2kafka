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

#include "tommyds/tommyhashdyn.h"

#define uuid_db_entry tommy_hashdyn_node

/// Sensors database entry
typedef struct uuid_entry_s {

#ifndef NDEBUG
	/* Private data - do not access directly */

/// Magic to assert coherency.
#define UUID_ENTRY_MAGIC 0x001D3A1C001D3A1CL
	/// Magic to assert coherency.
	uint64_t magic;
#endif

	/// Entry uuid
	const char *uuid;

	/// hashtable node
	uuid_db_entry node;

	/// Actual data.
	void *data;
} uuid_entry_t;

/// UUID db
#define uuid_db_t tommy_hashdyn

#ifdef UUID_ENTRY_MAGIC
#define uuid_entry_init(e) ((e)->magic = UUID_ENTRY_MAGIC)
#else
#define uuid_entry_init(e)
#endif

/// Init an uuid database
#define uuid_db_init tommy_hashdyn_init
#define uuid_db_done tommy_hashdyn_done
#define uuid_db_foreach tommy_hashdyn_foreach

/** Inserts an uuid element in uuid database
  @param db database
  @param entry uuid_entry to insert
  */
void uuid_db_insert(uuid_db_t *db, uuid_entry_t *entry);

/** Remove an existing entry from database
  @param db database
  @param entry Entry to remove
  */
#define uuid_db_remove(uuid_db, entry) \
	tommy_hashdyn_remove_existing(uuid_db, &(entry)->node)



/** Get an entry from uuid database.
  @note Obtained entry need to be freed with uuid_entry_decref
  @param db database
  @param uuid uuid
  @returns uuid entry
  */
uuid_entry_t *uuid_db_search(uuid_db_t *db, const char *uuid);