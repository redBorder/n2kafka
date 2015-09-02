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

#include <stdint.h>
#include <string.h>
#include <pthread.h>

/* All functions are thread-safe here, excepting free_valid_meraki_database */

struct json_t;
struct meraki_database {
	/* Private */
	pthread_rwlock_t rwlock;
	struct json_t *root;
};

static void init_meraki_database(struct meraki_database *db) __attribute__((unused));
static void init_meraki_database(struct meraki_database *db) {
	pthread_rwlock_init(&db->rwlock,0);
	db->root = NULL;
}

int meraki_opaque_creator(struct json_t *config,void **opaque);
int meraki_opaque_reload(struct json_t *config,void *opaque);
void meraki_opaque_destructor(void *opaque);

int parse_meraki_secrets(void *db, const struct json_t *meraki_object);

void meraki_database_done(struct meraki_database *db);

struct meraki_config {
	struct meraki_database database;
};

void meraki_decode(char *buffer,size_t buf_size,const char *topic,const char *client,void *listener_callback_opaque);
