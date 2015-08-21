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

/* All functions are thread-safe here, excepting free_valid_mse_database */

struct json_t;
struct rb_database {
	/* Private */
	pthread_rwlock_t rwlock;
	struct json_t *uuid_enrichment;
};
void init_rb_database(struct rb_database *db);
int parse_rb_config(void *_db,const struct json_t *rb_config,char *err,size_t err_size);
void free_valid_rb_database(struct rb_database *db);

struct rb_config {
	struct rb_database database;
};

int rb_opaque_creator(struct json_t *config,void **opaque,char *err,size_t errsize);
int rb_opaque_reload(struct json_t *config,void *opaque);
void rb_opaque_done(void *opaque);
void rb_decode(char *buffer,size_t buf_size,const char *topic,void *listener_callback_opaque);

/// @TODO make more generic
int rb_http2k_validate_uuid(struct rb_database *db,const char *uuid);
int rb_http2k_validate_topic(struct rb_database *db,const char *topic);