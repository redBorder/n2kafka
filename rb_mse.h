/*
**
** Copyright (c) 2014, Eneo Tecnologia
** Author: Eugenio Perez <eupm90@gmail.com>
** All rights reserved.
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU General Public License Version 2 as
** published by the Free Software Foundation.  You may not use, modify or
** distribute this program under any other version of the GNU General
** Public License.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program; if not, write to the Free Software
** Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*/

#pragma once

#include <stdint.h>
#include <string.h>
#include <pthread.h>

/* All functions are thread-safe here, excepting free_valid_mse_database */

struct json_t;
struct mse_database {
	/* Private */
	pthread_rwlock_t rwlock;
	struct json_t *root;
};
void init_mse_database(struct mse_database *db);
int parse_mse_array(struct mse_database *db, const struct json_t *mse_array,char *err,size_t errsize);
void free_valid_mse_database(struct mse_database *db);

struct mse_config {
	struct mse_database database;
};

#if 0
struct enrich_with *process_enrich_with(const char *enrich_with);
void free_enrich_with(struct enrich_with *enrich_with);

#endif
void mse_decode(char *buffer,size_t buf_size,void *listener_callback_opaque);
