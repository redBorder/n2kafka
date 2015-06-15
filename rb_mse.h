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
struct mse_database {
	/* Private */
	pthread_rwlock_t rwlock;
	struct json_t *root;
};
void init_mse_database(struct mse_database *db);
int parse_mse_array(void *_db,const struct json_t *mse_array,char *err,size_t err_size);
void free_valid_mse_database(struct mse_database *db);

struct mse_config {
	struct mse_database database;
};

int mse_opaque_creator(struct json_t *config,void **opaque,char *err,size_t errsize);
void mse_opaque_done(void **opaque);
void mse_decode(char *buffer,size_t buf_size,void *listener_callback_opaque);
