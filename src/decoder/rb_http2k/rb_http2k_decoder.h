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

#include "rb_database.h"

#include "pair.h"

#include <librdkafka/rdkafka.h>
#include <librd/rdavl.h>
#include <librd/rdsysqueue.h>
#include <jansson.h>

#include <stdint.h>
#include <string.h>
#include <pthread.h>

/* All functions are thread-safe here, excepting free_valid_mse_database */
struct json_t;
struct rb_config {
	struct rb_database database;
};

int parse_rb_config(void *_db,const struct json_t *rb_config);

int rb_opaque_creator(struct json_t *config,void **opaque);
int rb_opaque_reload(struct json_t *config,void *opaque);
void rb_opaque_done(void *opaque);
void rb_decode(char *buffer,size_t buf_size,const keyval_list_t *props,
                void *listener_callback_opaque,void **decoder_sessionp);
