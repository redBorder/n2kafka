/*
** Copyright (C) 2015 Eneo Tecnologia S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
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
#include "parse.h"

#include <string.h>

/* Private data */
struct rd_kafka_message_s;

struct kafka_message_array{
	size_t count; /* Number of used elements in msgs */
	size_t size;  /* Number of elements in msgs */
	struct rd_kafka_message_s *msgs; /* Real msgs */
};

void init_rdkafka();
void send_to_kafka(char *buffer,const size_t bufsize,int flags,void *opaque);
void dumb_decoder(char *buffer,size_t buf_size,const char *topic,void *listener_callback_opaque);

struct kafka_message_array *new_kafka_message_array(size_t size);
int save_kafka_msg_in_array(struct kafka_message_array *array,char *buffer,size_t buf_size,void *opaque);
void send_array_to_kafka(struct kafka_message_array *);


void kafka_poll();

void flush_kafka();
void stop_rdkafka();
