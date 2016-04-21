/*
**
** Copyright (c) 2014, Eneo Tecnologia
** Author: Eugenio Perez <eupm90@gmail.com>
** All rights reserved.
**
** This program is free software; you can redistribute it and/or modify
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

#include "rb_http2k_sensors_database.h"

#include <yajl/yajl_parse.h>
#include <yajl/yajl_gen.h>
#include <util/kafka_message_list.h>
#include <jansson.h>
#include <util/pair.h>

/// @TODO many of the fields here could be a state machine
/// @TODO separate parsing <-> not parsing fields
/// @TODO could this be private?
struct rb_session {
	/// Output generator.
	yajl_gen gen;

	/// JSON handler
	yajl_handle handler;

	/// Sensor information.
	sensor_db_entry_t *sensor;

	/// Bookmark if we are skipping an object or array
	size_t object_array_parsing_stack;

	/// Per POST business.
	const char *client_ip,*sensor_uuid,*topic,*kafka_partitioner_key;

	/// Topid handler
	struct topic_s *topic_handler;

	struct {
#define CURRENT_KEY_OFFSET_NOT_SETTED -1
		/// current kafka message key offset
		int current_key_offset;
		size_t current_key_length;
		int valid;
	} message;

	/// Message list in this call to decode()
	rd_kafka_message_queue_t msg_queue;

	/// We are parsing value of kafka_partitioner_key
	int in_partition_key;

	/// Skip next parsing value
	int skip_value;
};

struct rb_config;
struct rb_session *new_rb_session(struct rb_config *rb_config,
	                                const keyval_list_t *msg_vars);

int gen_jansson_object(yajl_gen gen, json_t *enrichment_data);

void free_rb_session(struct rb_config *rb_config, struct rb_session *sess);
