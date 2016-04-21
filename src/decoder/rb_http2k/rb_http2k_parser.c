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

#include "rb_http2k_parser.h"

/// @TODO this include is only for config. Separate config in another file,
/// since we have crossed includes
#include "rb_http2k_decoder.h"
#include "util/topic_database.h"

#include <yajl/yajl_parse.h>
#include <yajl/yajl_gen.h>
#include <jansson.h>
#include <librd/rdlog.h>
#include <librd/rdmem.h>

#include <assert.h>
#include <string.h>


/*
    PARSING & ENRICHMENT
*/

static int gen_jansson_array(yajl_gen gen, json_t *enrichment_data);

static int gen_jansson_value(yajl_gen gen, json_t *value) {
	json_error_t jerr;
	const char *str;
	size_t len;
	int rc;

	int type = json_typeof(value);
	switch(type) {
	case JSON_OBJECT:
		yajl_gen_map_open(gen);
		gen_jansson_object(gen,value);
		yajl_gen_map_close(gen);
		break;

	case JSON_ARRAY:
		yajl_gen_array_open(gen);
		gen_jansson_array(gen,value);
		yajl_gen_array_close(gen);
		break;

	case JSON_STRING:
		rc = json_unpack_ex(value, &jerr, 0, "s%", &str,&len);
		if(rc != 0) {
			rdlog(LOG_ERR,"Couldn't extract string: %s",jerr.text);
			return 0;
		}
		yajl_gen_string(gen, (const unsigned char *)str, len);
		break;

	case JSON_INTEGER:
		{
			json_int_t i = json_integer_value(value);
			yajl_gen_integer(gen,i);
		}
		break;

	case JSON_REAL:
		{
			double d = json_number_value(value);
			yajl_gen_double(gen,d);
		}
		break;

	case JSON_TRUE:
		yajl_gen_bool(gen,1);
		break;

	case JSON_FALSE:
		yajl_gen_bool(gen,0);
		break;

	case JSON_NULL:
		yajl_gen_null(gen);
		break;

	default:
		rdlog(LOG_ERR,"Unkown jansson type %d",type);
		break;
	};

	return 1;
}

/// @TODO check gen_ return
static int gen_jansson_array(yajl_gen gen, json_t *array) {
	size_t array_index;
	json_t *value;

	json_array_foreach(array, array_index, value) {
		gen_jansson_value(gen,value);
	}

	return 1;
}

/// @TODO check gen_ return
int gen_jansson_object(yajl_gen gen, json_t *object) {
	assert(gen);
	assert(object);

	json_t *value;
	const char *key;

	/// This function is suppose to be thread-safe
	json_object_foreach(object,key,value) {
		size_t key_len = strlen(key);
		yajl_gen_string(gen, (const unsigned char *)key, key_len);
		gen_jansson_value(gen,value);
	}

	return 1;
}

static void rb_session_reset_kafka_msg(struct rb_session *sess) {
	sess->message.current_key_offset = CURRENT_KEY_OFFSET_NOT_SETTED;
	sess->message.current_key_length = 0;
	sess->message.valid = 1;
}

#define GEN_AND_RETURN(func) \
	do { return yajl_gen_status_ok == func; } while(0);

/** key/Value generating that checks if we are in an value that we have to skip
	@param sess Current parser session
	@param func Function used to generate object
	@param check_root Check if we are in root object. If we are, we know
	that we have to stop skipping next input values
	*/
#define GEN_OR_SKIP0(sess,func,check_root)                                    \
	{                                                                     \
		if(!(sess)->skip_value) {                                     \
			GEN_AND_RETURN(func);                                 \
		} else {                                                      \
			if(check_root &&                                      \
				1 == (sess)->object_array_parsing_stack) {    \
				/* We are in the root, so we end the skip */  \
				(sess)->skip_value = 0;                       \
			}                                                     \
			return 1;                                             \
		}                                                             \
	}

/** Generates or skip a json value */
#define GEN_OR_SKIP(sess,func) GEN_OR_SKIP0(sess,func,1)

/** Generates or skip a json value if we know that we are not in the root
	object. Using this macro instead of GEN_OR_SKIP we are saving 1 branch.
	*/
#define GEN_OR_SKIP_NO_ROOT(sess,func) GEN_OR_SKIP0(sess,func,0)

#define CHECK_PARTITIONER_KEY_IS(sess,expected_val,...)   \
	if(expected_val != (sess)->in_partition_key) {    \
		rdlog(LOG_ERR,__VA_ARGS__);               \
		/* Stop parsing this message */           \
		(sess)->message.valid = 0;                \
		/* We are not in partition key anymore */ \
		(sess)->in_partition_key = 0;             \
		return 1;                                 \
	}

#define CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,...) \
	CHECK_PARTITIONER_KEY_IS(sess,0,__VA_ARGS__)  \

#define CHECK_SESSION_IN_ROOT_OBJECT(sess,...)    \
	if((sess)->object_array_parsing_stack != 1) { \
		rdlog(LOG_WARNING,__VA_ARGS__);           \
		return 0;                                 \
	}

#define CHECK_SESSION_NOT_IN_ROOT_OBJECT(sess,...) \
	if((sess)->object_array_parsing_stack == 1) {  \
		rdlog(LOG_WARNING,__VA_ARGS__);            \
		return 0;                                  \
	}

/// Checks that we are in an object different than root object
#define CHECK_IN_OBJECT(sess,...)                 \
	if((sess)->object_array_parsing_stack > 1) { \
		rdlog(LOG_WARNING,__VA_ARGS__);           \
		return 0;                                 \
	}

#define SKIP_IF_MESSAGE_NOT_VALID(sess) \
	if(!(sess)->message.valid) { \
		return 1;               \
	}

static int rb_parse_null(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	SKIP_IF_MESSAGE_NOT_VALID(sess)
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"%s as partition key","null");
	GEN_OR_SKIP(sess,yajl_gen_null(g));
}

static int rb_parse_boolean(void * ctx, int boolean)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	SKIP_IF_MESSAGE_NOT_VALID(sess)
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"%s as partition key",
		boolean?"true":"false");
	GEN_OR_SKIP(sess,yajl_gen_bool(g, boolean));
}

static int rb_parse_number(void * ctx, const char * s, size_t l)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	SKIP_IF_MESSAGE_NOT_VALID(sess)
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"%.*s as partition key",
		(int)l,s);
	GEN_OR_SKIP(sess,yajl_gen_number(g, s, l));
}

static int rb_parse_string(void * ctx, const unsigned char * stringVal,
                           size_t stringLen)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	SKIP_IF_MESSAGE_NOT_VALID(sess)

	if(sess->in_partition_key) {
		const unsigned char *buffer = NULL;
		size_t buffer_len = 0;

		if(sess->message.current_key_offset !=
					CURRENT_KEY_OFFSET_NOT_SETTED) {
			rdlog(LOG_ERR,
				"Partition key already present (%s key twice?)"
				, sess->kafka_partitioner_key);
			sess->message.valid = 0;
		}

		const int get_buf_rc = yajl_gen_get_buf(g,
                                  &buffer,
                                  &buffer_len);

		if(get_buf_rc != yajl_gen_status_ok) {
			/// @TODO manage this
		}

		// Message key will be the next stuff printed.
		sess->message.current_key_offset = buffer_len + strlen(":\"");
		sess->message.current_key_length = stringLen;
		sess->in_partition_key = 0;
	}

	GEN_OR_SKIP(sess,yajl_gen_string(g, stringVal, stringLen));
}

static int rb_parse_map_key(void * ctx, const unsigned char * stringVal,
                            size_t stringLen)
{
	char buf[stringLen+1];
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	SKIP_IF_MESSAGE_NOT_VALID(sess)

	if(sess->object_array_parsing_stack > 1) {
		/// We are not in root object. Should we print?
		if(sess->skip_value) {
			return 1;
		} else {
			GEN_AND_RETURN(yajl_gen_string(g, stringVal, stringLen));
		}
	} else {
		const json_t *sensor_enrichment
			= sensor_db_entry_json_enrichment(sess->sensor);
		if (sess->kafka_partitioner_key &&
			0 == strncmp(sess->kafka_partitioner_key,
					(const char *)stringVal, stringLen)) {
			/* We are in kafka partitioner key, need to watch for
									it */
			sess->in_partition_key = 1;
		}

		buf[stringLen] = '\0';
		memcpy(buf,stringVal,stringLen);
		const json_t *uuid_enrichment = json_object_get(
						sensor_enrichment, buf);
		if(NULL == uuid_enrichment) {
			/* Nothing to worry, go ahead */
			GEN_AND_RETURN(yajl_gen_string(g, stringVal, stringLen));
		} else {
			/* Need to skip this value, since it is contained in enrichment 
			values */
			sess->skip_value = 1;
			return 1;
		}
	}
}

static int rb_parse_start_map(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	++sess->object_array_parsing_stack;
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"Object as partitioner key");
	SKIP_IF_MESSAGE_NOT_VALID(sess)

	GEN_OR_SKIP_NO_ROOT(sess,yajl_gen_map_open(g));
}

/** Adds organizations bytes, and emit a warning if this message has caused
    that the organization's bytes quota has been reached. */
static void add_organization_bytes(organization_db_entry_t *org,
					uint64_t bytes, int *limit_reached) {
	organization_add_consumed_bytes(org, bytes);
	*limit_reached = organization_limit_reached(org);
	if (*limit_reached && !organization_fetch_set_warning_given(org)) {
		const char *org_uuid = organization_db_entry_get_uuid(org);
		rdlog(LOG_INFO,
			"Organzation %s has reached it's bytes quota",
							org_uuid);
	}
}

static int rb_parse_generate_rdkafka_message(const struct rb_session *sess,
						rd_kafka_message_t *msg) {
	const int message_key_offset = sess->message.current_key_offset;
	const unsigned char * buf;
	int limit_reached=0;
	organization_db_entry_t *organization = sensor_db_entry_organization(
								sess->sensor);
	memset(msg,0,sizeof(*msg));

	msg->partition = RD_KAFKA_PARTITION_UA;

	yajl_gen_get_buf(sess->gen, &buf, &msg->len);

	if (organization) {
		add_organization_bytes(organization, msg->len, &limit_reached);
		if (limit_reached) {
			return -1;
		}
	}

	/// @TODO do not copy, steal the buffer!
	msg->payload = strdup((const char *)buf);
	if(NULL == msg->payload) {
		rdlog(LOG_ERR,"Unable to duplicate buffer");
		return -1;
	}

	if(message_key_offset !=  CURRENT_KEY_OFFSET_NOT_SETTED) {
		msg->key = (char *)msg->payload + message_key_offset;
		msg->key_len = sess->message.current_key_length;
	}

	return 0;
}

static int rb_parse_end_map(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	--sess->object_array_parsing_stack;

	if(0 == sess->object_array_parsing_stack) {
		if (sess->message.valid) {
			json_t *client_enrichment =
				sensor_db_entry_json_enrichment(sess->sensor);
			rd_kafka_message_t msg;
			/* Ending message, we need to add enrichment values */
			gen_jansson_object(g,client_enrichment);
			yajl_gen_map_close(g);
			if (0 == rb_parse_generate_rdkafka_message(sess,
								&msg)) {
				rd_kafka_msg_q_add(&sess->msg_queue,&msg);
			}
		}

		memset(&sess->message,0,sizeof(sess->message));
		rb_session_reset_kafka_msg(sess);

		yajl_gen_reset(sess->gen,NULL);
		yajl_gen_clear(sess->gen);
		return 1;
	} else {
		SKIP_IF_MESSAGE_NOT_VALID(sess)
		GEN_OR_SKIP(sess,yajl_gen_map_close(g));
	}
}

static int rb_parse_start_array(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	++sess->object_array_parsing_stack;
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"array start as partition key");

	GEN_OR_SKIP_NO_ROOT(sess,yajl_gen_array_open(g));
}

static int rb_parse_end_array(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	--sess->object_array_parsing_stack;

	GEN_OR_SKIP(sess,yajl_gen_array_close(g));
}

static const yajl_callbacks callbacks = {
    rb_parse_null,
    rb_parse_boolean,
    NULL,
    NULL,
    rb_parse_number,
    rb_parse_string,
    rb_parse_start_map,
    rb_parse_map_key,
    rb_parse_end_map,
    rb_parse_start_array,
    rb_parse_end_array
};

/// @TODO do not use rb_config, but rb_config->database!
struct rb_session *new_rb_session(struct rb_config *rb_config,
	                                const keyval_list_t *msg_vars) {

	const char *client_ip = valueof(msg_vars, "client_ip");
	const char *sensor_uuid = valueof(msg_vars, "sensor_uuid");
	const char *topic = valueof(msg_vars, "topic");
	struct topic_s *topic_handler = NULL;
	sensor_db_entry_t *sensor = NULL;

	rb_http2k_database_get_topic_client(&rb_config->database, topic,
		sensor_uuid, &topic_handler, &sensor);

	if (NULL == topic_handler) {
		rdlog(LOG_ERR,"Invalid topic %s received from client %s",
			topic,client_ip);
		return NULL;
	} else if (NULL == sensor) {
		rdlog(LOG_ERR,"Invalid sensor UUID %s from client %s",
			sensor_uuid,client_ip);
		return NULL;
	}

	const char *kafka_partitioner_key = topics_db_partition_key(topic_handler);

	struct rb_session *sess = NULL;
	rd_calloc_struct(&sess,sizeof(*sess),
		-1,client_ip,&sess->client_ip,
		-1,sensor_uuid,&sess->sensor_uuid,
		-1,topic,&sess->topic,
		kafka_partitioner_key?-1:0,kafka_partitioner_key,&sess->kafka_partitioner_key,
		RD_MEM_END_TOKEN);

	if(NULL == sess) {
		rdlog(LOG_CRIT, "Couldn't allocate sess pointer");
		goto sensor_err;
	}

	rd_kafka_msg_q_init(&sess->msg_queue);
	sess->sensor = sensor;
	sess->topic_handler = topic_handler;

	if(NULL == kafka_partitioner_key) {
		sess->kafka_partitioner_key = NULL;
	}

	sess->gen = yajl_gen_alloc(NULL);
	if(NULL == sess->gen) {
		rdlog(LOG_CRIT,"Couldn't allocate yajl_gen");
		goto err_sess;
	}

	sess->handler = yajl_alloc(&callbacks, NULL, sess);
	if(NULL == sess->handler) {
		rdlog(LOG_CRIT,"Couldn't allocate yajl_handler");
		goto err_yajl_gen;
	}

	yajl_config(sess->handler, yajl_allow_multiple_values, 1);
	yajl_config(sess->handler, yajl_allow_trailing_garbage, 1);

	rb_session_reset_kafka_msg(sess);

	return sess;

err_yajl_gen:
	yajl_gen_free(sess->gen);

err_sess:
	free(sess);

sensor_err:
	sensor_db_entry_decref(sensor);

	return NULL;
}

void free_rb_session(struct rb_config *rb_config,struct rb_session *sess) {
	yajl_free(sess->handler);
	yajl_gen_free(sess->gen);

	sensor_db_entry_decref(sess->sensor);

	topic_decref(sess->topic_handler);

	free(sess);
}
