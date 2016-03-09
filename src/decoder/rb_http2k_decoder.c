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

#include "rb_http2k_decoder.h"
#include "rb_mac.h"
#include "kafka.h"
#include "global_config.h"
#include "rb_json.h"
#include "topic_database.h"
#include "kafka_message_list.h"

#include <yajl/yajl_parse.h>
#include <yajl/yajl_gen.h>
#include <librd/rdlog.h>
#include <librd/rdmem.h>
#include <assert.h>
#include <jansson.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <librd/rd.h>
#include <librdkafka/rdkafka.h>

static const char RB_HTTP2K_CONFIG_KEY[] = "rb_http2k_config";
static const char RB_SENSOR_UUID_ENRICHMENT_KEY[] = "uuids";
static const char RB_TOPICS_KEY[] = "topics";
static const char RB_SENSOR_UUID_KEY[] = "uuid";

static const char RB_TOPIC_PARTITIONER_KEY[] = "partition_key";
static const char RB_TOPIC_PARTITIONER_ALGORITHM_KEY[] = "partition_algo";

typedef int32_t (*partitioner_cb) (const rd_kafka_topic_t *rkt,
                                   const void *keydata, size_t keylen, int32_t partition_cnt,
                                   void *rkt_opaque, void *msg_opaque);

static int32_t (mac_partitioner) (const rd_kafka_topic_t *rkt,
                                  const void *keydata, size_t keylen, int32_t partition_cnt,
                                  void *rkt_opaque, void *msg_opaque);

/** Algorithm of messages partitioner */
enum partitioner_algorithm {
	/** Random partitioning */
	none,
	/** Mac partitioning */
	mac,
};

struct {
	enum partitioner_algorithm algoritm;
	const char *name;
	partitioner_cb partitioner;
} partitioner_algorithm_list[] = {
	{mac, "mac", mac_partitioner},
};

void init_rb_database(struct rb_database *db) {
	memset(db, 0, sizeof(*db));
	pthread_rwlock_init(&db->rwlock, 0);
}

#ifndef NDEBUG
#define RB_OPAQUE_MAGIC 0x0B0A3A1C0B0A3A1CL
#endif

enum warning_times_pos {
	LAST_WARNING_TIME__QUEUE_FULL,
	LAST_WARNING_TIME__MSG_SIZE_TOO_LARGE,
	LAST_WARNING_TIME__UNKNOWN_PARTITION,
	LAST_WARNING_TIME__UNKNOWN_TOPIC,
	LAST_WARNING_TIME__END
};

struct rb_opaque {
#ifdef RB_OPAQUE_MAGIC
	uint64_t magic;
#endif

	struct rb_config *rb_config;

	pthread_mutex_t produce_error_last_time_mutex[LAST_WARNING_TIME__END];
	time_t produce_error_last_time[LAST_WARNING_TIME__END];
};

static enum warning_times_pos kafka_error_to_warning_time_pos(
                                                    rd_kafka_resp_err_t err) {
	switch(err) {
	case RD_KAFKA_RESP_ERR__QUEUE_FULL:
		return LAST_WARNING_TIME__QUEUE_FULL;
	case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
		return LAST_WARNING_TIME__MSG_SIZE_TOO_LARGE;
	case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
		return LAST_WARNING_TIME__UNKNOWN_PARTITION;
	case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
		return LAST_WARNING_TIME__UNKNOWN_TOPIC;
	default:
		return LAST_WARNING_TIME__END;
	};
}

static int32_t mac_partitioner (const rd_kafka_topic_t *rkt,
                                const void *keydata, size_t keylen, int32_t partition_cnt,
                                void *rkt_opaque, void *msg_opaque) {
	size_t toks = 0;
	uint64_t intmac = 0;
	char mac_key[sizeof("00:00:00:00:00:00")];

	if (keylen != strlen("00:00:00:00:00:00")) {
		if (keylen != 0) {
			/* We were expecting a MAC and we do not have it */
			rdlog(LOG_WARNING, "Invalid mac %.*s len", (int)keylen,
				(const char *)keydata);
		}
		goto fallback_behavior;
	}

	mac_key[0] = '\0';
	strncat(mac_key, (const char *)keydata, sizeof(mac_key) - 1);

	for (toks = 1; toks < 6; ++toks) {
		const size_t semicolon_pos = 3 * toks - 1;
		if (':' != mac_key[semicolon_pos]) {
			rdlog(LOG_WARNING, "Invalid mac %.*s (it does not have ':' in char %zu.",
			      (int)keylen, mac_key, semicolon_pos);
			goto fallback_behavior;
		}
	}

	for (toks = 0; toks < 6; ++toks) {
		char *endptr = NULL;
		intmac = (intmac << 8) + strtoul(&mac_key[3 * toks], &endptr, 16);
		/// The key should end with '"' in json format
		if ((toks < 5 && *endptr != ':') || (toks == 5 && *endptr != '\0')) {
			rdlog(LOG_WARNING, "Invalid mac %.*s, unexpected %c end of %zu token",
			      (int)keylen, mac_key, *endptr, toks);
			goto fallback_behavior;

		}

		if (endptr != mac_key + 3 * (toks + 1) - 1) {
			rdlog(LOG_WARNING, "Invalid mac %.*s, unexpected token length at %zu token",
			      (int)keylen, mac_key, toks);
			goto fallback_behavior;
		}
	}

	return intmac % (uint32_t)partition_cnt;

fallback_behavior:
	return rd_kafka_msg_partitioner_random(rkt, keydata, keylen, partition_cnt,
	                                       rkt_opaque, msg_opaque);
}

/** Parsing of per uuid enrichment.
	@param config original config with
	RB_SENSOR_UUID_ENRICHMENT_KEY to extract it.
	@param uuid_enrichment per-uuid enrichment object.
	@param dangerous_values Json hsahtable with values that could could belong
	to any client
	@return 0 if ok. Any other value should be checked against jerr.
	*/
static int parse_per_uuid_opaque_config(json_t *config,
	                json_t **uuid_enrichment) {
	json_error_t jerr;

	assert(config);
	assert(uuid_enrichment);

	const int json_unpack_rc = json_unpack_ex(config, &jerr, 0, "{s:O}",
	                           RB_SENSOR_UUID_ENRICHMENT_KEY, uuid_enrichment);

	if (0 != json_unpack_rc) {
		rdlog(LOG_ERR,"Couldn't unpack %s key: %s",
			RB_SENSOR_UUID_ENRICHMENT_KEY,jerr.text);
	}

	return json_unpack_rc;
}

static partitioner_cb partitioner_of_name(const char *name) {
	size_t i = 0;

	assert(name);

	for (i = 0; i < RD_ARRAYSIZE(partitioner_algorithm_list); ++i) {
		if (0 == strcmp(partitioner_algorithm_list[i].name, name)) {
			return partitioner_algorithm_list[i].partitioner;
		}
	}

	return NULL;
}

static int parse_topic_list_config(json_t *config, struct topics_db *new_topics_db) {
	const char *key;
	json_t *value;
	json_error_t jerr;
	json_t *topic_list = NULL;
	int pass = 0;

	assert(config);

	const int json_unpack_rc = json_unpack_ex(config, &jerr, 0, "{s:o}",
	                           RB_TOPICS_KEY, &topic_list);

	if (0 != json_unpack_rc) {
		rdlog(LOG_ERR, "%s", jerr.text);
		return json_unpack_rc;
	}

	if (!json_is_object(topic_list)) {
		rdlog(LOG_ERR, "%s is not an object", RB_TOPICS_KEY);
		return -1;
	}

	const size_t array_size = json_object_size(topic_list);
	if (0 == array_size) {
		rdlog(LOG_ERR, "%s has no childs", RB_TOPICS_KEY);
		return -1;
	}

	json_object_foreach(topic_list, key, value) {
		const char *partition_key = NULL, *partition_algo = NULL;
		size_t partition_key_len = 0;
		const char *topic_name = key;
		rd_kafka_topic_t *rkt = NULL;

		if (!json_is_object(value)) {
			if (pass == 0) {
				rdlog(LOG_ERR, "Topic %s is not an object. Discarding.", topic_name);
			}
			continue;
		}

		const int topic_unpack_rc = json_unpack_ex(value, &jerr, 0, "{s?s%,s?s}",
		                            RB_TOPIC_PARTITIONER_KEY, &partition_key, &partition_key_len,
		                            RB_TOPIC_PARTITIONER_ALGORITHM_KEY, &partition_algo);

		if (0 != topic_unpack_rc) {
			rdlog(LOG_ERR, "Can't extract information of topic %s (%s). Discarding.",
			      topic_name, jerr.text);
			continue;
		}

		rd_kafka_topic_conf_t *my_rkt_conf = rd_kafka_topic_conf_dup(
                                global_config.kafka_topic_conf);
		if(NULL == my_rkt_conf) {
			rdlog(LOG_ERR,"Couldn't topic_conf_dup in topic %s",topic_name);
			continue;
		}

		if (NULL != partition_algo) {
			partitioner_cb partitioner = partitioner_of_name(partition_algo);
			if (NULL != partitioner) {
				rd_kafka_topic_conf_set_partitioner_cb(my_rkt_conf, partitioner);
			} else {
				rdlog(LOG_ERR, 
					"Can't found partitioner algorithm %s for topic %s", 
					partition_algo,topic_name);
			}
		}

		rkt = rd_kafka_topic_new(global_config.rk, topic_name, my_rkt_conf);
		if (NULL == rkt) {
			char buf[BUFSIZ];
			strerror_r(errno, buf, sizeof(buf));
			rdlog(LOG_ERR, "Can't create topic %s: %s", topic_name, buf);
			rd_kafka_topic_conf_destroy(my_rkt_conf);
			continue;
		}

		topics_db_add(new_topics_db,rkt,partition_key,partition_key_len);
	}

	return 0;
}

int rb_opaque_creator(json_t *config __attribute__((unused)), void **_opaque) {
	size_t i;

	assert(_opaque);

	struct rb_opaque *opaque = (*_opaque) = calloc(1, sizeof(*opaque));
	if (NULL == opaque) {
		rdlog(LOG_ERR, "Can't alloc RB_HTTP2K opaque (out of memory?)");
		return -1;
	}

#ifdef RB_OPAQUE_MAGIC
	opaque->magic = RB_OPAQUE_MAGIC;
#endif

	for(i=0;i<RD_ARRAYSIZE(opaque->produce_error_last_time_mutex);++i) {
		pthread_mutex_init(&opaque->produce_error_last_time_mutex[i], NULL);
	}

	/// @TODO move global_config to static allocated buffer
	opaque->rb_config = &global_config.rb;

	return 0;
}

int rb_opaque_reload(json_t *config, void *_opaque) {
	int rc = 0;
	struct rb_opaque *opaque = _opaque;
	struct topics_db *old_topics_db = NULL, *my_new_topics_db = NULL;
	json_t *new_uuid_enrichment = NULL, *old_uuid_enrichment = NULL;

	assert(opaque);
	assert(config);
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == opaque->magic);
#endif

	const int per_sensor_uuid_enrichment_rc = parse_per_uuid_opaque_config(config,
	        &new_uuid_enrichment);
	if (per_sensor_uuid_enrichment_rc != 0 || NULL == new_uuid_enrichment) {
		rc = -1;
		goto err;
	}

	my_new_topics_db = topics_db_new();

	const int topic_list_rc = parse_topic_list_config(config,my_new_topics_db);
	if (topic_list_rc != 0) {
		rc = -1;
		goto err;
	}

	pthread_rwlock_wrlock(&opaque->rb_config->database.rwlock);

	old_uuid_enrichment = opaque->rb_config->database.uuid_enrichment;
	opaque->rb_config->database.uuid_enrichment = new_uuid_enrichment;
	new_uuid_enrichment = NULL;

	old_topics_db = opaque->rb_config->database.topics_db;
	opaque->rb_config->database.topics_db = my_new_topics_db;
	my_new_topics_db = NULL;

	pthread_rwlock_unlock(&opaque->rb_config->database.rwlock);

err:
	if (my_new_topics_db) {
		topics_db_done(my_new_topics_db);
	}

	if (old_topics_db) {
		topics_db_done(old_topics_db);
	}

	if (old_uuid_enrichment) {
		json_decref(old_uuid_enrichment);
	}

	if (new_uuid_enrichment) {
		json_decref(new_uuid_enrichment);
	}

	return rc;
}

void rb_opaque_done(void *_opaque) {
	assert(_opaque);

	struct rb_opaque *opaque = _opaque;
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == opaque->magic);
#endif

	free(opaque);
}

int parse_rb_config(void *void_db, const struct json_t *config) {
	assert(void_db);
	char errbuf[BUFSIZ];
	struct rb_config *rb_config = void_db;

	struct rb_opaque dummy_opaque = {
#ifdef RB_OPAQUE_MAGIC
		.magic = RB_OPAQUE_MAGIC,
#endif
		/// @TODO quick hack, parse properly
		.rb_config = rb_config
	};

	if (only_stdout_output()) {
		rdlog(LOG_ERR, "Can't use rb_http2k decoder if not kafka brokers configured.");
		return -1;
	}

	const int rwlock_init_rc = pthread_rwlock_init(&rb_config->database.rwlock,
	                           NULL);
	if (rwlock_init_rc != 0) {
		strerror_r(errno, errbuf, sizeof(errbuf));
		rdlog(LOG_ERR, "Can't start rwlock: %s", errbuf);
	}

	json_t *aux = json_deep_copy(config);
	rb_opaque_reload(aux, &dummy_opaque);
	json_decref(aux);

	return 0;
}

void free_valid_rb_database(struct rb_database *db) {
	if (db) {
		if (db->uuid_enrichment) {
			json_decref(db->uuid_enrichment);
		}

		if (db->topics_db) {
			topics_db_done(db->topics_db);
		}

		pthread_rwlock_destroy(&db->rwlock);
	}
}

/*
	PRE-PROCESSING VALIDATIONS
*/

int rb_http2k_validate_uuid(struct rb_database *db, const char *uuid) {
	pthread_rwlock_rdlock(&db->rwlock);
	const int ret = NULL != json_object_get(db->uuid_enrichment, uuid);
	pthread_rwlock_unlock(&db->rwlock);

	return ret;
}

int rb_http2k_validate_topic(struct rb_database *db, const char *topic) {
       pthread_rwlock_rdlock(&db->rwlock);
       const int ret = topics_db_topic_exists(db->topics_db,topic);
       pthread_rwlock_unlock(&db->rwlock);

       return ret;
}


/*
    PARSING & ENRICHMENT
*/

static int gen_jansson_object(yajl_gen gen, json_t *enrichment_data);
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
static int gen_jansson_object(yajl_gen gen, json_t *object) {
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

/** Produce a batch of messages
	@param topic Topic handler
	@param msgs Messages to send
	@param len Length of msgs */
static void produce_or_free(struct rb_opaque *opaque, struct topic_s *topic,
                                        rd_kafka_message_t *msgs, int len) {
	assert(topic);
	assert(msgs);
	static const time_t alert_threshold = 5*60;

	rd_kafka_topic_t *rkt = topics_db_get_rdkafka_topic(topic);

	const int produce_ret = rd_kafka_produce_batch(rkt, RD_KAFKA_PARTITION_UA,
	                        RD_KAFKA_MSG_F_FREE, msgs, len);

	if (produce_ret != len) {
		int i;
		for(i=0;i<len;++i) {
			if(msgs[i].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
				time_t last_warning_time = 0;
				int warn = 0;
				const size_t last_warning_time_pos = kafka_error_to_warning_time_pos(msgs[i].err);

				if(last_warning_time_pos < LAST_WARNING_TIME__END) {
					const time_t curr_time = time(NULL);
					pthread_mutex_lock(&opaque->produce_error_last_time_mutex[last_warning_time_pos]);
					last_warning_time = opaque->produce_error_last_time[last_warning_time_pos];
					if(difftime(curr_time,last_warning_time) > alert_threshold) {
						opaque->produce_error_last_time[last_warning_time_pos] = curr_time;
						warn = 1;
					}
					pthread_mutex_unlock(&opaque->produce_error_last_time_mutex[last_warning_time_pos]);
				}

				if(warn) {
					/* If no alert threshold established or last alert is too old */
					rdlog(LOG_ERR, "Can't produce to topic %s: %s",
					      rd_kafka_topic_name(rkt),
					      rd_kafka_err2str(msgs[i].err));
				}

				free(msgs[i].payload);
			}
		}
	}
}

/// @TODO many of the fields here could be a state machine
struct rb_session {
	/// Output generator.
	yajl_gen gen;

	/// JSON handler
	yajl_handle handler;

	/// json_uuid with this flow client.
	json_t *client_enrichment;

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

static void rb_session_reset_kafka_msg(struct rb_session *sess) {
	sess->message.current_key_offset = CURRENT_KEY_OFFSET_NOT_SETTED;
	sess->message.current_key_length = 0;
	sess->message.valid = 1;
}

#define GEN_AND_RETURN(func) \
	do { return yajl_gen_status_ok == func; } while(0);

#define GEN_OR_SKIP(sess,func)                                 \
	{                                                          \
		if(!(sess)->skip_value) {                              \
			GEN_AND_RETURN(func);                              \
		} else {                                               \
			if(1 == (sess)->object_array_parsing_stack) {      \
				/* We are in the root, so we end the skip */   \
				(sess)->skip_value = 0;                        \
			}                                                  \
			return 1;                                          \
		}                                                      \
	}

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
		if (sess->kafka_partitioner_key &&
			0 == strncmp(sess->kafka_partitioner_key,(const char *)stringVal,
		                                                        stringLen)) {
			/* We are in kafka partitioner key, need to watch for it */
			sess->in_partition_key = 1;
		}

		buf[stringLen] = '\0';
		memcpy(buf,stringVal,stringLen);
		json_t *uuid_enrichment = json_object_get(sess->client_enrichment,buf);
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

	GEN_OR_SKIP(sess,yajl_gen_map_open(g));
}

static int rb_parse_generate_rdkafka_message(const struct rb_session *sess,
						rd_kafka_message_t *msg) {
	const int message_key_offset = sess->message.current_key_offset;
	memset(msg,0,sizeof(*msg));

	msg->partition = RD_KAFKA_PARTITION_UA;

	const unsigned char * buf;
	yajl_gen_get_buf(sess->gen, &buf, &msg->len);

	/// @TODO do not copy, steal the buffer!
	msg->payload = strdup((const char *)buf);
	if(NULL == msg->payload) {
		rdlog(LOG_ERR,"Unable to duplicate buffer");
		return 0;
	}

	if(message_key_offset !=  CURRENT_KEY_OFFSET_NOT_SETTED) {
		msg->key = (char *)msg->payload + message_key_offset;
		msg->key_len = sess->message.current_key_length;
	}

	return 1;
}

static int rb_parse_end_map(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;
	if(0 == sess->object_array_parsing_stack) {
		rdlog(LOG_WARNING,"Discarding parsing because closing an unopen JSON");
		return 0;
	}

	--sess->object_array_parsing_stack;
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"Object closing as partitioner key");

	if(0 == sess->object_array_parsing_stack) {
		if (sess->message.valid) {
			rd_kafka_message_t msg;
			/* Ending message, we need to add enrichment values */
			gen_jansson_object(g,sess->client_enrichment);
			yajl_gen_map_close(g);
			rb_parse_generate_rdkafka_message(sess,&msg);
			rd_kafka_msg_q_add(&sess->msg_queue,&msg);
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

	GEN_OR_SKIP(sess,yajl_gen_array_open(g));
}

static int rb_parse_end_array(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	CHECK_SESSION_NOT_IN_ROOT_OBJECT(sess,"Root object end with an array.");
	--sess->object_array_parsing_stack;
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"array en as partition key");

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

static struct rb_session *new_rb_session(struct rb_config *rb_config,
	                                const keyval_list_t *msg_vars) {

	const char *client_ip = valueof(msg_vars, "client_ip");
	const char *sensor_uuid = valueof(msg_vars, "sensor_uuid");
	const char *topic = valueof(msg_vars, "topic");
	struct topic_s *topic_handler = NULL;
	json_t *client_enrichment = NULL;
	
	pthread_rwlock_rdlock(&rb_config->database.rwlock);
	topic_handler = topics_db_get_topic(rb_config->database.topics_db,topic);

	if(topic_handler) {
		client_enrichment = json_object_get(
		        rb_config->database.uuid_enrichment,sensor_uuid);
		
		if (NULL != client_enrichment) {
			pthread_mutex_lock(&rb_config->database.uuid_enrichment_mutex);
			json_incref(client_enrichment);
			pthread_mutex_unlock(&rb_config->database.uuid_enrichment_mutex);
		}
	}
	pthread_rwlock_unlock(&rb_config->database.rwlock);

	if (NULL == topic_handler) {
		rdlog(LOG_ERR,"Invalid topic %s received from client %s",
			topic,client_ip);
		return NULL;
	} else if (NULL == client_enrichment) {
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
		goto client_enrichment_err;
	}

	rd_kafka_msg_q_init(&sess->msg_queue);
	sess->client_enrichment = client_enrichment;
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

client_enrichment_err:
	pthread_mutex_lock(&rb_config->database.uuid_enrichment_mutex);
	json_decref(client_enrichment);
	pthread_mutex_unlock(&rb_config->database.uuid_enrichment_mutex);

	return NULL;
}

static void free_rb_session(struct rb_config *rb_config,struct rb_session *sess) {
	yajl_free(sess->handler);
	yajl_gen_free(sess->gen);

	pthread_mutex_lock(&rb_config->database.uuid_enrichment_mutex);
	json_decref(sess->client_enrichment);
	pthread_mutex_unlock(&rb_config->database.uuid_enrichment_mutex);

	topic_decref(sess->topic_handler);

	free(sess);
}

/*
 *  MAIN ENTRY POINT
 */

static void process_rb_buffer(const char *buffer, size_t bsize,
            const keyval_list_t *msg_vars, struct rb_opaque *opaque,
            struct rb_session **sessionp) {

	// json_error_t err;
	// struct rb_database *db = &opaque->rb_config->database;
	// /* @TODO const */ json_t *uuid_enrichment_entry = NULL;
	// char *ret = NULL;
	struct rb_session *session = NULL;
	const unsigned char *in_iterator = (const unsigned char *)buffer;

	assert(sessionp);

	if(NULL == *sessionp) {
		/* First call */
		*sessionp = new_rb_session(opaque->rb_config,msg_vars);
		if(NULL == *sessionp) {
			return;
		}
	} else if (0 == bsize) {
		/* Last call, need to free session */
		free_rb_session(opaque->rb_config,*sessionp);
		*sessionp = NULL;
		return;
	}

	session = *sessionp;

	yajl_status stat = yajl_parse(session->handler, in_iterator, bsize);

	if (stat != yajl_status_ok) {
		/// @TODO improve this!
		unsigned char * str = yajl_get_error(session->handler, 1, in_iterator, bsize);
		fprintf(stderr, "%s", (const char *) str);
		yajl_free_error(session->handler, str);
	}
}

void rb_decode(char *buffer, size_t buf_size,
               const keyval_list_t *list,
               void *_listener_callback_opaque,
               void **vsessionp) {
	struct rb_opaque *rb_opaque = _listener_callback_opaque;
	struct rb_session **sessionp = (struct rb_session **)vsessionp;
	/// Helper pointer to simulate streaming behavior
	struct rb_session *my_session = NULL;

#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == rb_opaque->magic);
#endif

	if(NULL == vsessionp) {
		// Simulate an active
		sessionp = &my_session;
	}

	process_rb_buffer(buffer, buf_size, list, rb_opaque,sessionp);

	if(buffer) {
		/* It was not the last call, designed to free session */
		const size_t n_messages = rd_kafka_msg_q_size(&(*sessionp)->msg_queue);
		rd_kafka_message_t msgs[n_messages];
		rd_kafka_msg_q_dump(&(*sessionp)->msg_queue,msgs);

		if((*sessionp)->topic) {
			produce_or_free(rb_opaque,(*sessionp)->topic_handler, msgs, n_messages);
		}
	}

	if(NULL == vsessionp) {
		// Simulate last call that will free my_session
		process_rb_buffer(NULL,0,list,rb_opaque, sessionp);
	}
}
