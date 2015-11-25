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

#ifndef NDEBUG
#define TOPIC_S_MAGIC 0x01CA1C01CA1C01CAL
#endif

#define topic_list_init(l) TAILQ_INIT(l)
#define topic_list_push(l,e) TAILQ_INSERT_TAIL(l,e,list_node);

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

enum partitioner_algorithm {
	none, mac,
};

struct {
	enum partitioner_algorithm algoritm;
	const char *name;
	partitioner_cb partitioner;
} partitioner_algorithm_list[] = {
	{mac, "mac", mac_partitioner},
};

struct topic_s {
#ifdef TOPIC_S_MAGIC
	uint64_t magic;
#endif
	rd_kafka_topic_t *rkt;
	const char *topic_name;
	uint64_t refcnt;

	rd_avl_node_t avl_node;
	TAILQ_ENTRY(topic_s) list_node;

	const char *partition_key;
	enum partitioner_algorithm partitioner_algorithm;
};

static void topic_decref(struct topic_s *topic) {
	if(0==ATOMIC_OP(sub,fetch,&topic->refcnt,1)) {
		rd_kafka_topic_destroy(topic->rkt);
		free(topic);
	}
}

void init_rb_database(struct rb_database *db) {
	memset(db, 0, sizeof(*db));
	pthread_rwlock_init(&db->rwlock, 0);
}

#ifndef NDEBUG
#define RB_OPAQUE_MAGIC 0x0B0A3A1C0B0A3A1CL
#endif

struct rb_opaque {
#ifdef RB_OPAQUE_MAGIC
	uint64_t magic;
#endif

	struct rb_config *rb_config;
};

static int32_t mac_partitioner (const rd_kafka_topic_t *rkt,
                                const void *keydata, size_t keylen, int32_t partition_cnt,
                                void *rkt_opaque, void *msg_opaque) {
	size_t toks = 0;
	uint64_t intmac = 0;
	char mac_key[sizeof("00:00:00:00:00:00")];

	if (keylen != strlen("00:00:00:00:00:00")) {
		rdlog(LOG_WARNING, "Invalid mac %.*s len", (int)keylen, (const char *)keydata);
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
	                json_t **uuid_enrichment,json_t **dangerous_values) {
	json_error_t jerr;
	const char *uuid_key = NULL,*uuid_enrichment_key = NULL;
	json_t *uuid_value = NULL,*uuid_enrichment_value = NULL;

	assert(config);
	assert(uuid_enrichment);
	assert(dangerous_values);

	const int json_unpack_rc = json_unpack_ex(config, &jerr, 0, "{s:O}",
	                           RB_SENSOR_UUID_ENRICHMENT_KEY, uuid_enrichment);

	if (0 != json_unpack_rc) {
		rdlog(LOG_ERR,"Couldn't unpack %s key: %s",
			RB_SENSOR_UUID_ENRICHMENT_KEY,jerr.text);
	}

	*dangerous_values = json_object();
	if(NULL == *dangerous_values) {
		rdlog(LOG_ERR,"Couldn't create dangerous values table (out of memory?)");
	}

	json_object_foreach(*uuid_enrichment,uuid_key,uuid_value) {
		if(!json_is_object(uuid_value)) {
			continue;
		}

		json_object_foreach(uuid_value,uuid_enrichment_key,
		                            uuid_enrichment_value) {
			if(NULL == json_object_get(*dangerous_values,uuid_enrichment_key)) {
				// New empty object.
				json_t *new_object = json_null();
				if(NULL == new_object) {
					rdlog(LOG_ERR,"Couldn't allocate new dangerous values object");
					continue;
				}

				json_object_set_new_nocheck(*dangerous_values,
					uuid_enrichment_key,new_object);
			}
		}
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

/** @TODO each topic should have a refcnt, and live in sepparate callocs. This 
is currently a mess */
static int parse_topic_list_config(json_t *config, topics_list *new_topics_list,
                                   rd_avl_t *new_topics_db) {
	const char *key;
	json_t *value;
	json_error_t jerr;
	json_t *topic_list = NULL;
	int pass = 0;

	assert(config);
	assert(new_topics_list);

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
		struct topic_s *topic_s = NULL;
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

		const int topic_unpack_rc = json_unpack_ex(value, &jerr, 0, "{s?s%,s?s%}",
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


		rd_calloc_struct(&topic_s,sizeof(*topic_s),
			-1,topic_name,&topic_s->topic_name,
			partition_key_len,partition_key,&topic_s->partition_key,
			RD_MEM_END_TOKEN);

#ifdef TOPIC_S_MAGIC
		topic_s->magic = TOPIC_S_MAGIC;
#endif

		if (0 == partition_key_len) {
			topic_s->partition_key = NULL;
		}

		topic_s->rkt = rkt;
		topic_s->refcnt = 1;

		RD_AVL_INSERT(new_topics_db, topic_s, avl_node);
		topic_list_push(new_topics_list, topic_s);
	}

	return 0;
}

static int topics_cmp(const void *_t1, const void *_t2) {
	const struct topic_s *t1 = _t1;
	const struct topic_s *t2 = _t2;

#ifdef TOPIC_S_MAGIC
	assert(TOPIC_S_MAGIC == t1->magic);
	assert(TOPIC_S_MAGIC == t2->magic);
#endif

	return strcmp(t1->topic_name, t2->topic_name);
}

int rb_opaque_creator(json_t *config __attribute__((unused)), void **_opaque) {
	assert(_opaque);

	struct rb_opaque *opaque = (*_opaque) = calloc(1, sizeof(*opaque));
	if (NULL == opaque) {
		rdlog(LOG_ERR, "Can't alloc RB_HTTP2K opaque (out of memory?)");
		return -1;
	}

#ifdef RB_OPAQUE_MAGIC
	opaque->magic = RB_OPAQUE_MAGIC;
#endif

	/// @TODO move global_config to static allocated buffer
	opaque->rb_config = &global_config.rb;

	return 0;
}

static void free_topics(topics_list *list) {
	struct topic_s *elm = NULL, *tmpelm = NULL;
	TAILQ_FOREACH_SAFE(elm, tmpelm, list, list_node) {
#ifdef TOPIC_S_MAGIC
		assert(TOPIC_S_MAGIC == elm->magic);
#endif
		topic_decref(elm);
	}
}

int rb_opaque_reload(json_t *config, void *_opaque) {
	int rc = 0;
	struct rb_opaque *opaque = _opaque;
	topics_list old_topics_list, new_topics_list;
	topics_db *old_topics_db = NULL, *new_topics_db = NULL;
	json_t *new_uuid_enrichment = NULL, *old_uuid_enrichment = NULL;
	json_t *new_dangerous_values_table = NULL,
		*old_dangerous_values_table = NULL;

	assert(opaque);
	assert(config);
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == opaque->magic);
#endif

	memset(&old_topics_list, 0, sizeof(old_topics_list));
	memset(&new_topics_list, 0, sizeof(new_topics_list));

	const int per_sensor_uuid_enrichment_rc = parse_per_uuid_opaque_config(config,
	        &new_uuid_enrichment,&new_dangerous_values_table);
	if (per_sensor_uuid_enrichment_rc != 0 || NULL == new_uuid_enrichment) {
		rc = -1;
		goto err;
	}

	new_topics_db = rd_avl_init(NULL, topics_cmp, 0);
	if (NULL == new_topics_db) {
		rdlog(LOG_ERR, "Can't allocate new topics db (out of memory?)");
		goto err;
	}

	topic_list_init(&new_topics_list);

	const int topic_list_rc = parse_topic_list_config(config, &new_topics_list,
	                          new_topics_db);
	if (topic_list_rc != 0) {
		rc = -1;
		goto err;
	}

	pthread_rwlock_wrlock(&opaque->rb_config->database.rwlock);

	old_uuid_enrichment = opaque->rb_config->database.uuid_enrichment;
	opaque->rb_config->database.uuid_enrichment = new_uuid_enrichment;
	new_uuid_enrichment = NULL;

	old_dangerous_values_table = opaque->rb_config->database.dangerous_values;
	opaque->rb_config->database.dangerous_values = new_dangerous_values_table;
	new_dangerous_values_table = NULL;

	old_topics_list = opaque->rb_config->database.topics.list;
	opaque->rb_config->database.topics.list = new_topics_list;
	TAILQ_INIT(&new_topics_list);

	old_topics_db = opaque->rb_config->database.topics.topics;
	opaque->rb_config->database.topics.topics = new_topics_db;
	new_topics_db = NULL;

	pthread_rwlock_unlock(&opaque->rb_config->database.rwlock);

err:
	if (new_topics_db) {
		rd_avl_destroy(new_topics_db);
	}

	if (old_topics_db) {
		rd_avl_destroy(old_topics_db);
	}

	/** @TODO with yajl, we could delete a topic when it is in use. We should
	add a refcounter to this */
	free_topics(&old_topics_list);
	free_topics(&new_topics_list);

	if (old_uuid_enrichment) {
		json_decref(old_uuid_enrichment);
	}

	if (new_uuid_enrichment) {
		json_decref(new_uuid_enrichment);
	}

	if (new_dangerous_values_table) {
		json_decref(new_dangerous_values_table);
	}

	if (old_dangerous_values_table) {
		json_decref(old_dangerous_values_table);
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
	if (db && db->uuid_enrichment) {
		json_decref(db->uuid_enrichment);
	}

	free_topics(&db->topics.list);
	free(db->topics_memory);

	if (db->topics.topics) {
		rd_avl_destroy(db->topics.topics);
	}

	pthread_rwlock_destroy(&db->rwlock);
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

static struct topic_s *get_topic_nl(struct rb_database *db, const char *topic) {
	char buf[strlen(topic) + 1];
	strcpy(buf, topic);

	struct topic_s dumb_topic = {
#ifdef TOPIC_S_MAGIC
		.magic = TOPIC_S_MAGIC,
#endif
		.topic_name = buf
	};

	struct topic_s *ret = RD_AVL_FIND_NODE_NL(db->topics.topics, &dumb_topic);
	if(ret) {
		ATOMIC_OP(add,fetch,&ret->refcnt,1);
	}

	return ret;
}

int rb_http2k_validate_topic(struct rb_database *db, const char *topic) {

	pthread_rwlock_rdlock(&db->rwlock);
	const int ret = NULL != get_topic_nl(db, topic);
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

static void produce_or_free(struct topic_s *topic, char *buf, size_t bufsize,
                            const char *key, size_t keylen, void *opaque) {

	assert(topic);

	const int produce_ret = rd_kafka_produce(topic->rkt, RD_KAFKA_PARTITION_UA,
	                        RD_KAFKA_MSG_F_FREE, buf, bufsize, key, keylen, opaque);

	if (produce_ret != 0) {
		rdlog(LOG_ERR, "Can't produce to topic %s: %s",
		      topic->topic_name,
		      rd_kafka_err2str(rd_kafka_errno2err(errno)));
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
		/// current kafka message key
		const unsigned char *current_key;
		size_t current_key_length;
	} message;

	/// We are parsing value of kafka_partitioner_key
	int in_partition_key;

	/// Skip next parsing value
	int skip_value;
};

#define GEN_AND_RETURN(func)                         \
  {                                                  \
	yajl_gen_status __stat = func;                   \
	if (__stat == yajl_gen_generation_complete) {    \
		yajl_gen_reset(g, NULL);                     \
		__stat = func;                               \
	}                                                \
	return __stat == yajl_gen_status_ok; }

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

#define CHECK_PARTITIONER_KEY_IS(sess,expected_val,...) \
	if(expected_val != (sess)->in_partition_key) {          \
		rdlog(LOG_ERR,__VA_ARGS__);                         \
		/* Stop parsing */                                  \
		return 0;                                           \
	}

#define CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,...) \
	CHECK_PARTITIONER_KEY_IS(sess,0,__VA_ARGS__)

#define SESSION_IN_ROOT_OBJECT(sess,...)          \
	if((sess)->object_array_parsing_stack != 1) { \
		rdlog(LOG_WARNING,__VA_ARGS__);           \
		return 0;                                 \
	}                                             \

/// Checks that session is in an object
#define CHECK_IN_OBJECT(sess,...)                 \
	if((sess)->object_array_parsing_stack != 0) { \
		rdlog(LOG_WARNING,__VA_ARGS__);           \
		return 0;                                 \
	}

static int rb_parse_null(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"%s as partition key","null");
	GEN_OR_SKIP(sess,yajl_gen_null(g));
}

static int rb_parse_boolean(void * ctx, int boolean)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"%s as partition key",
		boolean?"true":"false");
	GEN_OR_SKIP(sess,yajl_gen_bool(g, boolean));
}

static int rb_parse_number(void * ctx, const char * s, size_t l)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"%.*s as partition key",
		(int)l,s);
	GEN_OR_SKIP(sess,yajl_gen_number(g, s, l));
}

static int rb_parse_string(void * ctx, const unsigned char * stringVal,
                           size_t stringLen)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;

	if(sess->in_partition_key) {
		if(sess->message.current_key) {
			rdlog(LOG_ERR,"Partition key already present (%s key twice?)",
				sess->kafka_partitioner_key);
			return 0;
		}

		size_t curr_gen_len = 0;
		sess->message.current_key = NULL;
		const int get_buf_rc = yajl_gen_get_buf(g,
                                  &sess->message.current_key,
                                  &curr_gen_len);

		if(get_buf_rc != yajl_gen_status_ok) {
			/// @TODO manage this
		}

		// Message key will be the next stuff printed.
		sess->message.current_key += strlen(":\"") + curr_gen_len;
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

	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,
		"Searching for kafka partition key while scanning json key %.*s",
		(int)stringLen,(const char *)stringVal);

	if(sess->object_array_parsing_stack > 1) {
		/// We are not in root object. Should we print?
		if(sess->skip_value) {
			return 0;
		} else {
			GEN_AND_RETURN(yajl_gen_string(g, stringVal, stringLen));
		}
	} else {
		/// We are in the root object
		if (sess->skip_value) {
			/* Just a check */
			rdlog(LOG_ERR,"Received unexpected key when ignoring value");
			return -1;
		}

		if (0 == strncmp(sess->kafka_partitioner_key,(const char *)stringVal,
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
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"Object as partitioner key");

	++sess->object_array_parsing_stack;

	GEN_OR_SKIP(sess,yajl_gen_map_open(g));
}

static int rb_parse_end_map(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;
	if(0 == sess->object_array_parsing_stack) {
		rdlog(LOG_WARNING,"Discarding parsing because closing an unopen JSON");
		return 0;
	}
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"Object closing as partitioner key");

	--sess->object_array_parsing_stack;
	if(0 == sess->object_array_parsing_stack) {
		/* Ending message, we need to add enrichment values */
		gen_jansson_object(g,sess->client_enrichment);

		const unsigned char * buf;
		size_t len;
		yajl_gen_map_close(g);
		yajl_gen_get_buf(sess->gen, &buf, &len);

		/// @TODO do not copy, steal the buffer!
		char *send_buffer = strdup((const char *)buf);
		if(NULL == send_buffer) {
			rdlog(LOG_ERR,"Unable to duplicate buffer");
			return 0;
		}

		// Key in duplicated buffer
		const char *key = NULL;
		if(sess->message.current_key) {
			const long int message_key_offset = sess->message.current_key - buf;
			key = send_buffer + message_key_offset;
			sess->message.current_key = NULL;
		}

		/// @TODO send outside, in order to be able to test!
	
		produce_or_free(sess->topic_handler, send_buffer, len, key, 
			sess->message.current_key_length, NULL);
		
		yajl_gen_clear(sess->gen);
		return 1;
	} else {
		GEN_OR_SKIP(sess,yajl_gen_map_close(g));
	}
}

static int rb_parse_start_array(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;
	CHECK_IN_OBJECT(sess,"Root object starts with an array.");
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"array start as partition key");

	++sess->object_array_parsing_stack;
	GEN_OR_SKIP(sess,yajl_gen_array_open(g));
}

static int rb_parse_end_array(void * ctx)
{
	struct rb_session *sess = ctx;
	yajl_gen g = sess->gen;
	--sess->object_array_parsing_stack;
	CHECK_NOT_EXPECTING_PARTITIONER_KEY(sess,"array en as partition key");
	CHECK_IN_OBJECT(sess,"Root object end with an array.");
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
	const char *kafka_partition_key = NULL;
	json_t *client_enrichment = NULL;

	pthread_rwlock_rdlock(&rb_config->database.rwlock);
	
	topic_handler = get_topic_nl(&rb_config->database,topic);
	if(topic_handler) {
		kafka_partition_key = topic_handler->partition_key?
			strdup(topic_handler->partition_key):NULL;

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

	struct rb_session *sess = NULL;
	rd_calloc_struct(&sess,sizeof(*sess),
		-1,client_ip,&sess->client_ip,
		-1,sensor_uuid,&sess->sensor_uuid,
		-1,topic,&sess->topic,
		kafka_partition_key?-1:0,kafka_partition_key,&sess->kafka_partitioner_key,
		RD_MEM_END_TOKEN);

	if(NULL == sess) {
		rdlog(LOG_CRIT, "Couldn't allocate sess pointer");
		goto client_enrichment_err;
	}

	sess->client_enrichment = client_enrichment;
	sess->topic_handler = topic_handler;

	if(NULL == kafka_partition_key) {
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

	assert(buffer);
	assert(bsize);
	assert(sessionp);

	if(NULL == *sessionp) {
		/* First call */
		*sessionp = new_rb_session(opaque->rb_config,msg_vars);
		if(NULL == *sessionp) {
			return;
		}
	} else if (0 == bsize) {
		/* TODO Last call, need to free sessionp */

	}

	session = *sessionp;

	// const char *client_ip = session->client_ip;
	// const char *sensor_uuid = session->sensor_uuid;
	// const char *topic = session->topic;

	yajl_status stat = yajl_parse(session->handler, in_iterator, bsize);

	if (stat != yajl_status_ok) {
		/// @TODO improve this!
		unsigned char * str = yajl_get_error(session->handler, 1, in_iterator, bsize);
		fprintf(stderr, "%s", (const char *) str);
		yajl_free_error(session->handler, str);
	}

	/*
err:
	if (json) {
		pthread_mutex_lock(&db->uuid_enrichment_mutex);
		json_decref(json);
		pthread_mutex_unlock(&db->uuid_enrichment_mutex);
	}
	*/
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
		sessionp = &my_session;
	}

	process_rb_buffer(buffer, buf_size, list, rb_opaque,sessionp);

	if(NULL == vsessionp) {
		process_rb_buffer(NULL,0,list,rb_opaque, sessionp);
	}
}
