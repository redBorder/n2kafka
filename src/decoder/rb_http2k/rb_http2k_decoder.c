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
#include "rb_http2k_parser.h"
#include "util/rb_mac.h"
#include "util/kafka.h"
#include "engine/global_config.h"
#include "util/rb_json.h"
#include "util/topic_database.h"
#include "util/kafka_message_list.h"
#include "util/util.h"

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

int rb_opaque_reload(json_t *config, void *opaque) {
	/* Do nothing, since this decoder does not save anything per-listener
	   information */
	(void)config;
	(void)opaque;
	return 0;
}

int rb_decoder_reload(void *vrb_config, const json_t *config) {
	int rc = 0;
	struct rb_config *rb_config = vrb_config;
	struct topics_db *topics_db = NULL;
	json_t *uuid_enrichment = NULL;

	assert(rb_config);
	assert_rb_config(rb_config);
	assert(config);

	json_t *my_config = json_deep_copy(config);

	const int per_sensor_enrichment_rc
		= parse_per_uuid_opaque_config(my_config, &uuid_enrichment);
	if (per_sensor_enrichment_rc != 0 || NULL == uuid_enrichment) {
		rc = -1;
		goto err;
	}

	topics_db = topics_db_new();

	const int topic_list_rc = parse_topic_list_config(my_config,
								topics_db);
	if (topic_list_rc != 0) {
		rc = -1;
		goto err;
	}

	pthread_rwlock_wrlock(&rb_config->database.rwlock);
	swap_ptrs(uuid_enrichment,rb_config->database.uuid_enrichment);
	swap_ptrs(topics_db,rb_config->database.topics_db);
	pthread_rwlock_unlock(&rb_config->database.rwlock);

err:
	if (topics_db) {
		topics_db_done(topics_db);
	}

	if (uuid_enrichment) {
		json_decref(uuid_enrichment);
	}

	json_decref(my_config);

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

int parse_rb_config(void *vconfig, const struct json_t *config) {
	struct rb_config *rb_config = vconfig;

	assert(vconfig);
	assert(config);

	if (only_stdout_output()) {
		rdlog(LOG_ERR, "Can't use rb_http2k decoder if not kafka brokers configured.");
		return -1;
	}

#ifdef RB_CONFIG_MAGIC
	rb_config->magic = RB_CONFIG_MAGIC;
#endif // RB_CONFIG_MAGIC
	const int rc = init_rb_database(&rb_config->database);

	if (rc == 0) {
		/// @TODO error treatment
		json_t *aux = json_deep_copy(config);
		rb_decoder_reload(rb_config, aux);
		json_decref(aux);
	}

	return rc;
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
