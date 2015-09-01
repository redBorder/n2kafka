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

#include <librd/rdlog.h>
#include <assert.h>
#include <jansson.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <librdkafka/rdkafka.h>

#define topic_list_init(l) TAILQ_INIT(l)
#define topic_list_push(l,e) TAILQ_INSERT_TAIL(l,e,list_node);

static const char RB_HTTP2K_CONFIG_KEY[] = "rb_http2k_config";
static const char RB_SENSOR_UUID_ENRICHMENT_KEY[] = "uuids";
static const char RB_TOPICS_KEY[] = "topics";
static const char RB_SENSOR_UUID_KEY[] = "uuid";

void init_rb_database(struct rb_database *db){
	memset(db,0,sizeof(db));
	pthread_rwlock_init(&db->rwlock,0);
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

static int parse_per_uuid_opaque_config(json_t *config,json_t **uuid_enrichment) {
	assert(uuid_enrichment);
	assert(config);
	json_error_t jerr;

	const int json_unpack_rc = json_unpack_ex(config,&jerr,0,"{s:O}",
		RB_SENSOR_UUID_ENRICHMENT_KEY,uuid_enrichment);

	if(0!=json_unpack_rc) {
		rdlog(LOG_ERR,"Can't parse valid uuid: %s",jerr.text);
	}
	
	return json_unpack_rc;
}

#if JANSSON_VERSION_HEX < 0x020700
static size_t json_string_length(const json_t *value) {
	const char *str = json_string_value(value);
	return str?strlen(str):0;
}
#endif

static int parse_topic_list_config(json_t *config,topics_list *new_topics_list,rd_avl_t *new_topics_db,void **new_topics_memory) {
	size_t idx;
	json_t *value;
	json_error_t jerr;
	json_t *topic_list = NULL;
	size_t char_array_len = 0;
	size_t char_array_pos = 0;

	assert(config);
	assert(new_topics_list);

	const int json_unpack_rc = json_unpack_ex(config,&jerr,0,"{s:o}",
		RB_TOPICS_KEY,&topic_list);

	if(0!=json_unpack_rc) {
		rdlog(LOG_ERR,"%s",jerr.text);
		return json_unpack_rc;
	}

	if(!json_is_array(topic_list)) {
		rdlog(LOG_ERR,"%s is not an array",RB_TOPICS_KEY);
		return -1;
	}

	const size_t array_size = json_array_size(topic_list);
	if(0 ==array_size) {
		rdlog(LOG_ERR,"%s array size is 0",RB_TOPICS_KEY);
		return -1;
	}

	json_array_foreach(topic_list, idx, value) {
		if(value && json_is_string(value)) {
			char_array_len += json_string_length(value) + 1;
		}
	}

	const size_t array_memsize = array_size*sizeof(struct topic_s);
	const size_t needed_memory = array_memsize + char_array_len;
	*new_topics_memory = calloc(1,needed_memory);
	struct topic_s *my_topics_memory = *new_topics_memory;
	char *topics = (char *)my_topics_memory + array_memsize;
	if(NULL == *new_topics_memory) {
		rdlog(LOG_ERR,"Can't allocate topics (out of memory?)");
		return -1;
	}

	json_array_foreach(topic_list, idx, value) {
		const size_t topic_len = json_string_length(value);
		const char *topic_name = json_string_value(value);

		if(NULL != topic_name) {
			char *topics_curr_pos = topics + char_array_pos;

#ifdef TOPIC_S_MAGIC
			my_topics_memory[idx].magic = TOPIC_S_MAGIC;
#endif
			my_topics_memory[idx].topic_name = strncpy(topics_curr_pos,topic_name,topic_len+1);

			RD_AVL_INSERT(new_topics_db,&my_topics_memory[idx],avl_node);
			topic_list_push(new_topics_list,&my_topics_memory[idx]);

			rd_kafka_topic_conf_t *myconf = rd_kafka_topic_conf_dup(global_config.kafka_topic_conf);
			my_topics_memory[idx].rkt = rd_kafka_topic_new(global_config.rk, my_topics_memory[idx].topic_name, myconf);

			if(NULL == my_topics_memory[idx].rkt) {
				char buf[BUFSIZ];
				strerror_r(errno,buf,sizeof(buf));
				rdlog(LOG_ERR,"Can't create topic %s: %s",topic_name,buf);
			} else {
				char_array_pos += topic_len+1;
			}
		} else {
			rdlog(LOG_ERR,"Can't parse rb_http2k topic number %zu.",idx);
		}
	}

	return 0;
}

static int topics_cmp(const void *_t1,const void *_t2) {
	const struct topic_s *t1 = _t1;
	const struct topic_s *t2 = _t2;

#ifdef TOPIC_S_MAGIC
	assert(TOPIC_S_MAGIC == t1->magic);
	assert(TOPIC_S_MAGIC == t2->magic);
#endif

	return strcmp(t1->topic_name,t2->topic_name);
}

int rb_opaque_creator(json_t *config __attribute__((unused)),void **_opaque,
			char *err,size_t errsize){
	assert(_opaque);

	struct rb_opaque *opaque = (*_opaque) = calloc(1,sizeof(*opaque));
	if(NULL == opaque) {
		snprintf(err,errsize,"Can't alloc RB_HTTP2K opaque (out of memory?)");
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
	struct topic_s *elm=NULL,*tmpelm=NULL;
	TAILQ_FOREACH_SAFE(elm,tmpelm,list,list_node) {
#ifdef TOPIC_S_MAGIC
		assert(TOPIC_S_MAGIC == elm->magic);
#endif
		rd_kafka_topic_destroy(elm->rkt);
	}
}

int rb_opaque_reload(json_t *config,void *_opaque) {
	int rc = 0;
	struct rb_opaque *opaque = _opaque;
	topics_list old_topics_list,new_topics_list;
	topics_db *old_topics_db=NULL,*new_topics_db=NULL;
	void *new_topics_memory=NULL,*old_topics_memory=NULL;
	json_t *new_uuid_enrichment=NULL,*old_uuid_enrichment=NULL;

	assert(opaque);
	assert(config);
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == opaque->magic);
#endif

	memset(&old_topics_list,0,sizeof(old_topics_list));
	memset(&new_topics_list,0,sizeof(new_topics_list));

	const int per_sensor_uuid_enrichment_rc = parse_per_uuid_opaque_config(config,&new_uuid_enrichment);
	if(per_sensor_uuid_enrichment_rc != 0 || NULL == new_uuid_enrichment){
		rc = -1;
		goto err;
	}

	new_topics_db = rd_avl_init(NULL,topics_cmp,0);
	if(NULL == new_topics_db) {
		rdlog(LOG_ERR,"Can't allocate new topics db (out of memory?)");
		goto err;
	}

	topic_list_init(&new_topics_list);

	const int topic_list_rc = parse_topic_list_config(config,&new_topics_list,
	                                        new_topics_db,&new_topics_memory);
	if(topic_list_rc != 0) {
		rc = -1;
		goto err;
	}

	pthread_rwlock_wrlock(&opaque->rb_config->database.rwlock);

	old_uuid_enrichment = opaque->rb_config->database.uuid_enrichment;
	opaque->rb_config->database.uuid_enrichment = new_uuid_enrichment;
	new_uuid_enrichment = NULL;

	old_topics_list = opaque->rb_config->database.topics.list;
	opaque->rb_config->database.topics.list = new_topics_list;
	TAILQ_INIT(&new_topics_list);

	old_topics_db = opaque->rb_config->database.topics.topics;
	opaque->rb_config->database.topics.topics = new_topics_db;
	new_topics_db = NULL;

	old_topics_memory = opaque->rb_config->database.topics_memory;
	opaque->rb_config->database.topics_memory = new_topics_memory;
	new_topics_memory = NULL;

	pthread_rwlock_unlock(&opaque->rb_config->database.rwlock);

err:
	if(new_topics_db) {
		rd_avl_destroy(new_topics_db);
	}

	if(old_topics_db) {
		rd_avl_destroy(old_topics_db);
	}

	free_topics(&old_topics_list);
	free_topics(&new_topics_list);

	if(new_topics_memory) {
		free(new_topics_memory);
	}

	if(old_topics_memory) {
		free(old_topics_memory);
	}

	if(old_uuid_enrichment) {
		json_decref(old_uuid_enrichment);
	}

	if(new_uuid_enrichment) {
		json_decref(new_uuid_enrichment);
	}

	return rc;
}

void rb_opaque_done(void *_opaque){
	assert(_opaque);

	struct rb_opaque *opaque = _opaque;
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == opaque->magic);
#endif

	free(opaque);
}

int parse_rb_config(void *void_db,const struct json_t *config,
			char *err __attribute__((unused)),size_t err_size __attribute__((unused))){
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

	if(only_stdout_output()) {
		rdlog(LOG_ERR,"Can't use rb_http2k decoder if not kafka brokers configured.");
		return -1;
	}

	const int rwlock_init_rc = pthread_rwlock_init(&rb_config->database.rwlock,NULL);
	if(rwlock_init_rc != 0) {
		strerror_r(errno, errbuf, sizeof(errbuf));
		rdlog(LOG_ERR,"Can't start rwlock: %s",errbuf);
	}

	json_t *aux = json_deep_copy(config);
	rb_opaque_reload(aux,&dummy_opaque);
	json_decref(aux);

	return 0;
}

void free_valid_rb_database(struct rb_database *db){
	if(db && db->uuid_enrichment)
		json_decref(db->uuid_enrichment);

	free_topics(&db->topics.list);
	free(db->topics_memory);

	rd_avl_destroy(db->topics.topics);

	pthread_rwlock_destroy(&db->rwlock);
}

/*
	PRE-PROCESSING VALIDATIONS
*/

int rb_http2k_validate_uuid(struct rb_database *db,const char *uuid) {
	pthread_rwlock_rdlock(&db->rwlock);
	const int ret = NULL != json_object_get(db->uuid_enrichment,uuid);
	pthread_rwlock_unlock(&db->rwlock);

	return ret;
}

static struct topic_s *get_topic_nl(struct rb_database *db,const char *topic) {
	char buf[strlen(topic)+1];
	strcpy(buf,topic);

	struct topic_s dumb_topic = {
#ifdef TOPIC_S_MAGIC
		.magic = TOPIC_S_MAGIC,
#endif
		.topic_name = buf
	};

	return RD_AVL_FIND_NODE_NL(db->topics.topics,&dumb_topic);
}

int rb_http2k_validate_topic(struct rb_database *db,const char *topic) {

	pthread_rwlock_rdlock(&db->rwlock);
	const int ret = NULL != get_topic_nl(db,topic);
	pthread_rwlock_unlock(&db->rwlock);

	return ret;
}


/* 
    ENRICHMENT
*/

static void enrich_rb_json(json_t *json, /* TODO const */ json_t *enrichment_data){
	assert(json);
	assert(enrichment_data);

	json_object_update_missing_copy(json,enrichment_data);
}

static void produce_or_free(rd_kafka_topic_t *rkt,char *buf,size_t bufsize,
                                                            void *opaque) {

	const int produce_ret = rd_kafka_produce(rkt,RD_KAFKA_PARTITION_UA,
		RD_KAFKA_MSG_F_FREE,buf,bufsize,NULL,0,opaque);

	if(produce_ret != 0) {
		rdlog(LOG_ERR,"Can't produce to topic %s: %s",
			rd_kafka_topic_name(rkt),
			rd_kafka_err2str(rd_kafka_errno2err(errno)));
	}

}

static void process_rb_buffer(const char *buffer,size_t bsize,
                struct rb_opaque *opaque,const char *topic) {
	json_error_t err;
	struct rb_database *db = &opaque->rb_config->database;
	/* @TODO const */ json_t *uuid_enrichment_entry = NULL;
	const char *sensor_uuid = NULL;
	char *ret = NULL;
	assert(buffer);
	assert(bsize);

	json_t *json = json_loadb(buffer,bsize,0,&err);
	if(NULL == json){
		rdlog(LOG_ERR,"Error decoding RB JSON (%s), line %d column %d: %s",
			buffer,err.line,err.column,err.text);
		goto err;
	}

	const int unpack_rc = json_unpack_ex(json,&err,0,"{s:s}",
		RB_SENSOR_UUID_KEY,&sensor_uuid);
	if(unpack_rc != 0) {
		rdlog(LOG_ERR,"Can't unpack %s: %s",RB_SENSOR_UUID_KEY,err.text);
		goto err;
	}

	if(NULL == sensor_uuid) {
		rdlog(LOG_ERR,"redBorder flow with no sensor uuid, discarding");
		goto err;
	}

	pthread_rwlock_rdlock(&opaque->rb_config->database.rwlock);

	struct topic_s *topic_handler = get_topic_nl(&opaque->rb_config->database,topic);
	if(NULL == topic_handler) {
		rdlog(LOG_ERR,"Can't produce to topic %s: topic not found",topic);
	} else {
		uuid_enrichment_entry = json_object_get(db->uuid_enrichment,sensor_uuid);
		if( NULL != uuid_enrichment_entry) {
			enrich_rb_json(json,uuid_enrichment_entry);
			ret = json_dumps(json,JSON_COMPACT|JSON_ENSURE_ASCII);
			if(ret == NULL) {
				rdlog(LOG_ERR,"Can't create json dump (out of memory?)");
			} else {
				produce_or_free(topic_handler->rkt,ret,strlen(ret),NULL);
			}
		}
	}

	pthread_rwlock_unlock(&opaque->rb_config->database.rwlock);

err:
	if(json)
		json_decref(json);
}

void rb_decode(char *buffer,size_t buf_size,
	                        const char *topic,
	                        void *_listener_callback_opaque) {

	struct rb_opaque *rb_opaque = _listener_callback_opaque;
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == rb_opaque->magic);
#endif

	process_rb_buffer(buffer,buf_size,rb_opaque,topic);
	free(buffer);
}
