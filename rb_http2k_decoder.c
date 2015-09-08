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
	const void *keydata,size_t keylen,int32_t partition_cnt,
	void *rkt_opaque,void *msg_opaque);

static int32_t (mac_partitioner) (const rd_kafka_topic_t *rkt,
	const void *keydata,size_t keylen,int32_t partition_cnt,
	void *rkt_opaque,void *msg_opaque);

enum partitioner_algorithm {
	none,mac,
};

struct {
	enum partitioner_algorithm algoritm;
	const char *name;
	partitioner_cb partitioner;
} partitioner_algorithm_list[] = {
	{mac,"mac",mac_partitioner},
};

struct topic_s{
#ifdef TOPIC_S_MAGIC
	uint64_t magic;
#endif
	rd_kafka_topic_t *rkt;
	const char *topic_name;

	rd_avl_node_t avl_node;
	TAILQ_ENTRY(topic_s) list_node;

	const char *partition_key;
	enum partitioner_algorithm partitioner_algorithm;
};

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

static int32_t mac_partitioner (const rd_kafka_topic_t *rkt,
		const void *keydata,size_t keylen,int32_t partition_cnt,
		void *rkt_opaque,void *msg_opaque) {
	size_t toks=0;
	uint64_t intmac = 0;
	char mac_key[sizeof("00:00:00:00:00:00")];
	
	if(keylen != strlen("00:00:00:00:00:00")) {
		rdlog(LOG_WARNING,"Invalid mac %.*s len",(int)keylen,(const char *)keydata);
		goto fallback_behavior;
	}

	mac_key[0]='\0';
	strncat(mac_key,(const char *)keydata,sizeof(mac_key)-1);

	for(toks=1;toks<6;++toks) {
		const size_t semicolon_pos = 3*toks-1;
		if(':' != mac_key[semicolon_pos]) {
			rdlog(LOG_WARNING,"Invalid mac %.*s (it does not have ':' in char %zu.",
				(int)keylen,mac_key,semicolon_pos);
			goto fallback_behavior;
		}
	}

	for(toks=0;toks<6;++toks) {
		char *endptr = NULL;
		intmac = (intmac<<8) + strtoul(&mac_key[3*toks],&endptr,16);
		/// The key should end with '"' in json format
		if((toks < 5 && *endptr != ':') || (toks==5 && *endptr!='\0')) {
			rdlog(LOG_WARNING,"Invalid mac %.*s, unexpected %c end of %zu token",
				(int)keylen,mac_key,*endptr,toks);
			goto fallback_behavior;

		}

		if(endptr != mac_key + 3*(toks+1)-1) {
			rdlog(LOG_WARNING,"Invalid mac %.*s, unexpected token length at %zu token",
				(int)keylen,mac_key,toks);
			goto fallback_behavior;
		}
	}

	return intmac % (uint32_t)partition_cnt;

fallback_behavior:
	return rd_kafka_msg_partitioner_random(rkt,keydata,keylen,partition_cnt,
		rkt_opaque,msg_opaque);
}

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

static partitioner_cb partitioner_of_name(const char *name) {
	size_t i=0;

	assert(name);

	for(i=0;i<RD_ARRAYSIZE(partitioner_algorithm_list);++i) {
		if(0==strcmp(partitioner_algorithm_list[i].name,name)) {
			return partitioner_algorithm_list[i].partitioner;
		}
	}

	return NULL;
}

static int parse_topic_list_config(json_t *config,topics_list *new_topics_list,rd_avl_t *new_topics_db,void **new_topics_memory) {
	const char *key;
	json_t *value;
	json_error_t jerr;
	json_t *topic_list = NULL;
	int pass=0;
	char *strings_buffer=NULL;
	struct topic_s *topics=NULL;

	assert(config);
	assert(new_topics_list);

	const int json_unpack_rc = json_unpack_ex(config,&jerr,0,"{s:o}",
		RB_TOPICS_KEY,&topic_list);

	if(0!=json_unpack_rc) {
		rdlog(LOG_ERR,"%s",jerr.text);
		return json_unpack_rc;
	}

	if(!json_is_object(topic_list)) {
		rdlog(LOG_ERR,"%s is not an object",RB_TOPICS_KEY);
		return -1;
	}

	const size_t array_size = json_object_size(topic_list);
	if(0 ==array_size) {
		rdlog(LOG_ERR,"%s has no childs",RB_TOPICS_KEY);
		return -1;
	}

	for(pass=0;pass<2;pass++) {
		size_t char_array_pos = 0;
		size_t idx = 0;


		json_object_foreach(topic_list, key, value) {
			const char *partition_key=NULL,*partition_algo=NULL;
			const char *topic_name = key;
			const size_t topic_len = strlen(topic_name);

			if(!json_is_object(value)) {
				if(pass == 0) {
					rdlog(LOG_ERR,"Topic %s is not an object. Discarding.",topic_name);
				}
				continue;
			}

			const int topic_unpack_rc = json_unpack_ex(value,&jerr,0,"{s?s,s?s}",
				RB_TOPIC_PARTITIONER_KEY,&partition_key,
				RB_TOPIC_PARTITIONER_ALGORITHM_KEY,&partition_algo);

			if(0!=topic_unpack_rc) {
				if(pass==0) {
					rdlog(LOG_ERR,"Can't extract information of topic %s (%s). Discarding.",
						topic_name,jerr.text);
				}
				continue;
			}

			const size_t partition_key_len = partition_key?strlen(partition_key):0;

			if(1==pass) {
				char *topics_curr_pos = strings_buffer + char_array_pos;

	#ifdef TOPIC_S_MAGIC
				topics[idx].magic = TOPIC_S_MAGIC;
	#endif
				topics[idx].topic_name = strncpy(topics_curr_pos,topic_name,
					topic_len+1);
				topics_curr_pos += topic_len+1;
				if(partition_key_len) {
					topics[idx].partition_key = strncpy(topics_curr_pos,
						partition_key,partition_key_len+1);
				}

				RD_AVL_INSERT(new_topics_db,&topics[idx],avl_node);
				topic_list_push(new_topics_list,&topics[idx]);

				rd_kafka_topic_conf_t *myconf = rd_kafka_topic_conf_dup(global_config.kafka_topic_conf);
				
				if(NULL != partition_algo) {
					partitioner_cb partitioner = partition_algo != NULL ? 
						partitioner_of_name(partition_algo) : NULL;
					if(NULL != partitioner) {
						rd_kafka_topic_conf_set_partitioner_cb(myconf,partitioner);
					} else {
						rdlog(LOG_ERR,"Can't found partitioner algorithm %s",partition_algo);
					}
				}
				
				topics[idx].rkt = rd_kafka_topic_new(global_config.rk, topics[idx].topic_name, myconf);

				if(NULL == topics[idx].rkt) {
					char buf[BUFSIZ];
					strerror_r(errno,buf,sizeof(buf));
					rdlog(LOG_ERR,"Can't create topic %s: %s",topic_name,buf);
				}

			}

			char_array_pos += topic_len+1 + (partition_key_len?partition_key_len+1:0);
			idx++;
		}

		if(0 == pass) {
			const size_t array_memsize = idx*sizeof(struct topic_s);
			const size_t needed_memory = array_memsize + char_array_pos;
			*new_topics_memory = calloc(1,needed_memory);
			topics = *new_topics_memory;
			strings_buffer = (char *)topics + array_memsize;
			if(NULL == *new_topics_memory) {
				rdlog(LOG_ERR,"Can't allocate topics (out of memory?)");
				return -1;
			}
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

int rb_opaque_creator(json_t *config __attribute__((unused)),void **_opaque){
	assert(_opaque);

	struct rb_opaque *opaque = (*_opaque) = calloc(1,sizeof(*opaque));
	if(NULL == opaque) {
		rdlog(LOG_ERR,"Can't alloc RB_HTTP2K opaque (out of memory?)");
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

int parse_rb_config(void *void_db,const struct json_t *config){
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
	if(db && db->uuid_enrichment) {
		json_decref(db->uuid_enrichment);
	}

	free_topics(&db->topics.list);
	free(db->topics_memory);

	if(db->topics.topics) {
		rd_avl_destroy(db->topics.topics);
	}

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

static void produce_or_free(struct topic_s *topic,char *buf,size_t bufsize,
        const char *key,size_t keylen,void *opaque) {

	assert(topic);

	const int produce_ret = rd_kafka_produce(topic->rkt,RD_KAFKA_PARTITION_UA,
		RD_KAFKA_MSG_F_FREE,buf,bufsize,key,keylen,opaque);

	if(produce_ret != 0) {
		rdlog(LOG_ERR,"Can't produce to topic %s: %s",
			topic->topic_name,
			rd_kafka_err2str(rd_kafka_errno2err(errno)));
	}

}

static void process_rb_buffer(const char *buffer,size_t bsize,
        struct rb_opaque *opaque,const char *topic,const char *client_ip) {
	json_error_t err;
	struct rb_database *db = &opaque->rb_config->database;
	/* @TODO const */ json_t *uuid_enrichment_entry = NULL;
	const char *sensor_uuid = NULL;
	char *ret = NULL;
	assert(buffer);
	assert(bsize);

	json_t *json = json_loadb(buffer,bsize,0,&err);
	if(NULL == json){
		rdlog(LOG_ERR,"Error decoding RB JSON (%s) of source %s, line %d column %d: %s",
			buffer,client_ip,err.line,err.column,err.text);
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
		const char *key = NULL;
		if(NULL != topic_handler->partition_key) {
			const int unpack_partitioner_key_rc = json_unpack_ex(json,&err,0,"{s:s}",
				topic_handler->partition_key,&key);
			if(0 != unpack_partitioner_key_rc) {
				rdlog(LOG_ERR,"Can't unpack %s key from message of %s",
					topic_handler->partition_key,client_ip);
				rdlog(LOG_ERR,"Message was:\n%.*s",(int)bsize,buffer);
			}
		}
		uuid_enrichment_entry = json_object_get(db->uuid_enrichment,sensor_uuid);
		if( NULL != uuid_enrichment_entry) {
			enrich_rb_json(json,uuid_enrichment_entry);
			ret = json_dumps(json,JSON_COMPACT|JSON_ENSURE_ASCII);
			if(ret == NULL) {
				rdlog(LOG_ERR,"Can't create json dump (out of memory?)");
			} else {
				produce_or_free(topic_handler,ret,strlen(ret),strstr(ret,key),
					key?strlen(key):0,NULL);
			}
		}
	}

	pthread_rwlock_unlock(&opaque->rb_config->database.rwlock);

err:
	if(json)
		json_decref(json);
}

void rb_decode(char *buffer,size_t buf_size,
	                        const char *topic,const char *client_ip,
	                        void *_listener_callback_opaque) {

	struct rb_opaque *rb_opaque = _listener_callback_opaque;
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == rb_opaque->magic);
#endif

	process_rb_buffer(buffer,buf_size,rb_opaque,topic,client_ip);
	free(buffer);
}
