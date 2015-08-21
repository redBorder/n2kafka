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

static const char RB_HTTP2K_CONFIG_KEY[] = "rb_http2k_config";
static const char RB_SENSOR_UUID_ENRICHMENT_KEY[] = "uuids";
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

static int parse_per_uuid_opaque_config(struct rb_opaque *opaque,json_t *config,char *err,size_t errsize) {
	assert(opaque);
	assert(config);
	assert(err);
	json_error_t jerr;

	const int json_unpack_rc = json_unpack_ex(config,&jerr,0,"{s?O}",
		RB_SENSOR_UUID_ENRICHMENT_KEY,&opaque->rb_config->database.rwlock);

	if(0!=json_unpack_rc)
		snprintf(err,errsize,"%s",jerr.text);
	
	return json_unpack_rc;
}

int rb_opaque_creator(json_t *config,void **_opaque,char *err,size_t errsize){
	assert(_opaque);
	char errbuf[BUFSIZ];

	struct rb_opaque *opaque = (*_opaque) = calloc(1,sizeof(*opaque));
	if(NULL == opaque) {
		snprintf(err,errsize,"Can't alloc RB_HTTP2K opaque (out of memory?)");
		return -1;
	}

#ifdef RB_OPAQUE_MAGIC
	opaque->magic = RB_OPAQUE_MAGIC;
#endif

	opaque->rb_config = &global_config.rb;

	const int rwlock_init_rc = pthread_rwlock_init(&opaque->rb_config->database.rwlock,NULL);
	if(rwlock_init_rc != 0) {
		strerror_r(errno, errbuf, sizeof(errbuf));
		rdlog(LOG_ERR,"Can't start rwlock: %s",errbuf);
		goto _err;
	}

	const int per_sensor_uuid_enrichment_rc = parse_per_uuid_opaque_config(
		opaque,config,err,errsize);
	if(per_sensor_uuid_enrichment_rc != 0){
		goto err_rwlock;
	}

	/// @TODO move global_config to static allocated buffer
	opaque->rb_config = &global_config.rb;

	return 0;

err_rwlock:
	pthread_rwlock_destroy(&opaque->rb_config->database.rwlock);
_err:
	free(opaque);
	*_opaque = NULL;
	return -1;
}

int rb_opaque_reload(json_t *config,void *_opaque) {
	struct rb_opaque *opaque = _opaque;
	assert(opaque);
	assert(config);
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == opaque->magic);
#endif

	json_error_t jerr;

	json_t *new_db = NULL;
	const int unpack_rc = json_unpack_ex(config,&jerr,0,"{s?O}",
		RB_SENSOR_UUID_ENRICHMENT_KEY,&new_db);

	if(0 != unpack_rc) {
		rdlog(LOG_ERR,"Can't unpack rb_http2k config: %s",jerr.text);
		return -1;
	}

	if(!json_is_object(new_db)){
		rdlog(LOG_ERR,"Expected %s to be a json object",RB_SENSOR_UUID_ENRICHMENT_KEY);
		return -1;
	}

	new_db = json_deep_copy(new_db);

	pthread_rwlock_wrlock(&opaque->rb_config->database.rwlock);
	json_t *old_db = opaque->rb_config->database.uuid_enrichment;
	opaque->rb_config->database.uuid_enrichment = new_db;
	pthread_rwlock_unlock(&opaque->rb_config->database.rwlock);

	if(old_db)
		json_decref(old_db);

	return 0;
}

void rb_opaque_done(void *_opaque){
	assert(_opaque);

	struct rb_opaque *opaque = _opaque;
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == opaque->magic);
#endif
	pthread_rwlock_destroy(&opaque->rb_config->database.rwlock);
	if(opaque->rb_config->database.uuid_enrichment)
		json_decref(opaque->rb_config->database.uuid_enrichment);
	free(opaque);
}

int parse_rb_config(void *_db,const struct json_t *config,
			char *err __attribute__((unused)),size_t err_size __attribute__((unused))){
	assert(_db);

	struct rb_opaque dummy_opaque = {
#ifdef RB_OPAQUE_MAGIC
		.magic = RB_OPAQUE_MAGIC,
#endif
		/// @TODO quick hack, parse properly
		.rb_config = (struct rb_config *)_db
	};

	json_t *aux = json_deep_copy(config);
	rb_opaque_reload(aux,&dummy_opaque);
	json_decref(aux);

	return 0;
}

void free_valid_rb_database(struct rb_database *db){
	if(db && db->uuid_enrichment)
		json_decref(db->uuid_enrichment);
}

/* 
    ENRICHMENT
*/

static void enrich_rb_json(json_t *json, /* TODO const */ json_t *enrichment_data){
	assert(json);
	assert(enrichment_data);

	json_object_update_missing_copy(json,enrichment_data);
}

static char *process_rb_buffer(const char *buffer,size_t bsize,
                                 struct rb_opaque *opaque){
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
	uuid_enrichment_entry = json_object_get(db->uuid_enrichment,sensor_uuid);
	if( NULL != uuid_enrichment_entry) {
		enrich_rb_json(json,uuid_enrichment_entry);
	}
	pthread_rwlock_unlock(&opaque->rb_config->database.rwlock);

	ret = json_dumps(json,JSON_COMPACT|JSON_ENSURE_ASCII);

err:
	if(json)
		json_decref(json);
	return ret;
}

void rb_decode(char *buffer,size_t buf_size,
	                        const char *topic __attribute__((unused)),
	                        void *_listener_callback_opaque) {

	struct rb_opaque *rb_opaque = _listener_callback_opaque;
#ifdef RB_OPAQUE_MAGIC
	assert(RB_OPAQUE_MAGIC == rb_opaque->magic);
#endif

	char *to_send = process_rb_buffer(buffer,buf_size,rb_opaque);
	free(buffer);

	if(to_send) {
		send_to_kafka(to_send,strlen(to_send),
			RD_KAFKA_MSG_F_FREE,NULL);
	}
}
