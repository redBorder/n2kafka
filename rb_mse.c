/*
**
** Copyright (c) 2014, Eneo Tecnologia
** Author: Eugenio Perez <eupm90@gmail.com>
** All rights reserved.
**
** This program is free software; you can redistribute it and/or modify
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU General Public License Version 2 as
** published by the Free Software Foundation.  You may not use, modify or
** distribute this program under any other version of the GNU General
** Public License.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program; if not, write to the Free Software
** Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*/

#include "rb_mse.h"
#include "rb_mac.h"
#include "kafka.h"

#include <librd/rdlog.h>
#include <assert.h>
#include <jansson.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <librdkafka/rdkafka.h>

static const char MSE8_STREAMING_NOTIFICATION_KEY[] = "StreamingNotification";
static const char MSE8_LOCATION_KEY[] = "location";
static const char MSE8_MAC_ADDRESS_KEY[] = "macAddress";

static const char MSE_SUBSCRIPTION_NAME_KEY[] = "subscriptionName";
static const char MSE_DEVICE_ID_KEY[] = "deviceId";

static const char MSE10_NOTIFICATIONS_KEY[] = "notifications";

/* 
    VALIDATING MSE
*/

struct mse_data {
	uint64_t client_mac;
	const char *subscriptionName;
	/* private */
	const char *_client_mac;
};

void init_mse_database(struct mse_database *db){
	memset(db,0,sizeof(db));
	pthread_rwlock_init(&db->rwlock,0);
}

static int parse_sensor(json_t *sensor,json_t *streams_db){
	json_error_t err;
	const char *stream=NULL;
	const json_t *enrichment=NULL;

	const int unpack_rc = json_unpack_ex((json_t *)sensor,&err,0,
		"{s:s,s?o}","stream",&stream,"enrichment",&enrichment);

	if(unpack_rc != 0){
		rdlog(LOG_ERR,"Can't parse sensor (%s): %s",json_dumps(sensor,0),err.text);
		return -1;
	}

	if(stream == NULL){
		rdlog(LOG_ERR,"Can't parse sensor (%s): %s",json_dumps(sensor,0),"No \"stream\"");
		return -1;
	}

	json_t *_enrich = enrichment ? json_deep_copy(enrichment) : json_object();
	
	const int set_rc = json_object_set_new(streams_db,stream,_enrich);
	if(set_rc != 0){
		rdlog(LOG_ERR,"Can't set new MSE enrichment db entry (out of memory?)");
	}

	return 0;
}

int parse_mse_array(struct mse_database *db,const struct json_t *mse_array,char *err,size_t err_size){
	assert(db);

	json_t *value=NULL,*new_db = NULL;
	size_t _index;

	if(!json_is_array(mse_array)){
		snprintf(err,err_size,"Expected array");
		return -1;
	}

	new_db = json_object();
	if(!new_db){
		snprintf(err,err_size,"Can't create json object (out of memory?)");
		return -1;
	}

	json_array_foreach(mse_array,_index,value){
		parse_sensor(value,new_db);
	}

	pthread_rwlock_wrlock(&db->rwlock);
	json_t *old_db = db->root;
	db->root = new_db;
	pthread_rwlock_unlock(&db->rwlock);

	if(old_db)
		json_decref(old_db);

	return 0;
}

static json_t *mse_database_entry_copy(const char *subscriptionName,struct mse_database *db){
	assert(subscriptionName);
	assert(db);
	pthread_rwlock_rdlock(&db->rwlock);
	json_t *ret = json_object_get(db->root,subscriptionName);
	pthread_rwlock_unlock(&db->rwlock);
	return ret;
}

void free_valid_mse_database(struct mse_database *db){
	if(db && db->root)
		json_decref(db->root);
}

/* 
    ENRICHMENT
*/

static int is_mse8_message(const json_t *json){
	return NULL != json_object_get(json,MSE8_STREAMING_NOTIFICATION_KEY);
}

static int is_mse10_message(const json_t *json){
	/* If it has notification array, it is a mse10 flow */
	return NULL != json_object_get(json,MSE10_NOTIFICATIONS_KEY);
}

static int extract_mse8_rich_data(json_t *from,struct mse_data *to){
	json_error_t err;
	const char *macAddress=NULL;
	const int unpack_rc = json_unpack_ex(from, &err, 0, 
		"{s:{"         /* Streaming notification */
			"s:s,"     /* subscriptionName */
			"s:s,"     /* deviceId */
			"s:{"      /* location */
				"s:s"  /* macAddress */
			"}"
		"}}",
		MSE8_STREAMING_NOTIFICATION_KEY,
			MSE_SUBSCRIPTION_NAME_KEY,&to->subscriptionName,
			MSE_DEVICE_ID_KEY,&to->_client_mac,
			MSE8_LOCATION_KEY,
				MSE8_MAC_ADDRESS_KEY,&macAddress);

	if(unpack_rc < 0){
		rdlog(LOG_ERR,"Can't extract MSE8 rich data from (%s), line %d column %d: %s",
			err.source,err.line,err.column,err.text);
	}else{
		assert(to->_client_mac);
		assert(macAddress);

		if(0!=strcmp(to->_client_mac,macAddress)){
			rdlog(LOG_WARNING,"deviceId != macAddress: [%s]!=[%s]. Using deviceId",
				to->_client_mac,macAddress);
		}
	}

	return unpack_rc;
}

static int extract_mse10_rich_data(json_t *from,struct mse_data *to) {
	json_error_t err;

	const int unpack_rc = json_unpack_ex(from, &err, 0, 
		"{s:s,"  /* deviceId */
		"s:s}",  /* subscriptionName */
		MSE_DEVICE_ID_KEY,&to->client_mac,
		MSE_SUBSCRIPTION_NAME_KEY,&to->subscriptionName);

	return unpack_rc;
}

static int extract_mse_data(const char *buffer,json_t *json,struct mse_data *to){
	const int extract_rc =  
		is_mse8_message(json)  ? extract_mse8_rich_data(json,to)  :
		is_mse10_message(json) ? extract_mse10_rich_data(json,to) :
		({rdlog(LOG_ERR,"This is not an valid MSE JSON: %s",buffer);-1;});


	if(extract_rc < 0)
		return -1;

	to->client_mac = parse_mac(to->_client_mac);
	if(!valid_mac(to->client_mac)){
		rdlog(LOG_WARNING,"Can't found client mac in (%s), using random partitioner",
			buffer);
		to->client_mac = 0;
		return -1;
	}

	return 0;
}

static void enrich_mse_json(json_t *json,json_t *enrichment_data){
	assert(json);
	assert(enrichment_data);

	json_object_update_missing(json,enrichment_data);
}

static char *process_mse_buffer(char *buffer,size_t *bsize,struct mse_data *to,
                                 struct mse_database *db){
	assert(bsize);
	assert(to);

	json_error_t err;
	json_t *json = json_loadb(buffer,*bsize,0,&err);
	if(NULL == json){
		rdlog(LOG_ERR,"Error decoding MSE JSON (%s), line %d column %d: %s",
			buffer,err.line,err.column,err.text);
		goto err;
	}

	extract_mse_data(buffer,json,to);
	if(!to->subscriptionName){
		/* Does not have subscriptionName -> we should drop */
		goto err;
	}
	
	json_t *enrichment = mse_database_entry_copy(to->subscriptionName,db);
	if(db && !enrichment) {
		goto err;
	}
	
	if(enrichment){
		enrich_mse_json(json,enrichment);
		char *_buffer = json_dumps(json,JSON_COMPACT|JSON_ENSURE_ASCII);
		if(_buffer){
			free(buffer);
			buffer = _buffer;
			*bsize = strlen(buffer);
		}else{
			rdlog(LOG_ERR,"Can't dump JSON buffer (out of memory?)");
		}
	}

	to->_client_mac = NULL;
	json_decref(json);

	return buffer; /* @TODO If we change buffer, we have to modify bsize */

err:
	if(json)
		json_decref(json);
	*bsize = 0;
	free(buffer);
	return NULL;
}

void mse_decode(char *buffer,size_t buf_size,void *_listener_callback_opaque){
	struct mse_data data = {
		.client_mac = 0,
		._client_mac = NULL,
		.subscriptionName = NULL
	};

	struct mse_config *mse_cfg = _listener_callback_opaque;

	char *_buffer = process_mse_buffer(buffer,&buf_size,&data,&mse_cfg->database);
	if(_buffer){
		send_to_kafka(_buffer,buf_size,RD_KAFKA_MSG_F_FREE,(void *)(intptr_t)data.client_mac);
	}
}
