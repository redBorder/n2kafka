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

#include <librd/rdlog.h>
#include <assert.h>
#include <jansson.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>

static const char MSE8_STREAMING_NOTIFICATION_KEY[] = "StreamingNotification";
static const char MSE8_LOCATION_KEY[] = "location";
static const char MSE8_MAC_ADDRESS_KEY[] = "macAddress";

static const char MSE_SUBSCRIPTION_NAME_KEY[] = "subscriptionName";
static const char MSE_DEVICE_ID_KEY[] = "deviceId";

static const char MSE10_NOTIFICATIONS_KEY[] = "notifications";

/* 
    VALIDATING MSE
*/

struct valid_mse_database{
	/* Root with all valid MSE. We trust in jansson hashtable performance here */
	pthread_rwlock_t rwlock;
	char *path;
	json_t *root;
};

int reload_valid_mse_database(struct valid_mse_database *db,char *err,size_t err_size){
	assert(db);
	assert(db->path);

	json_error_t json_err;
	json_t *value=NULL,*new_db = NULL;
	size_t _index;

	json_t *file = json_load_file(db->path,0,&json_err);
	if(!file){
		snprintf(err,err_size,"Can't load MSE database %s, line %d column %d: %s",
			db->path,json_err.line,json_err.column,json_err.text);
		goto err_tag;
	}

	if(!json_is_array(file)){
		snprintf(err,err_size,"Can't load MSE database %s: Expected array",db->path);
		json_decref(file);
		goto free_file_error;
	}

	new_db = json_object();
	if(!new_db){
		snprintf(err,err_size,"Can't create json object (out of memory?)");
		json_decref(file);
		goto free_file_error;
	}

	json_array_foreach(file,_index,value){
		const char *key = json_string_value(value);
		if(NULL == key){
			snprintf(err,err_size,"Can't extract string of element %zu",_index);
			goto free_new_db;
		}

		json_object_set_new(new_db,key,json_object());
	}

	pthread_rwlock_wrlock(&db->rwlock);
	json_t *old_db = db->root;
	db->root = new_db;
	pthread_rwlock_unlock(&db->rwlock);

	if(old_db)
		json_decref(old_db);

	return 0;

free_new_db:
	json_decref(new_db);
free_file_error:
	json_decref(file);
err_tag:
	return -1;
}

struct valid_mse_database *parse_valid_mse_file(const char *path,char *err,size_t err_size){
	assert(path);
	assert(err);

	char strerror_buf[BUFSIZ];
	struct valid_mse_database *db = calloc(1,sizeof(*db));
	if(NULL == db){
		snprintf(err,err_size,"Can't allocate mse database (out of memory?)");
		goto error;
	}
	
	const int rc = pthread_rwlock_init(&db->rwlock,NULL);
	if(rc != 0){
		strerror_r(errno,strerror_buf,sizeof(strerror_buf));
		snprintf(err,err_size,"Can't init mse database pthread lock: %s",strerror_buf);
		goto free_db_error;
	}

	db->path = strdup(path);

	if(NULL == db->path) {
		snprintf(err,err_size,"Can't strdup (out of memory?)");
		goto delete_lock_error;
	}

	const int read_rc = reload_valid_mse_database(db,err,err_size);
	if(read_rc != 0){
		goto free_strdupped_error;
	}

	return db;

free_strdupped_error:
	free(db->path);
delete_lock_error:
	pthread_rwlock_destroy(&db->rwlock);
free_db_error:
	free(db);
error:
	return NULL;
}

static int is_valid_mse(const char *mse,struct valid_mse_database *db){
	assert(db);
	pthread_rwlock_wrlock(&db->rwlock);
	const int rc = json_object_get(db->root,mse) != NULL;
	pthread_rwlock_unlock(&db->rwlock);
	return rc;
}

void free_valid_mse_database(struct valid_mse_database *db){
	pthread_rwlock_destroy(&db->rwlock);
	free(db->path);
	json_decref(db->root);
}

/* 
    ENRICHMENT
*/

struct enrich_with {
	json_t *json;
};

struct enrich_with *process_enrich_with(const char *_enrich_with){
	json_error_t err;

	struct enrich_with *ret = calloc(1,sizeof(ret[0]));
	if(ret){
		ret->json = json_loads(_enrich_with,0,&err);
		if(NULL == ret->json){
			rdlog(LOG_ERR,"Can't parse entich with (%s): %s",
				_enrich_with,err.text);
			free(ret);
			ret = NULL;
		}
	}

	return ret;
}

void free_enrich_with(struct enrich_with *enrich_with){
	json_decref(enrich_with->json);
	free(enrich_with);
}

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

static void enrich_mse_json(json_t *json,const struct enrich_with *enrich_with){
	json_t *_enrich_with = json_deep_copy(enrich_with->json);
	if(NULL == _enrich_with){
		rdlog(LOG_ERR,"Can't json_deep_copy (out of memory?)");
	}else{
		json_object_update_missing(json,_enrich_with);
		json_decref(_enrich_with);
	}
}

char *process_mse_buffer(char *buffer,size_t *bsize,struct mse_data *to,
                                 const struct enrich_with *enrich_with,
                                 struct valid_mse_database *db){
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
	if(db && !is_valid_mse(to->subscriptionName,db)) {
		free(buffer);
		return NULL;
	}
	
	if(enrich_with){
		enrich_mse_json(json,enrich_with);
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
