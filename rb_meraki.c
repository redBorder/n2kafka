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

#include "rb_meraki.h"
#include "rb_mac.h"
#include "kafka.h"
#include "global_config.h"

#include <librd/rdlog.h>
#include <assert.h>
#include <jansson.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <librdkafka/rdkafka.h>

static const char CONFIG_MERAKI_SECRETS_KEY[] = "meraki-secrets";

static const char MERAKI_TYPE_KEY[] = "type";
static const char MERAKI_TYPE_VALUE[] = "meraki";
static const char MERAKI_WIRELESS_STATION_KEY[] = "wireless_station";

static const char MERAKI_SRC_ORIGINAL_KEY[] = "ipv4";
static const char MERAKI_SRCv6_ORIGINAL_KEY[] = "ipv6";
static const char MERAKI_SRC_DESTINATION_KEY[] = "src";

static const char MERAKI_CLIENT_OS_ORIGINAL_KEY[] = "os";
static const char MERAKI_CLIENT_OS_DESTINATION_KEY[] = "client_os";

static const char MERAKI_CLIENT_MAC_VENDOR_ORIGINAL_KEY[] = "manufacturer";
static const char MERAKI_CLIENT_MAC_VENDOR_DESTINATION_KEY[] = "client_mac_vendor";

static const char MERAKI_CLIENT_MAC_ORIGINAL_KEY[] = "clientMac";
static const char MERAKI_CLIENT_MAC_DESTINATION_KEY[] = "client_mac";

static const char MERAKI_TIMESTAMP_ORIGINAL_KEY[] = "seenEpoch";
static const char MERAKI_SEEN_TIME_KEY[] = "seenTime";
static const char MERAKI_TIMESTAMP_DESTINATION_KEY[] = "timestamp";

static const char MERAKI_CLIENT_RSSI_NUM_ORIGINAL_KEY[] = "rssi";
static const char MERAKI_CLIENT_RSSI_NUM_DESTINATION_KEY[] = "client_rssi_num";

static const char MERAKI_WIRELESS_ID_ORIGINAL_KEY[] = "ssid";
static const char MERAKI_WIRELESS_ID_DESTINATION_KEY[] = "wireless_id";

static const char MERAKI_LOCATION_KEY[] = "location";
static const char MERAKI_LOCATION_LAT_KEY[] = "lat";
static const char MERAKI_LOCATION_LNG_KEY[] = "lng";
static const char MERAKI_CLIENT_LATLON_DESTINATION_KEY[] = "client_latlong";
static const char MERAKI_ENRICHMENT_KEY[] = "enrichment";

/* 
    VALIDATING MERAKI SECRET
*/

int parse_meraki_secrets(void *_db,const struct json_t *meraki_secrets,char *err,size_t err_size){
	assert(_db);

	struct meraki_database *db = _db;

	const char *key;
	json_t *value;

	json_t *new_db = NULL;

	new_db = json_deep_copy(meraki_secrets);
	if(!new_db){
		snprintf(err,err_size,"Can't create json object (out of memory?)");
		return -1;
	}

	pthread_rwlock_wrlock(&db->rwlock);
	json_t *old_db = db->root;
	db->root = new_db;
	if(new_db) {
		json_object_foreach(new_db,key,value) {
			// This field is needed in all output messages
			json_t *meraki_type = json_string(MERAKI_TYPE_VALUE);
			json_object_set_new(value,MERAKI_TYPE_KEY,meraki_type);
		}
	}

	pthread_rwlock_unlock(&db->rwlock);

	if(old_db)
		json_decref(old_db);

	return 0;
}

void meraki_database_done(struct meraki_database *db) {
	json_decref(db->root);
	pthread_rwlock_destroy(&db->rwlock);
}

/* 
    ENRICHMENT
*/

#define MERAKI_OPAQUE_MAGIC 0x3A10AEA1C
struct meraki_opaque{
#ifdef MERAKI_OPAQUE_MAGIC
	uint64_t magic;
#endif

	pthread_rwlock_t per_listener_enrichment_rwlock;
	struct meraki_config *meraki_config;
	json_t *per_listener_enrichment;
};

static struct meraki_opaque *meraki_opaque_cast(void *_opaque) {
	assert(_opaque);

	struct meraki_opaque *opaque = _opaque;
	assert(MERAKI_OPAQUE_MAGIC == opaque->magic);
	return opaque;
}

static int parse_per_listener_opaque_config(struct meraki_opaque *opaque,json_t *config,char *err,size_t errsize) {
	assert(opaque);
	assert(config);
	assert(err);
	json_error_t jerr;

	const int json_unpack_rc = json_unpack_ex(config,&jerr,0,"{s?O}",
		MERAKI_ENRICHMENT_KEY,&opaque->per_listener_enrichment);

	if(0!=json_unpack_rc)
		snprintf(err,errsize,"%s",jerr.text);

	return json_unpack_rc;
}

int meraki_opaque_creator(struct json_t *config,void **_opaque,char *err,size_t errsize) {
	assert(config);
	assert(_opaque);
	char errbuf[BUFSIZ];

	struct meraki_opaque *opaque = (*_opaque) = calloc(1,sizeof(*opaque));
	if(NULL == opaque) {
		snprintf(err,errsize,"%s","Can't allocate meraki opaque (out of memory?)");
		return -1;
	}

#ifdef MERAKI_OPAQUE_MAGIC
	opaque->magic = MERAKI_OPAQUE_MAGIC;
#endif

	const int rwlock_init_rc = pthread_rwlock_init(&opaque->per_listener_enrichment_rwlock,NULL);
	if(rwlock_init_rc != 0) {
		strerror_r(errno, errbuf, sizeof(errbuf));
		rdlog(LOG_ERR,"Can't start rwlock: %s",errbuf);
		goto _err;
	}

	const int per_listener_enrichment_rc = parse_per_listener_opaque_config(
		opaque,config,err,errsize);
	if(per_listener_enrichment_rc != 0){
		goto err_rwlock;
	}

	/// @TODO move global_config to static allocated buffer
	opaque->meraki_config = &global_config.meraki;

	return 0;

err_rwlock:
	pthread_rwlock_destroy(&opaque->per_listener_enrichment_rwlock);
_err:
	free(opaque);
	*_opaque = NULL;
	return -1;
}

int meraki_opaque_reload(json_t *config,void *_opaque) {
	struct meraki_opaque *opaque = _opaque;
	assert(opaque);
	assert(config);
#ifdef MSE_OPAQUE_MAGIC
	assert(MSE_OPAQUE_MAGIC == opaque->magic);
#endif

	json_t *new_enrichment = NULL,*old_enrichment = opaque->per_listener_enrichment;

	json_error_t jerr;

	const int unpack_rc = json_unpack_ex(config,&jerr,0,"{s?O}",
		MERAKI_ENRICHMENT_KEY,&new_enrichment);

	if(unpack_rc != 0) {
		rdlog(LOG_ERR,"Can't parse enrichment config: %s",jerr.text);
		return -1;
	}

	pthread_rwlock_wrlock(&opaque->per_listener_enrichment_rwlock);
	opaque->per_listener_enrichment = new_enrichment;
	pthread_rwlock_unlock(&opaque->per_listener_enrichment_rwlock);

	json_decref(old_enrichment);

	return 0;
}

void meraki_opaque_destructor(void *_opaque) {
	struct meraki_opaque *opaque = _opaque;
#ifdef MERAKI_OPAQUE_MAGIC
	assert(MERAKI_OPAQUE_MAGIC == opaque->magic);
#endif
	if(opaque->per_listener_enrichment)
		json_decref(opaque->per_listener_enrichment);
	free(opaque);
}


/* Data that should be in all kafka messages */
struct meraki_transversal_data {
	json_t *wireless_station;
	json_t *enrichment;
};

static int rename_key_if_exists(json_t *root,json_t *object,const char *old_key,const char *new_key) {
	if(object && !json_is_null(object)) {
		json_object_set(root,new_key,object);
	}
	return json_object_del(root,old_key) || object ? 0 : -1;
}

static int double_cmp(const double a,const double b) {
	return (a>b) - (a<b);
}

static void enrich_meraki_observation(json_t *observation,
                             struct meraki_transversal_data *transversal_data) {

	if(!transversal_data->enrichment) {
		rdlog(LOG_WARNING,"No enrichment, cannot add extra data like type");
		return;
	} 

	const int update_rc = json_object_update_missing(observation,transversal_data->enrichment);
	if(update_rc != 0) {
		rdlog(LOG_ERR,"Error on update missing.");
	}
}

/* transform meraki observation in our suitable keys/values */
static void transform_meraki_observation(json_t *observation,
                           struct meraki_transversal_data *transversal_data) {
	json_error_t jerr;
	
	json_t *src=NULL,*client_os = NULL,*client_mac=NULL,*client_mac_vendor=NULL,
	       *timestamp=NULL,*client_rssi_num=NULL,*wireless_id=NULL;
	double location_lat=0,location_lon=0;

	/* Unused */
	json_object_del(observation,MERAKI_SEEN_TIME_KEY);

	if(NULL == transversal_data->wireless_station) {
		rdlog(LOG_WARNING,"No %s in meraki message", MERAKI_WIRELESS_STATION_KEY);
	} else {
		json_object_set(observation,MERAKI_WIRELESS_STATION_KEY,
			                       transversal_data->wireless_station);
	}

	const int unpack_rc = json_unpack_ex(observation,&jerr,0,
	 	"{"
	 		"s?o," /* src */
	 		"s?o," /* client_os */
	 		"s:o," /* client_mac */
	 		"s?o," /* client_mac_vendor */
	 		"s:o," /* timestamp */
	 		"s?o," /* client_rssi_num */
	 		"s?o," /* ssid */
	 		"s?{"  /* location */
	 			"s?f,"  /* lat */
	 			"s?f"   /* long */
	 		"}"
	 	"}",
	 	MERAKI_SRC_ORIGINAL_KEY,&src,
	 	MERAKI_CLIENT_OS_ORIGINAL_KEY,&client_os,
		MERAKI_CLIENT_MAC_ORIGINAL_KEY,&client_mac,
		MERAKI_CLIENT_MAC_VENDOR_ORIGINAL_KEY,&client_mac_vendor,
		MERAKI_TIMESTAMP_ORIGINAL_KEY,&timestamp,
		MERAKI_CLIENT_RSSI_NUM_ORIGINAL_KEY,&client_rssi_num,
		MERAKI_WIRELESS_ID_ORIGINAL_KEY,&wireless_id,
		MERAKI_LOCATION_KEY,
			MERAKI_LOCATION_LAT_KEY,&location_lat,
			MERAKI_LOCATION_LNG_KEY,&location_lon
	 );

	if(unpack_rc != 0) {
		rdlog(LOG_ERR,"Can't unpack meraki message: %s",jerr.text);
		return;
	}

	/// @TODO ipv6 treatment
	if(src) {
		const char *_src = json_string_value(src);
		if(_src) {
			// src is /XXX.XXX.XXX.XXX ip. Need to chop first /
			json_t *new_src = json_string(&_src[1]);
			json_object_set_new(observation,MERAKI_SRC_DESTINATION_KEY,
				                                              new_src);
		}

		// Delete original form
		json_object_del(observation,MERAKI_SRC_ORIGINAL_KEY);
		json_object_del(observation,MERAKI_SRCv6_ORIGINAL_KEY);
	}

	if(0!=double_cmp(0,location_lat) && 0!=double_cmp(0,location_lon)) {
		char buf[BUFSIZ];
		snprintf(buf,sizeof(buf),"%.5f,%.5f",location_lat,location_lon);
		json_t *client_latlon = json_string(buf);
		json_object_set_new(observation,MERAKI_CLIENT_LATLON_DESTINATION_KEY,client_latlon);
	}

	json_object_del(observation,MERAKI_LOCATION_KEY);

	rename_key_if_exists(observation,client_os,
	        MERAKI_CLIENT_OS_ORIGINAL_KEY,MERAKI_CLIENT_OS_DESTINATION_KEY);
	rename_key_if_exists(observation,client_mac_vendor,
	        MERAKI_CLIENT_MAC_VENDOR_ORIGINAL_KEY,MERAKI_CLIENT_MAC_VENDOR_DESTINATION_KEY);
	rename_key_if_exists(observation,client_mac,
	        MERAKI_CLIENT_MAC_ORIGINAL_KEY,MERAKI_CLIENT_MAC_DESTINATION_KEY);
	rename_key_if_exists(observation,client_rssi_num,
	        MERAKI_CLIENT_RSSI_NUM_ORIGINAL_KEY,MERAKI_CLIENT_RSSI_NUM_DESTINATION_KEY);
	rename_key_if_exists(observation,wireless_id,
	        MERAKI_WIRELESS_ID_ORIGINAL_KEY,MERAKI_WIRELESS_ID_DESTINATION_KEY);
	rename_key_if_exists(observation,timestamp,
	        MERAKI_TIMESTAMP_ORIGINAL_KEY,MERAKI_TIMESTAMP_DESTINATION_KEY);

	enrich_meraki_observation(observation,transversal_data);
}

static void extract_meraki_observation(struct kafka_message_array *msgs,size_t idx,
	json_t *observations,struct meraki_transversal_data *transversal_data) {

	json_t *observation_i = json_array_get(observations,idx);

	if(NULL == observation_i) {
		rdlog(LOG_ERR,"NULL observation %zu. Can't process",idx);
		return;
	}

	transform_meraki_observation(observation_i,transversal_data);

	char *buf = json_dumps(observation_i,JSON_COMPACT|JSON_ENSURE_ASCII);
	/// @TODO Don't use strlen
	save_kafka_msg_in_array(msgs,buf,strlen(buf),NULL);
}

static int json_object_update_missing_copy(json_t *dst, json_t *src) {
	const char *key=NULL;
	json_t *value = NULL;

	if(!json_is_object(src) || !json_is_object(dst))
		return -1;

	json_object_foreach(src,key,value) {
		if(NULL == json_object_get(dst,key)){
			json_t *new_json = json_deep_copy(value);
			json_object_set_new_nocheck(dst,key,new_json);
		}
	}

	return 0;
}

static struct kafka_message_array *extract_meraki_data(json_t *json,struct meraki_opaque *opaque) {
	assert(json);
	assert(opaque);

	struct meraki_database *db = &opaque->meraki_config->database;
	size_t i;
	json_error_t jerr;
	struct meraki_transversal_data meraki_transversal = {NULL,NULL};
	json_t *observations = NULL;

	const char *meraki_secret = NULL;
	const int json_unpack_rc = json_unpack_ex(json,&jerr,0,"{s:s,s:{s:o,s:o}}",
		"secret",&meraki_secret,
		"data",
			"apMac",&meraki_transversal.wireless_station,
			"observations",&observations);

	if(0 != json_unpack_rc) {
		rdlog(LOG_ERR,"Can't decode meraki JSON: \"%s\" in line %d column %d",
			jerr.text,jerr.line,jerr.column);
	}

	if(NULL == meraki_secret) {
		rdlog(LOG_ERR,"Meraki JSON received with no secret. Discarding.");
		return NULL;
	}

	if(NULL == observations) {
		rdlog(LOG_ERR,"Meraki JSON received with no observations. Discarding.");
		return NULL;
	}

	if(!json_is_array(observations)) {
		rdlog(LOG_ERR,"Meraki JSON observations is not an array. Discarding.");
		return NULL;
	}

	pthread_rwlock_rdlock(&db->rwlock);
	json_t *enrichment_tmp = json_object_get(db->root,meraki_secret);
	if(enrichment_tmp){
		pthread_rwlock_rdlock(&opaque->per_listener_enrichment_rwlock);
		if(opaque->per_listener_enrichment)
			meraki_transversal.enrichment = json_deep_copy(opaque->per_listener_enrichment);
		pthread_rwlock_unlock(&opaque->per_listener_enrichment_rwlock);
		if(meraki_transversal.enrichment)
			json_object_update_missing_copy(meraki_transversal.enrichment,enrichment_tmp);
		else
			meraki_transversal.enrichment = json_deep_copy(enrichment_tmp);
	}
	pthread_rwlock_unlock(&db->rwlock);

	if(NULL == meraki_transversal.enrichment) {
		rdlog(LOG_ERR, "Meraki JSON received with no valid secret (%s). Discarding.",
			meraki_secret);
		return NULL;
	}

	json_object_update_missing_copy(meraki_transversal.enrichment,opaque->per_listener_enrichment);

	const size_t msgs_size = json_array_size(observations);
	struct kafka_message_array *msgs = new_kafka_message_array(msgs_size);

	for(i=0;i<msgs_size;++i)
		extract_meraki_observation(msgs,i,observations,&meraki_transversal);

	json_decref(meraki_transversal.enrichment);

	return msgs;
}

static struct kafka_message_array *process_meraki_buffer(const char *buffer,size_t bsize,
                                 struct meraki_opaque *opaque){
	struct kafka_message_array *notifications = NULL;
	assert(bsize);

	json_error_t err;
	json_t *json = json_loadb(buffer,bsize,0,&err);
	if(NULL == json){
		rdlog(LOG_ERR,"Error decoding meraki JSON (%s), line %d column %d: %s",
			buffer,err.line,err.column,err.text);
		goto err;
	}

	notifications = extract_meraki_data(json,opaque);
	if(!notifications || notifications->size == 0) {
		/* Nothing to do here */
		free(notifications);
		notifications = NULL;
		goto err;
	}

err:
	if(json)
		json_decref(json);
	return notifications;
}

void meraki_decode(char *buffer,size_t buf_size,void *_listener_callback_opaque){
	assert(buffer);
	assert(_listener_callback_opaque);

	struct meraki_opaque *meraki_opaque = meraki_opaque_cast(_listener_callback_opaque);

	struct kafka_message_array *notifications = process_meraki_buffer(buffer,buf_size,meraki_opaque);

	if(notifications){
		send_array_to_kafka(notifications);
		free(notifications);
	}
	free(buffer);
}
