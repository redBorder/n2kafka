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

static const char MSE8_STREAMING_NOTIFICATION_KEY[] = "StreamingNotification";
static const char MSE8_LOCATION_KEY[] = "location";
static const char MSE8_MAC_ADDRESS_KEY[] = "macAddress";

static const char MSE_SUBSCRIPTION_NAME_KEY[] = "subscriptionName";
static const char MSE_DEVICE_ID_KEY[] = "deviceId";

static const char MSE10_NOTIFICATIONS_KEY[] = "notifications";

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


char *extract_mse_rich_data(char *buffer,size_t *bsize,struct mse_data *to){
	json_error_t err;
	json_t *json = json_loadb(buffer,*bsize,0,&err);
	if(NULL == json){
		rdlog(LOG_ERR,"Error decoding MSE JSON (%s), line %d column %d: %s",
			buffer,err.line,err.column,err.text);
		return buffer;
	}

	const int extract_rc = 
		is_mse8_message(json)  ? extract_mse8_rich_data(json,to)  :
		is_mse10_message(json) ? extract_mse10_rich_data(json,to) :
		({rdlog(LOG_ERR,"This is not an valid MSE JSON: %s",buffer);-1;});
	
	if(extract_rc == 0){
		to->client_mac = parse_mac(to->_client_mac);
		if(!valid_mac(to->client_mac))
			to->client_mac = 0;
	}

	to->_client_mac = NULL;
	json_decref(json);

	return buffer; /* If we change buffer, we have to modify bsize */
}
