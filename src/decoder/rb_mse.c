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

#include "rb_mse.h"
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
#include <sys/queue.h>

static const char MSE8_STREAMING_NOTIFICATION_KEY[] = "StreamingNotification";
static const char MSE8_LOCATION_KEY[] = "location";
static const char MSE8_MAC_ADDRESS_KEY[] = "macAddress";

static const char MSE8_TIMESTAMP[] = "timestampMillis";
static const char MSE10_TIMESTAMP[] = "timestamp";

static const char MSE_SUBSCRIPTION_NAME_KEY[] = "subscriptionName";
static const char MSE_DEVICE_ID_KEY[] = "deviceId";
static const char MSE_DEFAULT_STREAM[] = "*";

static const char MSE10_NOTIFICATIONS_KEY[] = "notifications";

static const char CONFIG_MSE_SENSORS_KEY[] = "mse-sensors";
static const char MSE_ENRICHMENT_KEY[] = "enrichment";

static const char MSE_MAX_TIME_OFFSET[] = "max_time_offset";
static const char MSE_MAX_TIME_OFFSET_WARNING_WAIT[] =
    "max_time_offset_warning_wait";
/*
    VALIDATING MSE
*/

static int queue_init;

static pthread_mutex_t mutex;

struct mse_device {
	TAILQ_ENTRY(mse_device) tailq;
	char *subscriptionName;
	time_t warning_timestamp;
};

TAILQ_HEAD(, mse_device) mse_device_q;

struct mse_data {
	uint64_t client_mac;
	const char *subscriptionName;
	/* private */
	const char *_client_mac;
	json_t *json;
	char *string;
	size_t string_size;
	time_t timestamp;
	int timestamp_warnings;
};

struct mse_array {
	struct mse_data *data;
	size_t size;
};

void init_mse_database(struct mse_database *db) {
	memset(db, 0, sizeof(*db));
	pthread_rwlock_init(&db->rwlock, 0);
}

#define MSE_OPAQUE_MAGIC 0xE0AEA1CE0AEA1CL
struct mse_opaque {
#ifdef MSE_OPAQUE_MAGIC
	uint64_t magic;
#endif

	pthread_rwlock_t per_listener_enrichment_rwlock;
	json_t *per_listener_enrichment;
	long max_time_offset;
	long max_time_offset_warning_wait;
	struct mse_config *mse_config;
};

static int parse_per_listener_opaque_config(struct mse_opaque *opaque,
        json_t *config) {
	assert(opaque);
	assert(config);
	json_error_t jerr;
	json_int_t max_time_offset = 3600;
	json_int_t max_time_offset_warning_wait = 0;

	int json_unpack_rc = json_unpack_ex(config, &jerr, 0,
	                                    "{s?O"
	                                    "s?I"
	                                    "s?I}",
	                                    MSE_ENRICHMENT_KEY,
	                                    &opaque->per_listener_enrichment,
	                                    MSE_MAX_TIME_OFFSET_WARNING_WAIT,
	                                    &max_time_offset_warning_wait,
	                                    MSE_MAX_TIME_OFFSET,
	                                    &max_time_offset);

	opaque->max_time_offset = max_time_offset;
	opaque->max_time_offset_warning_wait = max_time_offset_warning_wait;

	if (0 != json_unpack_rc)
		rdlog(LOG_ERR, "%s", jerr.text);

	return json_unpack_rc;
}

int mse_opaque_creator(json_t *config, void **_opaque) {
	assert(_opaque);
	char errbuf[BUFSIZ];

	struct mse_opaque *opaque = (*_opaque) = calloc(1, sizeof(*opaque));
	if (NULL == opaque) {
		rdlog(LOG_ERR, "Can't alloc MSE opaque (out of memory?)");
		return -1;
	}

#ifdef MSE_OPAQUE_MAGIC
	opaque->magic = MSE_OPAQUE_MAGIC;
#endif

	const int rwlock_init_rc = pthread_rwlock_init(
	                               &opaque->per_listener_enrichment_rwlock, NULL);
	if (rwlock_init_rc != 0) {
		strerror_r(errno, errbuf, sizeof(errbuf));
		rdlog(LOG_ERR, "Can't start rwlock: %s", errbuf);
		goto _err;
	}

	const int per_listener_enrichment_rc = parse_per_listener_opaque_config(opaque,
	                                       config);
	if (per_listener_enrichment_rc != 0) {
		goto err_rwlock;
	}

	/// @TODO move global_config to static allocated buffer
	opaque->mse_config = &global_config.mse;

	return 0;

err_rwlock:
	pthread_rwlock_destroy(&opaque->per_listener_enrichment_rwlock);
_err:
	free(opaque);
	*_opaque = NULL;
	return -1;
}

static void mse_warn_timestamp(struct mse_data *data,
                               struct mse_opaque *opaque,
                               time_t now) {
	int found = 0;
	struct mse_device *p;

	TAILQ_FOREACH(p, &mse_device_q, tailq) {
		if (0 == strcmp(p->subscriptionName, data->subscriptionName)) {
			found = 1;
			if (
			    (now - p->warning_timestamp) >= opaque->max_time_offset_warning_wait
			    &&
			    abs(data->timestamp - now) > opaque->max_time_offset
			) {
				rdlog(LOG_WARNING, "Timestamp out of date");
				data->timestamp_warnings++;
				p->warning_timestamp = now;
			}
			break;
		}
	}

	if (found == 0) {
		rdlog(LOG_WARNING, "Timestamp out of date");
		struct mse_device *mse_device = calloc(1, sizeof(struct mse_device));
		mse_device->subscriptionName = strdup(data->subscriptionName);
		mse_device->warning_timestamp = now;
		pthread_mutex_lock(&mutex);
		TAILQ_INSERT_TAIL(&mse_device_q, mse_device, tailq);
		pthread_mutex_unlock(&mutex);
	}
}

int mse_opaque_reload(json_t *config, void *_opaque) {
	struct mse_opaque *opaque = _opaque;
	assert(opaque);
	assert(config);
#ifdef MSE_OPAQUE_MAGIC
	assert(MSE_OPAQUE_MAGIC == opaque->magic);
#endif

	json_t *new_enrichment = NULL,
	        *old_enrichment = opaque->per_listener_enrichment;

	json_error_t jerr;

	const int unpack_rc = json_unpack_ex(config, &jerr, 0, "{s?O}",
	                                     MSE_ENRICHMENT_KEY, &new_enrichment);

	if (unpack_rc != 0) {
		rdlog(LOG_ERR, "Can't parse enrichment config: %s", jerr.text);
		return -1;
	}


	pthread_rwlock_wrlock(&opaque->per_listener_enrichment_rwlock);
	opaque->per_listener_enrichment = new_enrichment;
	pthread_rwlock_unlock(&opaque->per_listener_enrichment_rwlock);

	json_decref(old_enrichment);

	return 0;
}

void mse_opaque_done(void *_opaque) {
	assert(_opaque);

	struct mse_opaque *opaque = _opaque;
#ifdef MSE_OPAQUE_MAGIC
	assert(MSE_OPAQUE_MAGIC == opaque->magic);
#endif
	pthread_rwlock_destroy(&opaque->per_listener_enrichment_rwlock);
	if (opaque->per_listener_enrichment)
		json_decref(opaque->per_listener_enrichment);
	free(opaque);
}


static int parse_sensor(json_t *sensor, json_t *streams_db) {
	json_error_t err;
	const char *stream = NULL;
	const json_t *enrichment = NULL;

	assert(sensor);
	assert(streams_db);

	const int unpack_rc = json_unpack_ex((json_t *)sensor, &err, 0,
	                                     "{s:s,s?o}", "stream", &stream, MSE_ENRICHMENT_KEY, &enrichment);

	if (unpack_rc != 0) {
		rdlog(LOG_ERR, "Can't parse sensor (%s): %s", json_dumps(sensor, 0), err.text);
		return -1;
	}

	if (stream == NULL) {
		rdlog(LOG_ERR, "Can't parse sensor (%s): %s", json_dumps(sensor, 0),
		      "No \"stream\"");
		return -1;
	}

	json_t *_enrich = enrichment ? json_deep_copy(enrichment) : json_object();

	const int set_rc = json_object_set_new(streams_db, stream, _enrich);
	if (set_rc != 0) {
		rdlog(LOG_ERR, "Can't set new MSE enrichment db entry (out of memory?)");
	}

	return 0;
}

int parse_mse_array(void *_db, const struct json_t *mse_array) {
	assert(_db);

	struct mse_database *db = _db;

	json_t *value = NULL, *new_db = NULL;
	size_t _index;

	if (!json_is_array(mse_array)) {
		rdlog(LOG_ERR, "Expected array");
		return -1;
	}

	new_db = json_object();
	if (!new_db) {
		rdlog(LOG_ERR, "Can't create json object (out of memory?)");
		return -1;
	}

	json_array_foreach(mse_array, _index, value) {
		parse_sensor(value, new_db);
	}

	pthread_rwlock_wrlock(&db->rwlock);
	json_t *old_db = db->root;
	db->root = new_db;
	pthread_rwlock_unlock(&db->rwlock);

	if (old_db) {
		json_decref(old_db);
	}

	return 0;
}

static json_t *mse_database_entry_copy(const char *subscriptionName,
                                       struct mse_database *db) {
	assert(subscriptionName);
	assert(db);
	pthread_rwlock_rdlock(&db->rwlock);
	json_t *ret = json_object_get(db->root, subscriptionName);
	pthread_rwlock_unlock(&db->rwlock);
	return ret;
}

void free_valid_mse_database(struct mse_database *db) {
	if (db && db->root)
		json_decref(db->root);
}

/*
    ENRICHMENT
*/

static int is_mse8_message(const json_t *json) {
	return NULL != json_object_get(json, MSE8_STREAMING_NOTIFICATION_KEY);
}

static int is_mse10_message(const json_t *json) {
	/* If it has notification array, it is a mse10 flow */
	return NULL != json_object_get(json, MSE10_NOTIFICATIONS_KEY);
}

static int extract_mse8_rich_data0(json_t *from, struct mse_data *to) {
	json_error_t err;
	const char *macAddress = NULL;
	json_int_t current_timestamp_ms = 0;
	const int unpack_rc = json_unpack_ex(from, &err, 0,
	                                     "{s:{"         /* Streaming notification */
	                                     "s:s,"     /* subscriptionName */
	                                     "s:s,"     /* deviceId */
	                                     "s:I"      /* timestamp */
	                                     "s:{"      /* location */
	                                     "s:s"  /* macAddress */
	                                     "}"
	                                     "}}",
	                                     MSE8_STREAMING_NOTIFICATION_KEY,
	                                     MSE_SUBSCRIPTION_NAME_KEY, &to->subscriptionName,
	                                     MSE_DEVICE_ID_KEY, &to->_client_mac,
	                                     MSE8_TIMESTAMP, &current_timestamp_ms,
	                                     MSE8_LOCATION_KEY,
	                                     MSE8_MAC_ADDRESS_KEY, &macAddress);

	to->timestamp = current_timestamp_ms / 1000;

	if (unpack_rc < 0) {
		rdlog(LOG_ERR, "Can't extract MSE8 rich data from (%s), line %d column %d: %s",
		      err.source, err.line, err.column, err.text);
	} else {
		assert(to->_client_mac);
		assert(macAddress);

		if (0 != strcmp(to->_client_mac, macAddress)) {
			rdlog(LOG_WARNING, "deviceId != macAddress: [%s]!=[%s]. Using deviceId",
			      to->_client_mac, macAddress);
		}

		to->json = json_object_get(from, MSE8_STREAMING_NOTIFICATION_KEY);
	}

	return unpack_rc;
}


static struct mse_array *extract_mse8_rich_data(json_t *from, int *extract_rc) {
	assert(extract_rc);

	struct mse_array *array = calloc(1,
	                                 sizeof(struct mse_array) + sizeof(struct mse_data));
	if (!array) {
		*extract_rc = -1;
		return NULL;
	}

	array->size = 1;
	array->data = (void *)&array[1];
	*extract_rc = extract_mse8_rich_data0(from, array->data);

	return array;
}

static int extract_mse10_rich_data0(json_t *from, struct mse_data *to) {
	json_error_t err;
	json_int_t current_timestamp_ms = 0;
	const int unpack_rc = json_unpack_ex(from, &err, 0,
	                                     "{s:s,"  /* deviceId */
	                                     "s:I"			/* timestamp */
	                                     "s:s}",  /* subscriptionName */
	                                     MSE_DEVICE_ID_KEY, &to->_client_mac,
	                                     MSE10_TIMESTAMP, &current_timestamp_ms,
	                                     MSE_SUBSCRIPTION_NAME_KEY, &to->subscriptionName);

	if (unpack_rc != 0) {
		rdlog(LOG_ERR, "Can't extract mse 10 rich data: %s", err.text);
	}

	to->timestamp = current_timestamp_ms / 1000;

	return unpack_rc;
}

static struct mse_array *extract_mse10_rich_data(json_t *from,
        int *extract_rc) {
	assert(from);
	assert(extract_rc);

	size_t i;
	json_error_t err;
	json_t *notifications_array;

	*extract_rc = json_unpack_ex(from, &err, 0,
	                             "{s:o}",  /* subscriptionName */
	                             MSE10_NOTIFICATIONS_KEY, &notifications_array);

	if (*extract_rc != 0) {
		rdlog(LOG_ERR, "Can't parse MSE10 JSON notifications array: %s", err.text);
		return NULL;
	}

	const size_t mse_array_size = json_array_size(notifications_array);
	const size_t alloc_size = sizeof(struct mse_array) + mse_array_size * sizeof(
	                              struct mse_data);

	struct mse_array *mse_array = calloc(1, alloc_size);
	mse_array->size = mse_array_size;
	mse_array->data = (void *)&mse_array[1];

	for (i = 0; i < mse_array_size; ++i) {
		mse_array->data[i].json = json_array_get(notifications_array, i);
		if (NULL == mse_array->data[i].json) {
			rdlog(LOG_ERR, "Can't extract MSE10 notification position %zu", i);
		} else {
			extract_mse10_rich_data0(mse_array->data[i].json, &mse_array->data[i]);
		}
	}

	return mse_array;
}

static void parse_mac_addresses(const char *buffer,
                                struct mse_array *mse_array) {
	size_t i;
	for (i = 0; i < mse_array->size; ++i) {
		struct mse_data *to = &mse_array->data[i];
		to->client_mac = parse_mac(to->_client_mac);
		if (!valid_mac(to->client_mac)) {
			rdlog(LOG_WARNING, "Can't found client mac in (%s), using random partitioner",
			      buffer);
			to->client_mac = 0;
		}
	}
}

static struct mse_array *extract_mse_data(const char *buffer, json_t *json) {
	int extract_rc =  0;
	struct mse_array *mse_array =
	    is_mse8_message(json)  ? extract_mse8_rich_data(json, &extract_rc)  :
	    is_mse10_message(json) ? extract_mse10_rich_data(json, &extract_rc) :
	    ( {rdlog(LOG_ERR, "This is not an valid MSE JSON: %s", buffer); NULL;});


	if (extract_rc < 0 || mse_array == NULL)
		return NULL;

	parse_mac_addresses(buffer, mse_array);

	return mse_array;
}

static void enrich_mse_json(json_t *json, /* TODO const */ json_t
                            *enrichment_data) {
	assert(json);
	assert(enrichment_data);

	json_object_update_missing_copy(json, enrichment_data);
}

static struct mse_array *process_mse_buffer(const char *buffer, size_t bsize,
        const char *client, struct mse_opaque *opaque, time_t now) {
	struct mse_database *db = &opaque->mse_config->database;
	struct mse_array *notifications = NULL;
	size_t i;
	assert(bsize);

	json_error_t err;
	json_t *json = json_loadb(buffer, bsize, 0, &err);
	if (NULL == json) {
		rdlog(LOG_ERR,
		      "Error decoding MSE JSON (%s) of client (%s), line %d column %d: %s",
		      buffer, client, err.line, err.column, err.text);
		goto err;
	}

	notifications = extract_mse_data(buffer, json);
	if (!notifications || notifications->size == 0) {
		/* Nothing to do here */
		free(notifications);
		notifications = NULL;
		goto err;
	}

	for (i = 0; i < notifications->size; ++i) {
		struct mse_data *to = &notifications->data[i];
		json_t *enrichment = NULL;
		json_error_t _err;

		if (db && !to->subscriptionName) {
			rdlog(LOG_ERR, "Received MSE message with no subscription name. Discarding.");
		}

		if (db && to->subscriptionName) {
			enrichment = mse_database_entry_copy(to->subscriptionName, db);
			if (NULL == enrichment) {
				/* Try the default one */
				enrichment = mse_database_entry_copy(MSE_DEFAULT_STREAM, db);
			}

			if (NULL == enrichment) {
				rdlog(LOG_ERR, "MSE message (%s) has unknown subscription "
				      "name %s, and no default stream \"%s\" specified. "
				      "Discarding.",
				      buffer, to->subscriptionName, MSE_DEFAULT_STREAM);
				memset(to, 0, sizeof(to[0]));
				continue;
			}
		}

		if (db && opaque->per_listener_enrichment) {
			pthread_rwlock_rdlock(&opaque->per_listener_enrichment_rwlock);
			enrich_mse_json(to->json, opaque->per_listener_enrichment);
			pthread_rwlock_unlock(&opaque->per_listener_enrichment_rwlock);
		}

		if (db && enrichment) {
			enrich_mse_json(to->json, enrichment);
		}

		mse_warn_timestamp(to, opaque, now);

		if (notifications->size > 1) {
			/* Creating a new MSE notification mesage dissecting notifications in array.
			   This is due a kafka partitioner: We couldn't partition if >1 MACS come in the same
			   message */

			json_t *out = json_pack_ex(&_err, 0, "{s:[O]}", MSE10_NOTIFICATIONS_KEY,
			                           to->json);
			if (NULL == out) {
				rdlog(LOG_ERR, "Can't pack a new value: %s", err.text);
			} else {
				to->string = json_dumps(out,
				                        JSON_COMPACT | JSON_ENSURE_ASCII);
				json_decref(out);
			}

			to->json = NULL;
		} else {
			/* We can use the current json, no need to create a new one.
			   This is MSE8 case too. */
			to->string = json_dumps(json, JSON_COMPACT | JSON_ENSURE_ASCII);
		}
		to->string_size = strlen(to->string);
	}

err:
	if (json)
		json_decref(json);
	return notifications;
}

void mse_decode(char *buffer, size_t buf_size,
                const keyval_list_t *keyval,
                void *_listener_callback_opaque) {
	size_t i;
	struct mse_opaque *mse_opaque = _listener_callback_opaque;
#ifdef MSE_OPAQUE_MAGIC
	assert(MSE_OPAQUE_MAGIC == mse_opaque->magic);
#endif
	const char *client = valueof(keyval, "client_ip");
	if (NULL == client) {
		client = "(unknown)";
	}

	if (queue_init == 0) {
		TAILQ_INIT(&mse_device_q);
		queue_init = 1;
	}

	time_t now = time(NULL);
	struct mse_array *notifications = process_mse_buffer(buffer, buf_size, client,
	                                  mse_opaque, now);
	free(buffer);

	if (NULL == notifications)
		return;

	for (i = 0; i < notifications->size; ++i) {
		if (notifications->data[i].string) {
			send_to_kafka(notifications->data[i].string,
			              notifications->data[i].string_size,
			              RD_KAFKA_MSG_F_FREE, (void *)(intptr_t)notifications->data[i].client_mac);
		}
	}
}
