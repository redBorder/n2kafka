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

#include "rb_http2k_sync_thread.h"
#include "rb_http2k_sync_common.h"
#include "util/util.h"

#include "engine/global_config.h"
#include <librd/rdlog.h>
#include <jansson.h>

#include <assert.h>
#include <string.h>
#include <errno.h>

/// librdkafka property to modify consumer group id
static const char RDKAFKA_CONF_GROUP_ID[] = "group.id";

/** Check that we are using a valid sync thread */
static void assert_sync_thread(const sync_thread_t *thread) {
#ifdef SYNC_THREAD_MAGIC
	assert(SYNC_THREAD_MAGIC == thread->magic);
#endif
}

/* FW declaration */
static void *sync_thread(void *);
struct msg_consume_ctx;
static void sync_thread_msg_consume(rd_kafka_message_t *msg, void *ctx);

int sync_thread_init(sync_thread_t *thread, rd_kafka_conf_t *rk_conf,
						organizations_db_t *org_db) {
	char err[BUFSIZ];
	size_t group_id_size = 0;
	static const int thread_shared_sem = 0;
	static const unsigned int sem_initial_value = 0;
	memset(thread, 0, sizeof(*thread));

#ifdef SYNC_THREAD_MAGIC
	thread->magic = SYNC_THREAD_MAGIC;
#endif
	thread->run = 1;
	thread->rk_conf = rk_conf;
	thread->org_db = org_db;

	const rd_kafka_conf_res_t get_group_id_rc = rd_kafka_conf_get (rk_conf,
		RDKAFKA_CONF_GROUP_ID, NULL, &group_id_size);
	if (RD_KAFKA_RESP_ERR_NO_ERROR != get_group_id_rc) {
		rdlog(LOG_ERR,
			"Couldn't get a valid group_id from rk_conf: %s",
			rd_kafka_err2str(get_group_id_rc));
		return -1;
	}
	if (0 == group_id_size) {
		rdlog(LOG_ERR, "n2kafka_id not specified");
		return -1;
	}

	sem_init(&thread->sem, thread_shared_sem, sem_initial_value);

	const int thread_rc = pthread_create(&thread->thread, NULL,
							sync_thread, thread);
	if (thread_rc != 0) {
		rdlog(LOG_ERR, "Couldn't create thread: %s", mystrerror(errno,
							err, sizeof(err)));
		sem_destroy(&thread->sem);
		return -1;
	}

	/* wait until child say it's ok */
	sem_wait(&thread->sem);
	sem_destroy(&thread->sem);

	return thread->creation_rc;
}

void sync_thread_done(sync_thread_t *thread) {
	thread->run = 0;
	pthread_join(thread->thread, NULL);
}

/** Checks if rk current topic is also the requested topic
  @param thread Thread to check
  @param topic Topic to use for sync
  @return 0 if no need to reload, !0 in other case
  */
static int udpate_sync_topic_need_to_reload(sync_thread_t *thread,
						rd_kafka_topic_t *topic) {
	int rc = 1;
	rd_kafka_topic_partition_list_t *curr_topics = NULL;

	const rd_kafka_resp_err_t subscription_rc =
				rd_kafka_assignment(thread->rk, &curr_topics);

	if (subscription_rc != RD_KAFKA_RESP_ERR_NO_ERROR) {
		rdlog(LOG_ERR, "Couldn't get current assignments: %s",
					rd_kafka_err2str(subscription_rc));
		/* Better not to reload */
		rc = 0;
		goto done;
	}

	if (NULL == topic && (NULL == curr_topics || 0 == curr_topics->cnt)) {
		/* We don't have a sync topic and we have specified any */
		rc = 0;
		goto done;
	}

	if (curr_topics && curr_topics->cnt > 1) {
		const char *requested_topic = rd_kafka_topic_name(topic);
		if (topic && 0!=strcmp(curr_topics->elems[0].topic,
							requested_topic)) {
			/* We assume that we only have one topic, so all list
			   elements belongs to the same topic */
			rdlog(LOG_ERR, "Sync topic can't be reloaded.");
		}
		rc = 0;
		goto done;
	}

done:
	if (curr_topics) {
		rd_kafka_topic_partition_list_destroy(curr_topics);
	}

	return rc;
}

/** Actual update of sync topic
  @param thread Thread
  @param topic New topic to consume
  @return 0 if success, !0 in other case
  */
static int update_sync_topic0(sync_thread_t *thread, rd_kafka_topic_t *rkt) {
	int rc = 0, i;
	const char *topic_name = rd_kafka_topic_name(rkt);
	const struct rd_kafka_metadata *metadata = NULL;
	rd_kafka_topic_partition_list_t *topics = NULL;

	rdlog(LOG_INFO, "Getting topic %s metadata", topic_name);
	const rd_kafka_resp_err_t get_topic_metadata_err =
				kafka_get_topic_metadata(rkt, &metadata, 5000);
	if (get_topic_metadata_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
		/* Log already given */
		return -1;
	}

	if (NULL == metadata || 0 == metadata->topic_cnt) {
		rdlog(LOG_ERR, "No metadata returned");
		return -1;
	}

	const struct rd_kafka_metadata_topic *topic_metadata =
							metadata->topics;
	const int partition_cnt = topic_metadata->partition_cnt;

	rdlog(LOG_INFO, "Topic %s has %d partitions", topic_name,
						partition_cnt);


	topics = rd_kafka_topic_partition_list_new(partition_cnt);
	if (NULL == topics) {
		rdlog(LOG_ERR, "Couldn't allocate topic list (out of memory?)");
		rc = -1;
		goto list_new_err;
	}

	for (i = 0; i < partition_cnt; ++i) {
		rd_kafka_topic_partition_list_add(topics, topic_name, i);
		rd_kafka_topic_partition_list_set_offset(topics, topic_name, i,
									0);
	}

	rdlog(LOG_INFO, "Assigning %d partitions", topics->cnt);

	const rd_kafka_resp_err_t assign_rc = rd_kafka_assign(thread->rk,
								topics);
	if (assign_rc != RD_KAFKA_RESP_ERR_NO_ERROR) {
		rdlog(LOG_ERR, "Couldn't assign topic %s: %s", topic_name,
						rd_kafka_err2str(assign_rc));
		rc = -1;
	}

	rd_kafka_topic_partition_list_destroy(topics);
list_new_err:
	rd_kafka_metadata_destroy(metadata);
	return rc;
}

int update_sync_topic(sync_thread_t *thread, rd_kafka_topic_t *topic) {
	if (udpate_sync_topic_need_to_reload(thread, topic)) {
		return update_sync_topic0(thread,topic);
	} else {
		return 0;
	}
}

/// Context that message_consume needs
struct msg_consume_ctx {
#ifndef NDEBUG
#define MSG_CONSUME_CTX_MAGIC 0x5C053CA1C5C053CAl
	/// Constant to assert coherency
	uint64_t magic;
#endif
	/// Sync thread
	sync_thread_t *thread;

	/// This n2kafka is in sync
	int in_sync;

	/// My n2kafka id to not consume out own messages
	char *n2kafka_id;
};

/** Assert that we are using a valid msg_consume_ctx
  @param ctx Context to check
  */
static void assert_msg_consume_ctx(const struct msg_consume_ctx *ctx) {
#ifdef MSG_CONSUME_CTX_MAGIC
	assert(MSG_CONSUME_CTX_MAGIC == ctx->magic);
#endif
}

/** Get an object from a json. If it can't get it, it will print an error
	message
  @param root JSON object to get child
  @param key child key
  @return Child object if we could found it, NULL in other case
  */
static const json_t *json_object_get_verbose(const json_t *root, const char *key) {
	const json_t *value = json_object_get(root, key);
	if (NULL == value) {
		rdlog(LOG_DEBUG, "Couldn't found %s key", key);
	}
	return value;
}

/** Verbose strtoul */
static uint64_t my_strtouint64(const char *str) {
	char err[BUFSIZ];
	char *endptr = NULL;
	const unsigned long int ret = strtoul(str, &endptr, 10);
	if (!(*endptr == '\0')) {
		rdlog(LOG_ERR, "Couldn't parse number %s: %s",str,
					mystrerror(errno, err, sizeof(err)));
	}

	return ret;
}

/** Get the integer child of an object. If the child is a string, we will
  convert it.
  @param root Root to obtain child
  @param key child key
  @return Integer value
  */
static uint64_t int_value_of(const json_t *root, const char *key) {
	const char *value_str = NULL;
	const json_t *value = json_object_get_verbose(root, key);
	if (NULL == value) {
		return 0;
	}

	switch(json_typeof(value)) {
	case JSON_STRING:
		value_str = json_string_value(value);
		return my_strtouint64(value_str);

	case JSON_INTEGER:
		return (uint64_t)json_integer_value(value);

	case JSON_REAL:
		return json_real_value(value);

	case JSON_OBJECT:
	case JSON_ARRAY:
	case JSON_TRUE:
	case JSON_FALSE:
	case JSON_NULL:
	default:
		rdlog(LOG_ERR,"Couldn't parse %s: No valid type", key);
		return 0;
	};
}

/// Decodification of a sync message
struct organization_bytes_update {
	/// N2kafka id that sent message
	const char *n2kafka_id;
	/// Organization uuid that this message refers
	const char *organization_uuid;
	/// Interval bytes
	uint64_t bytes;
};

/** Unpack a message
  @param msg Message to unpack
  @param bytes_update struct to save unpack
  @return 0 if success, !0 in other case
  */
static int real_sync_thread_msg_consume_unpack(json_t *msg,
			struct organization_bytes_update *bytes_update) {
	int rc = 0;
	json_error_t jerr;
	const char *monitor = NULL;

	const int json_unpack_rc = json_unpack_ex(msg, &jerr, 0,
		"{s?s,s?s,s?s}",
		MONITOR_MSG_MONITOR_KEY, &monitor,
		MONITOR_MSG_ORGANIZATION_UUID_KEY,
					&bytes_update->organization_uuid,
		MONITOR_MSG_N2KAFKA_ID_KEY, &bytes_update->n2kafka_id);

	if (json_unpack_rc != 0) {
		rdlog(LOG_ERR, "Couldn't unpack msg: %s", jerr.text);
		rc = -1;
		goto done;
	}

	if (NULL == monitor || NULL == bytes_update->organization_uuid
			|| NULL == bytes_update->n2kafka_id
			|| 0!=strcmp(monitor, "organization_received_bytes")) {
		/* this message is not for us */
		rc = -1;
		goto done;
	}

	bytes_update->bytes = int_value_of(msg, MONITOR_MSG_VALUE_KEY);

done:
	return rc;
}

/** Consume sync message
  @param organization Organization to update
  @param bytes_update struct to save unpack
  @return 0 if success, !0 in other case
  */
static void real_sync_thread_msg_consume_update(
		organization_db_entry_t *organization,
		const struct organization_bytes_update *bytes_update) {
	organization_add_other_consumed_bytes(organization,
				bytes_update->n2kafka_id, bytes_update->bytes);
}

/** Consume a real message (i.e., is not an error signal)
  @param msg Message
  @param ctx Context
  */
static void real_sync_thread_msg_consume(rd_kafka_message_t *msg,
						struct msg_consume_ctx *ctx) {
	(void)ctx;
	struct organization_bytes_update bytes_update;
	json_error_t jerr;

	memset(&bytes_update, 0, sizeof(bytes_update));
	json_t *jmsg = json_loadb(msg->payload, msg->len, 0, &jerr);
	if (NULL == jmsg) {
		rdlog(LOG_ERR, "Couldn't decode message [%.*s]: %s",
			(int)msg->len, (char *)msg->payload, jerr.text);
		return;
	}

	const int unpack_rc = real_sync_thread_msg_consume_unpack(jmsg,
								&bytes_update);
	if (0 != unpack_rc) {
		goto done;
	}

	if (0 == bytes_update.bytes || 0 == strcmp(bytes_update.n2kafka_id,
						ctx->n2kafka_id)) {
		/* We have nothing to do here */
		goto done;
	}

	organization_db_entry_t *organization = organizations_db_get(
		ctx->thread->org_db, bytes_update.organization_uuid);
	if (NULL == organization) {
		rdlog(LOG_ERR, "Couldn't locate organization %s",
			bytes_update.organization_uuid);
		goto done;
	}

	real_sync_thread_msg_consume_update(organization, &bytes_update);

	organizations_db_entry_decref(organization);

done:
	json_decref(jmsg);
}

/** Consume an error
  @param msg Error
  @param ctx Context
  */
static void sync_thread_msg_consume_err(rd_kafka_message_t *msg,
						struct msg_consume_ctx *ctx) {
	(void)ctx;
	switch(msg->err) {
	case RD_KAFKA_RESP_ERR__UNKNOWN_GROUP:
		rdlog(LOG_CRIT,
			"Error consuming: %s. Use rdkafka.group.id to set",
			rd_kafka_err2str(msg->err));
		rdlog(LOG_CRIT, "Need to restart to apply");
		/* This thread is no use anymore */
		pthread_exit(NULL);
		break;

	case RD_KAFKA_RESP_ERR__PARTITION_EOF:
		break;
	default:
		rdlog(LOG_ERR, "Error consuming: %.*s",
			(int)msg->len, (char *)msg->payload);
	}
}

/** Consume a message / error
  @param msg MEssage or error
  @param ctx Consume context
  */
static void sync_thread_msg_consume0(rd_kafka_message_t *msg,
						struct msg_consume_ctx *ctx) {
	(void)msg;
	assert_msg_consume_ctx(ctx);

	if(msg->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
		real_sync_thread_msg_consume(msg,ctx);
	} else {
		sync_thread_msg_consume_err(msg,ctx);
	}
}

/** Convenience function */
static void sync_thread_msg_consume(rd_kafka_message_t *msg, void *vctx) {
	struct msg_consume_ctx *ctx = vctx;
	assert_msg_consume_ctx(ctx);
	sync_thread_msg_consume0(msg, ctx);
}

/** Start sync thread kafka system
  @param thread Sync thread
  @param rk_opaque Opaque to set to kafka handle
  @return 0 if success, !0 in other case
  */
static int sync_thread_kafka_init(sync_thread_t *thread, void *rk_opaque) {
	char err[BUFSIZ];

	rd_kafka_conf_set_consume_cb(thread->rk_conf, sync_thread_msg_consume);
	rd_kafka_conf_set_opaque(thread->rk_conf, rk_opaque);

	thread->rk = rd_kafka_new(RD_KAFKA_CONSUMER, thread->rk_conf,
							err, sizeof(err));
	if (NULL == thread->rk) {
		rdlog(LOG_ERR, "Couldn't create rk: %s",
			err);
		/// @TODO should destroy conf?
		return -1;
	}

	thread->rk_conf = NULL;

	rd_kafka_poll_set_consumer(thread->rk);
	return 0;
}

/** De-initialize kafka system
  @param thread Sync thread
  */
static void sync_thread_kafka_done(sync_thread_t *thread) {
	rd_kafka_resp_err_t close_rc = rd_kafka_consumer_close(thread->rk);
	if (close_rc != RD_KAFKA_RESP_ERR_NO_ERROR) {
		rdlog(LOG_ERR, "Couldn't close previous consumer: %s",
						rd_kafka_err2str(close_rc));
	}

	rd_kafka_destroy(thread->rk);
}

/** Entry point for sync thread */
static void *sync_thread(void *vthread) {
	struct msg_consume_ctx ctx = {
#ifdef MSG_CONSUME_CTX_MAGIC
		.magic = MSG_CONSUME_CTX_MAGIC,
#endif

		/// @TODO does not use global config here!!
		.n2kafka_id = global_config.n2kafka_id,
		.thread = vthread,
		.in_sync = 0,
	};

	assert_sync_thread(ctx.thread);
	int rc = sync_thread_kafka_init(ctx.thread, &ctx);
	sem_post(&ctx.thread->sem);
	if (rc != 0) {
		return NULL;
	}

	while(ctx.thread->run) {
		/// @TODO end of partition message is returned. Why I can't
		/// handle it via consumer_cb?
		rd_kafka_message_t *msg;
		msg = rd_kafka_consumer_poll(ctx.thread->rk, 1000);
		if (msg) {
			sync_thread_msg_consume0(msg, &ctx);
		}
	}

	sync_thread_kafka_done(ctx.thread);

	return NULL;
}
