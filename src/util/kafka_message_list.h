#pragma once

#include <librdkafka/rdkafka.h>
#include <sys/queue.h>

struct rd_kafka_message_queue_elm_s;

/** Queue element with one rdkafka message */
typedef struct rd_kafka_message_queue_elm_s rd_kafka_message_queue_elm_t;

/** Kafka message queue */
typedef struct rd_kafka_message_queue_s {
	/** Number of elements */
	size_t count;
	/** Actual list */
	TAILQ_HEAD(,rd_kafka_message_queue_elm_s) list;
} rd_kafka_message_queue_t;

#define rd_kafka_msg_q_size(q) ((q)->count)

/** Init a message queue
	@param q Queue */
void rd_kafka_msg_q_init(rd_kafka_message_queue_t *q);

/** Add a message to the message queue
	@param q Queue
	@param msg Message
	@note The message will be copied
	@return 1 if ok, 0 if no memory avalable */
int rd_kafka_msg_q_add(rd_kafka_message_queue_t *q,
	const rd_kafka_message_t *msg);

/** Dump messages to an allocated messages array. It is
	suppose to be able to hold as many messages as
	rd_kafka_msg_q_size(q)
	@param q Queue
	@param msgs Messages to dump to
	@note after this call, queue will be empty */
void rd_kafka_msg_q_dump(rd_kafka_message_queue_t *q,
	rd_kafka_message_t *msgs);
