
#include "kafka_message_list.h"

#include <stdlib.h>
#include <string.h>

struct rd_kafka_message_queue_elm_s {
	/** Kafka message */
	rd_kafka_message_t msg;
	/** List entry */
	TAILQ_ENTRY(rd_kafka_message_queue_elm_s) list_entry;
};

void rd_kafka_msg_q_init(rd_kafka_message_queue_t *q) {
	q->count = 0;
	TAILQ_INIT(&q->list);
}

int rd_kafka_msg_q_add(rd_kafka_message_queue_t *q,
                    const rd_kafka_message_t *msg) {
	rd_kafka_message_queue_elm_t *elm = malloc(sizeof(*elm));
	if(elm) {
		++q->count;
		memcpy(&elm->msg,msg,sizeof(msg[0]));
		TAILQ_INSERT_TAIL(&q->list,elm,list_entry);
	}

	return elm != NULL;
}

void rd_kafka_msg_q_dump(rd_kafka_message_queue_t *q,
	rd_kafka_message_t *msgs) {

	rd_kafka_message_queue_elm_t *elm = NULL;
	size_t i = 0;

	while((elm = TAILQ_FIRST(&q->list))) {
		TAILQ_REMOVE(&q->list,elm,list_entry);
		memcpy(&msgs[i++],&elm->msg,sizeof(msgs[0]));
		free(elm);
	}

	q->count = 0;
}