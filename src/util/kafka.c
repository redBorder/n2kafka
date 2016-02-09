/*
** Copyright (C) 2015 Eneo Tecnologia S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
** Based on librdkafka example
**
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

#include "util.h"
#include "parse.h"
#include "global_config.h"

#include <pthread.h>

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>

#define ERROR_BUFFER_SIZE   256
#define RDKAFKA_ERRSTR_SIZE ERROR_BUFFER_SIZE

/** Creates a new topic handler using global configuration
    @param topic_name Topic name
    @param partitioner Partitioner function
    @return New topic handler */
rd_kafka_topic_t *new_rkt_global_config(const char *topic_name,
        rb_rd_kafka_partitioner_t partitioner,char *err,size_t errsize) {
	rd_kafka_topic_conf_t *template_config = global_config.kafka_topic_conf;
	rd_kafka_topic_conf_t *my_rkt_conf
		= rd_kafka_topic_conf_dup(template_config);

	if(NULL == my_rkt_conf) {
		rdlog(LOG_ERR,"Couldn't topic_conf_dup in topic %s",topic_name);
		return NULL;
	}

	rd_kafka_topic_conf_set_partitioner_cb(my_rkt_conf, partitioner);

	rd_kafka_topic_t *ret = rd_kafka_topic_new(global_config.rk, topic_name,
		my_rkt_conf);
	if (NULL == ret) {
		strerror_r(errno, err, errsize);
		rd_kafka_topic_conf_destroy(my_rkt_conf);
	}

	return ret;
}

/**
* Message delivery report callback.
* Called once for each message.
* See rdkafka.h for more information.
*/
static void msg_delivered (rd_kafka_t *_rk RB_UNUSED,
void *payload, size_t len,
int error_code,
void *opaque RB_UNUSED, void *msg_opaque RB_UNUSED) {

	if (error_code){
		rblog(LOG_ERR,   "Message delivery failed: %s",rd_kafka_err2str(error_code));
	}else{
		rblog(LOG_DEBUG, "Message delivered (%zd bytes): %*.*s", len, (int)len,(int)len, (char *)payload);
	}
}

int32_t rb_client_mac_partitioner (const rd_kafka_topic_t *_rkt,
					const void *key __attribute__((unused)),
					size_t keylen __attribute__((unused)),
					int32_t partition_cnt,
					void *rkt_opaque,
					void *msg_opaque){
	const uint64_t client_mac = (uint64_t)(intptr_t)msg_opaque;
	if(client_mac == 0)
		return rd_kafka_msg_partitioner_random(_rkt,NULL,0,partition_cnt,rkt_opaque,msg_opaque);
	else
		return client_mac % (unsigned)partition_cnt;
}

void init_rdkafka(){
	char errstr[RDKAFKA_ERRSTR_SIZE];

	assert(global_config.kafka_conf);
	assert(global_config.kafka_topic_conf);

	if(only_stdout_output()){
		rblog(LOG_DEBUG,"No brokers and no topic specified. Output will be printed in stdout.");
		return;
	}

	rd_kafka_conf_set_dr_cb(global_config.kafka_conf, msg_delivered);
	global_config.rk = rd_kafka_new(RD_KAFKA_PRODUCER,global_config.kafka_conf,errstr,RDKAFKA_ERRSTR_SIZE);

	if(!global_config.rk){
		fatal("%% Failed to create new producer: %s",errstr);
	}

	if(global_config.debug){
		rd_kafka_set_log_level (global_config.rk, LOG_DEBUG);
	}

	if(global_config.brokers == NULL){
		fatal("%% No brokers specified");
	}

	const int brokers_res = rd_kafka_brokers_add(global_config.rk,global_config.brokers);
	if(brokers_res==0){
		fatal( "%% No valid brokers specified");
	}

	/* Security measure: If we start n2kafka while sending data, it will give a SIGSEGV */
	sleep(1); 
}

static void flush_kafka0(int timeout_ms){
	rd_kafka_poll(global_config.rk,timeout_ms);
}

void send_to_kafka(rd_kafka_topic_t *rkt,char *buf,const size_t bufsize,
                                                int flags,void *opaque) {
	int retried = 0;
	char errbuf[ERROR_BUFFER_SIZE];

	do{
		if(NULL == rkt) {
			rdlog(LOG_ERR,"Can't produce message, no topic specified");
			if(flags & RD_KAFKA_MSG_F_FREE) {
				free(buf);
			}
		}

		const int produce_ret = rd_kafka_produce(rkt,RD_KAFKA_PARTITION_UA,flags,
			buf,bufsize,NULL,0,opaque);

		if(produce_ret == 0)
			break;

		if(ENOBUFS==errno && !(retried++)){
			rd_kafka_poll(global_config.rk,5); // backpressure
		}else{
			//rdbg(LOG_ERR, "Failed to produce message: %s",rd_kafka_errno2err(errno));
			rblog(LOG_ERR, "Failed to produce message: %s",mystrerror(errno,errbuf,ERROR_BUFFER_SIZE));
			if(flags & RD_KAFKA_MSG_F_FREE)
				free(buf);
			break;
		}
	}while(1);
}

struct kafka_message_array *new_kafka_message_array(size_t size){
	const size_t memsize = sizeof(struct kafka_message_array) + size*sizeof(rd_kafka_message_t);
	struct kafka_message_array *ret = calloc(1,memsize);
	if(NULL == ret) {
		rdlog(LOG_ERR,"Error allocating kafka message array (out of memory?)");
	} else {
		ret->size = size;
		ret->msgs = (void *)&ret[1];
	}

	return ret;
}

int save_kafka_msg_in_array(struct kafka_message_array *array,char *buffer,size_t buf_size,
                                                                               void *opaque) {
	if(array->count == array->size) {
		rdlog(LOG_ERR,"Can't save msg in array: Not enough space");
		return -1;
	}

	const size_t i = array->count;
	array->msgs[i].partition = RD_KAFKA_PARTITION_UA;
	array->msgs[i].payload = buffer;
	array->msgs[i].len = buf_size;
	array->msgs[i]._private = opaque;

	array->count++;

	return 0;
}

void send_array_to_kafka(rd_kafka_topic_t *rkt, 
                                struct kafka_message_array *msgs) {
	size_t i;

	if(rkt) {
		rd_kafka_produce_batch(rkt,RD_KAFKA_PARTITION_UA,RD_KAFKA_MSG_F_FREE,
			msgs->msgs,msgs->count);
	}

	for(i=0; i<msgs->count; ++i) {
		if(msgs->msgs[i].err) {
			const char *payload = msgs->msgs[i].payload;
			int payload_len = msgs->msgs[i].len;
			const char *msg_error = rd_kafka_err2str(msgs->msgs[i].err);
			rdlog(LOG_ERR,"Couldn't produce message [%.*s]: %s",payload_len,payload,msg_error);
		}

		if(!rkt || msgs->msgs[i].err) {
			free(msgs->msgs[i].payload);
		}
	}
}


void dumb_decoder(char *buffer,size_t buf_size,
            const keyval_list_t *keyval __attribute__((unused)),
            void *listener_callback_opaque,
            void **sessionp __attribute__((unused))) {
	send_to_kafka(NULL,buffer,buf_size,RD_KAFKA_MSG_F_FREE,listener_callback_opaque);
}

void flush_kafka(){
	flush_kafka0(1000);
}

void kafka_poll(int timeout_ms){
	rd_kafka_poll(global_config.rk,timeout_ms);
}

void stop_rdkafka(){
	rdlog(LOG_INFO,"Waiting kafka handler to stop properly");

	/* Make sure all outstanding requests are transmitted and handled. */
	while (rd_kafka_outq_len(global_config.rk) > 0) {
		rd_kafka_poll(global_config.rk, 50);
	}

	rd_kafka_destroy(global_config.rk);
	while(0 != rd_kafka_wait_destroyed(5000));
}
