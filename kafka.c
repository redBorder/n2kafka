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

static rd_kafka_t *rk = NULL;
static rd_kafka_topic_t *rkt = NULL;

#define ERROR_BUFFER_SIZE   256
#define RDKAFKA_ERRSTR_SIZE ERROR_BUFFER_SIZE

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
		rblog(LOG_ERR,   "Message delivery failed: %s\n",rd_kafka_err2str(error_code));
	}else{
		rblog(LOG_DEBUG, "Message delivered (%zd bytes): %*.*s\n", len, (int)len,(int)len, (char *)payload);
	}
}

static int32_t rb_client_mac_partitioner (const rd_kafka_topic_t *_rkt,
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
		rblog(LOG_DEBUG,"No brokers and no topic specified. Output will be printed in stdout.\n");
		return;
	}

	rd_kafka_conf_set_dr_cb(global_config.kafka_conf, msg_delivered);
	rk = rd_kafka_new(RD_KAFKA_PRODUCER,global_config.kafka_conf,errstr,RDKAFKA_ERRSTR_SIZE);

	if(!rk){
		fatal("%% Failed to create new producer: %s\n",errstr);
	}

	if(global_config.debug){
		rd_kafka_set_log_level (rk, LOG_DEBUG);
	}

	if(global_config.brokers == NULL){
		fatal("%% No brokers specified\n");
	}

	const int brokers_res = rd_kafka_brokers_add(rk,global_config.brokers);
	if(brokers_res==0){
		fatal( "%% No valid brokers specified\n");
	}

	if(global_config.topic == NULL){
		fatal("%% No valid topic specified\n");
	}

	rd_kafka_topic_conf_set_partitioner_cb(global_config.kafka_topic_conf, rb_client_mac_partitioner);
	rkt = rd_kafka_topic_new(rk, global_config.topic, global_config.kafka_topic_conf);
	if(rkt == NULL){
		fatal("%% Cannot create kafka topic\n");
	}

	/* Security measure: If we start n2kafka while sending data, it will give a SIGSEGV */
	sleep(1); 
}

static void flush_kafka0(int timeout_ms){
	rd_kafka_poll(rk,timeout_ms);
}

void send_to_kafka(char *buf,const size_t bufsize,int flags,void *opaque){
	int retried = 0;
	char errbuf[ERROR_BUFFER_SIZE];

	do{
		const int produce_ret = rd_kafka_produce(rkt,RD_KAFKA_PARTITION_UA,flags,
			buf,bufsize,NULL,0,opaque);

		if(produce_ret == 0)
			break;

		if(ENOBUFS==errno && !(retried++)){
			rd_kafka_poll(rk,5); // backpressure
		}else{
			//rdbg(LOG_ERR, "Failed to produce message: %s\n",rd_kafka_errno2err(errno));
			rblog(LOG_ERR, "Failed to produce message: %s\n",mystrerror(errno,errbuf,ERROR_BUFFER_SIZE));
			if(flags | RD_KAFKA_MSG_F_FREE)
				free(buf);
			break;
		}
	}while(1);
}

void dumb_decoder(char *buffer,size_t buf_size,void *listener_callback_opaque){
	send_to_kafka(buffer,buf_size,RD_KAFKA_MSG_F_FREE,listener_callback_opaque);
}

void flush_kafka(){
	flush_kafka0(1000);
}

void kafka_poll(int timeout_ms){
	rd_kafka_poll(rk,timeout_ms);
}

void stop_rdkafka(){
	rd_kafka_destroy(rk);
	rd_kafka_topic_destroy(rkt);
}
