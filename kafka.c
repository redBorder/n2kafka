/*
** Copyright (C) 2014 Eneo Tecnologia S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
** Based on librdkafka example
**
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

#include "util.h"
#include "parse.h"
#include "config.h"

#include <pthread.h>
#include <librdkafka/rdkafka.h>

#include <string.h>
#include <stdlib.h>
#include <errno.h>

static rd_kafka_t *rk = NULL;
static rd_kafka_topic_t *rkt = NULL;

#define RDKAFKA_ERRSTR_SIZE 512

#define RB_UNUSED __attribute__((unused))

/**
* Message delivery report callback.
* Called once for each message.
* See rdkafka.h for more information.
*/
static void msg_delivered (rd_kafka_t *rk RB_UNUSED,
void *payload RB_UNUSED, size_t len RB_UNUSED,
int error_code,
void *opaque RB_UNUSED, void *msg_opaque RB_UNUSED) {

	if (error_code){
		fprintf(stderr, "%% Message delivery failed: %s\n",rd_kafka_err2str(error_code));
	}else{
		// fprintf(stderr, "%% Message delivered (%zd bytes)\n", len);
	}
}


void init_rdkafka(){
	char errstr[RDKAFKA_ERRSTR_SIZE];

	rd_kafka_conf_t *conf = rd_kafka_conf_new();
	rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();

	rd_kafka_conf_set_dr_cb(conf, msg_delivered);
	rk = rd_kafka_new(RD_KAFKA_PRODUCER,conf,errstr,RDKAFKA_ERRSTR_SIZE);

	if(!rk){
		fprintf(stderr,
		"%% Failed to create new producer: %s\n",errstr);
		exit(1);
	}

	if(global_config.brokers == NULL){
		fprintf(stderr,"%% No brokers specified\n");
		exit(1);
	}

	const int brokers_res = rd_kafka_brokers_add(rk,global_config.brokers);
	if(brokers_res==0){
		fprintf(stderr, "%% No valid brokers specified\n");
		exit(1);
	}

	if(global_config.topic == NULL){
		fprintf(stderr,"%% No valid brokers specified\n");
		exit(1);
	}

	rkt = rd_kafka_topic_new(rk, global_config.topic, topic_conf);
	if(rkt == NULL){
		fprintf(stderr,"%% Cannot create kafka topic\n");
		exit(1);
	}
}

static void flush_kafka0(int timeout_ms){
	rd_kafka_poll(rk,timeout_ms);
}

static void send_to_kafka0(char *buf,const size_t bufsize,int msgflags){
	const int produce_ret = rd_kafka_produce(rkt,RD_KAFKA_PARTITION_UA,msgflags,
		buf,bufsize,NULL,0,NULL);

	if(produce_ret == -1){
		//fprintf(stderr, "Failed to produce message: %s\n",rd_kafka_errno2err(errno));
		fprintf(stderr, "Failed to produce message: %s\n",strerror(errno));
		if(msgflags | RD_KAFKA_MSG_F_FREE)
			free(buf);
	}
}

void send_to_kafka(char *buf,const size_t bufsize){
	send_to_kafka0(buf,bufsize,RD_KAFKA_MSG_F_FREE);
}

void flush_kafka(){
	flush_kafka0(1000);
}

void stop_rdkafka(){
	rd_kafka_destroy(rk);
}