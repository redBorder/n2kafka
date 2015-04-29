/*
** Copyright (C) 2014 Eneo Tecnologia S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
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

#pragma once

#include "config.h"

#include "rb_mse.h"
#include "kafka.h"
#include "in_addr_list.h"

#include <stdint.h>
#include <stdbool.h>
#include <sys/queue.h>
#include <librdkafka/rdkafka.h>

struct json_t;
struct listener;
typedef void (*listener_callback)(char *buffer,size_t buf_size,void *listener_callback_opaque);
typedef struct listener* (*listener_creator)(struct json_t *config,
                        listener_callback cb,void *cb_opaque,
                        char *err,size_t errsize);
typedef void (*listener_join)(void *listener_private);
// @TODO we need this callback to split data acquiring || data processing
// typedef void (*data_process)(void *data_process_private,const char *buffer,size_t bsize);
typedef void (*listener_reload)(void *listener_private);
struct listener{
    void *private;
    void *callback_opaque;
    listener_callback callback;
    listener_creator create;
    listener_join join;
    listener_reload reload;
    LIST_ENTRY(listener) entry;
};

typedef LIST_HEAD(,listener) listener_list;

struct n2kafka_config{
#ifdef HAVE_LIBMICROHTTPD
#define N2KAFKA_HTTP 3
#endif
    char *format;

    char *topic;
    char *brokers;

    rd_kafka_conf_t *kafka_conf;
    rd_kafka_topic_conf_t *kafka_topic_conf;

    in_addr_list_t *blacklist;

    struct mse_config mse;

    char *response;
    int response_len;

    listener_list listeners;

    struct json_t *stream_enrichment;

    bool debug;
};

extern struct n2kafka_config global_config;

static inline bool only_stdout_output(){
	return global_config.debug && !global_config.brokers && !global_config.topic;
}

void init_global_config();

void parse_config(const char *config_file_path);

void reload_listeners(struct n2kafka_config *config);

void free_global_config();