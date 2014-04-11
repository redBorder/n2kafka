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

#include <stdint.h>
#include <stdbool.h>

struct n2kafka_config{
#define N2KAFKA_TCP 1
#define N2KAFKA_UDP 2
    int proto;
    unsigned int threads;
    char *format;
    uint16_t listen_port;
    char *topic;
    char *brokers;
};

extern struct n2kafka_config global_config;

void init_global_config();

void parse_config(const char *config_file_path);

void free_global_config();
