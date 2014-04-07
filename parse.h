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

#include <jansson.h>

#include <sys/queue.h>
#include <stdlib.h>

struct message_list_element{
	char *msg;
	SLIST_ENTRY(message_list_element) slist_entry;
};

typedef SLIST_HEAD(,message_list_element) message_list;

static message_list  __attribute__((unused)) new_message_list(){
	message_list list;
	SLIST_INIT(&list);
	return list;
}

message_list single_object_list(const json_t *json_object);

message_list json_array_to_message_list(const char *str);
