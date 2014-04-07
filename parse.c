/*
** Copyright (C) 2014 Eneo Tecnologia S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
** Based on rdkafka example
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

#include "parse.h"

#include <jansson.h>
#include <librdkafka/rdkafka.h>

#include <pthread.h>

static struct message_list_element *object_to_list_element(const json_t *json_object){
	struct message_list_element *elm = calloc(1,sizeof(*elm));
	elm->msg = json_dumps(json_object,JSON_COMPACT);
	return elm;
}

message_list single_object_list(const json_t *json_object){
	message_list list = new_message_list();

	struct message_list_element *elm = object_to_list_element(json_object);
	if(elm){
		SLIST_INSERT_HEAD(&list,elm,slist_entry);
	}

	return list;
}

static message_list parse_array(const json_t *json_array){
	message_list list = new_message_list();

	unsigned int i;
	for(i = 0; i < json_array_size(json_array); i++){
		const json_t *object = json_array_get(json_array,i);
		struct message_list_element *elm = object_to_list_element(object);
		if(elm)
			SLIST_INSERT_HEAD(&list,elm,slist_entry);
	}

	return list;
}

message_list json_array_to_message_list(const char *str){
	json_error_t error;
	json_t * json_object = json_loads(str,0,&error);
	message_list list = new_message_list();

	if(!json_object){
		fprintf(stderr,"json error on line %d: %s",error.line, error.text);
	}else if(!json_is_array(json_object)){
		list = single_object_list(json_object);
	}else{
		list = parse_array(json_object);
	}

	return list;
}