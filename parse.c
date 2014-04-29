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
#include "util.h"

#include <jansson.h>
#include <librdkafka/rdkafka.h>

#include <pthread.h>
#include <string.h>
#include <assert.h>


static struct message_list_element *object_to_list_element(const json_t *json_object){
	struct message_list_element *elm = calloc(1,sizeof(*elm));
	if(likely(elm)){
		elm->msg = json_dumps(json_object,JSON_COMPACT);
		elm->msg_len = strlen(elm->msg);
	}else{
		rblog(LOG_ERR,"Memory error\n");
	}
	return elm;
}

static void add_object_to_list(message_list *list,const json_t *json_object){
	assert(list);
	assert(json_object);

	struct message_list_element *elm = object_to_list_element(json_object);
	if(likely(elm))
		SLIST_INSERT_HEAD(list,elm,slist_entry);

}

static void parse_array(message_list *list,const json_t *json_array){
	unsigned int i;
	for(i = 0; i < json_array_size(json_array); i++){
		const json_t *object = json_array_get(json_array,i);
		if(likely(object)){
			add_object_to_list(list,object);
		}else{
			rblog(LOG_ERR,"Error in json_array get, index %d",i);
		}
	}
}

message_list json_array_to_message_list(const char *str){
	json_error_t error;
	json_t * json_object = json_loads(str,0,&error);
	message_list list = new_message_list();

	if(unlikely(!json_object)){
		rblog(LOG_ERR,"json error on line %d: %s",error.line, error.text);
	}else if(!json_is_array(json_object)){
		add_object_to_list(&list,json_object);
	}else{
		parse_array(&list,json_object);
	}

	json_decref(json_object);

	return list;
}