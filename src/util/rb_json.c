/*
**
** Copyright (c) 2014, Eneo Tecnologia
** Author: Eugenio Perez <eupm90@gmail.com>
** All rights reserved.
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

#include "rb_json.h"

#include <string.h>

int json_object_update_missing_copy(json_t *dst, const json_t *src) {
	const char *key=NULL;
	const json_t *value = NULL;
	/* We promise that we will not modify this */
	json_t *my_src = NULL;

	if(!json_is_object(src) || !json_is_object(dst))
		return -1;

	memcpy(&my_src,&src,sizeof(my_src));

	json_object_foreach(my_src,key,value) {
		if(NULL == json_object_get(dst,key)){
			json_t *new_json = json_deep_copy(value);
			json_object_set_new_nocheck(dst,key,new_json);
		}
	}

	return 0;
}