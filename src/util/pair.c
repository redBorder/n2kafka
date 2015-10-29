/*
** Copyright (C) 2015 Eneo Tecnologia S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
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

#include "pair.h"

#include <string.h>

void add_key_value_pair(keyval_list_t *list,struct pair *pair) {
	TAILQ_INSERT_TAIL(list,pair,entry);
}

const char *valueof(const keyval_list_t *list,const char *key) {
	struct pair *pair = NULL;
	TAILQ_FOREACH(pair, list, entry) {
		if(0 == strcmp(key,pair->key)) {
			return pair->value;
		}
	}

	return NULL;
}