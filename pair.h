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

#pragma once

#include <sys/queue.h>

struct pair {
	const char *key;
	const char *value;
	TAILQ_ENTRY(pair) entry;
};

typedef TAILQ_HEAD(,pair) keyval_list_t;

void add_key_value_pair(keyval_list_t *list,struct pair *pair);
const char *valueof(keyval_list_t *list,const char *key);
