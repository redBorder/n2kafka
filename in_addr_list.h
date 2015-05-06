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
#include <netinet/in.h>

typedef struct in_addr_list_s in_addr_list_t; /* FW DECLARATION */

/// Init a sockaddr_in list.
in_addr_list_t *in_addr_list_new();

/// Add an address to list.
void in_addr_list_add(in_addr_list_t *list,const struct in_addr *addr);

/// Check if an addr is in list.
int in_addr_list_contains(const in_addr_list_t *list,const struct in_addr *addr);

/// Deallocate a list.
void in_addr_list_done(in_addr_list_t *list);
