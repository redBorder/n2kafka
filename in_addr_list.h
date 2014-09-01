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
