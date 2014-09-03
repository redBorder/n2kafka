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

#include "in_addr_list.h"

#include <sys/queue.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>


typedef struct in_addr_list_node_s{
	struct in_addr addr;
	LIST_ENTRY(in_addr_list_node_s) entry;
}in_addr_list_node_t;

typedef LIST_HEAD(,in_addr_list_node_s) in_addr_list_head_t;

struct in_addr_list_s{
	in_addr_list_head_t head;
};

/// Return the syslist from sockadd_in_list_t
#define syslist(list) (&(list)->head)

/// Init a sockaddr_in list.
in_addr_list_t *in_addr_list_new(){
	in_addr_list_t *list = calloc(1,sizeof(*list));
	LIST_INIT(syslist(list));
	return list;
}

/// Add an address to list.
void in_addr_list_add(in_addr_list_t *list,const struct in_addr *addr){
	in_addr_list_node_t *node = calloc(1,sizeof(*node));

	memcpy(&node->addr,addr,sizeof(*addr));

	LIST_INSERT_HEAD(syslist(list),node,entry);
}

/// Check if an addr is in list.
int in_addr_list_contains(const in_addr_list_t *list,const struct in_addr *addr){
	in_addr_list_node_t *n=NULL;
	for(n=LIST_FIRST(syslist(list));n!=NULL;n=LIST_NEXT(n,entry)){
		if(0==memcmp(addr,&n->addr,sizeof(n->addr)))
			return 1;
	}
	return 0;
}

/// Deallocate a list.
void in_addr_list_done(in_addr_list_t *list){
	in_addr_list_node_t *n=NULL;
	while((n = LIST_FIRST(syslist(list)))){
		LIST_REMOVE(n,entry);
		free(n);
	}
}
