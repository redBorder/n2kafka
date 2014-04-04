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

#include "engine.h"
#include <sys/time.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

int do_shutdown = 0;

static int createListenSocket(const struct listensocket_info *listensocket_info){
	int listenfd = socket(AF_INET,SOCK_STREAM,0);
	if(listenfd==-1){
		perror("Error creating socket: ");
		return -1;
	}

	struct sockaddr_in server_addr;
	memset(&server_addr,0,sizeof(server_addr));

	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr=htonl(INADDR_ANY);
	server_addr.sin_port=htons(listensocket_info->listen_port);

	const int bind_ret = bind(listenfd,(struct sockaddr *)&server_addr,sizeof(server_addr));
	if(bind_ret == -1){
		perror("Error binding socket: ");
		close(listenfd);
		return -1;
	}
	
	const int listen_ret = listen(listenfd,SOMAXCONN);
	if(listen_ret == -1){
		perror("Error listen()");
		return -1;
	}

	return listenfd;
}

static int createListenSocketMutex(pthread_mutex_t *mutex){
	const int init_returned = pthread_mutex_init(mutex,NULL);
	if(init_returned!=0)
		perror("Error creating mutex: ");
	return init_returned;
}

int accept_connection(int listenfd){
	const int accept_return = accept(listenfd,NULL,NULL);
	if(accept_return==-1)
		perror("accept error: ");
	return accept_return;
}

int select_socket(int listenfd,struct timeval *tv){
	fd_set listenfd_set;

	FD_ZERO(&listenfd_set);
	FD_SET(listenfd,&listenfd_set);
	return select(listenfd+1,&listenfd_set,NULL,NULL,(struct timeval *)tv);
}

void read_from_socket(int fd){
	struct timeval tv = {.tv_sec = 5,.tv_usec = 0};
	const int select_result = select_socket(fd,&tv);
	if(select_result>1){
		char buffer[4096] = {'\0'};
		const int recv_result = recv(fd,buffer,4096,0);
		// @TODO: Recv_result > buffer?
		// @TODO: assert that starts with '{' and end with '}''
		// @TODO: How many messages have coming?
		if(recv_result > 0){
			printf("received data: [%d bytes][%s]\n", recv_result,buffer);
			
		}else if(recv_result < 0){
			perror("Recv error: ");
		}
		// else: end of connection
		close(fd);
	}
}

void main_loop(struct listensocket_info *listensocket_info){
	struct thread_info thread_info;

	thread_info.listenfd = createListenSocket(listensocket_info);
	if(thread_info.listenfd == -1)
		exit(-1);

	if(0 != createListenSocketMutex(&thread_info.listenfd_mutex))
		exit(-1);

	while(!do_shutdown){
		struct timeval tv = {.tv_sec = 1,.tv_usec = 0};
		int connection_fd = 0;
		pthread_mutex_lock(&thread_info.listenfd_mutex);
		int select_result = select_socket(thread_info.listenfd,&tv);
		if(select_result==-1 && errno!=EINTR){
			perror("listen select error: ");
		}else if(select_result>0){
			connection_fd = accept_connection(thread_info.listenfd);
		}else{
			printf("timeout\n");
		}
		pthread_mutex_unlock(&thread_info.listenfd_mutex);

		if(connection_fd>0){
			read_from_socket(connection_fd);
		}
	}
}
