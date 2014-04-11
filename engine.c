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

#include "util.h"

#include "engine.h"
#include "parse.h"
#include "kafka.h"
#include "config.h"

#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>

#define READ_BUFFER_SIZE 4096



int do_shutdown = 0;

static int createListenSocket(){
	int listenfd = global_config.proto == N2KAFKA_UDP ? socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK,0) : socket(AF_INET,SOCK_STREAM,0);
	if(listenfd==-1){
		perror("Error creating socket: ");
		return -1;
	}

	struct sockaddr_in server_addr;
	memset(&server_addr,0,sizeof(server_addr));

	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr=htonl(INADDR_ANY);
	assert(global_config.listen_port > 0);
	server_addr.sin_port=htons(global_config.listen_port);

	const int bind_ret = bind(listenfd,(struct sockaddr *)&server_addr,sizeof(server_addr));
	if(bind_ret == -1){
		perror("Error binding socket: ");
		close(listenfd);
		return -1;
	}
	
	if(global_config.proto != N2KAFKA_UDP){
		const int listen_ret = listen(listenfd,SOMAXCONN);
		if(listen_ret == -1){
			perror("Error listen()");
			close(listenfd);
			return -1;
		}
	}
	
	printf("Listening socket created successfuly\n");
	return listenfd;
}

static int createListenSocketMutex(pthread_mutex_t *mutex){
	const int init_returned = pthread_mutex_init(mutex,NULL);
	if(init_returned!=0)
		perror("Error creating mutex: ");
	return init_returned;
}

static void set_nonblock_flag(int fd){
	int flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int accept_connection(int listenfd){
	const int accept_return = accept(listenfd,NULL,0);
	//	printf("Connection established\n");
	if(accept_return==-1){
		perror("accept error: ");
		return accept_return;
	}
	set_nonblock_flag(accept_return);
	return accept_return;
}

static int select_socket(int listenfd,struct timeval *tv){
	fd_set listenfd_set;

	FD_ZERO(&listenfd_set);
	FD_SET(listenfd,&listenfd_set);
	return select(listenfd+1,&listenfd_set,NULL,NULL,(struct timeval *)tv);
}

static int receive_from_socket(int fd,char *buffer,const size_t buffer_size){
	return recv(fd,buffer,buffer_size,0);
}

/* return 1: all ok, keep trying to receive data */
/* return 0: error/end-of-data. dont keep asking for it */
static int process_data_received_from_socket(char *buffer,const int recv_result){
	if(recv_result > 0){
		send_to_kafka(buffer,recv_result);
	}else if(recv_result < 0){
		if(errno == EAGAIN){
			usleep(1000);
		}else{
			perror("Recv error: ");
			return 0;
		}

		free(buffer);
	}else{ /* recv_result == 0 */
		free(buffer);
		return 0;
	}
	return 1;
}

static void process_data_from_socket(int fd){
	while(likely(!do_shutdown)){
		char *buffer = calloc(READ_BUFFER_SIZE,sizeof(char));
		// struct timeval timeout = {.tv_sec = 5,.tv_usec = 0};

		const int recv_result = receive_from_socket(fd,buffer,READ_BUFFER_SIZE);
		if(0==process_data_received_from_socket(buffer,recv_result))
			break;
	}
}

static void *main_consumer_loop(void *_thread_info){
	struct thread_info *thread_info = _thread_info;
	while(!do_shutdown){
		struct timeval tv = {.tv_sec = 1,.tv_usec = 0};
		int connection_fd = 0;
		pthread_mutex_lock(&thread_info->listenfd_mutex);
		if(likely(!do_shutdown)){
			int select_result = select_socket(thread_info->listenfd,&tv);
			if(select_result==-1 && errno!=EINTR){ /* NOT INTERRUPTED */
				perror("listen select error");
			}else if(select_result>0){
				connection_fd = accept_connection(thread_info->listenfd);
			}else{
				// printf("timeout\n");
			}
		}
		pthread_mutex_unlock(&thread_info->listenfd_mutex);

		if(connection_fd > 0){
			process_data_from_socket(connection_fd);
			close(connection_fd);
		}

		connection_fd = 0;
	}

	return NULL;
}

static void *main_consumer_loop_udp(void *_thread_info){
	struct thread_info *thread_info = _thread_info;
	while(!do_shutdown){
		int recv_result = 0;
		struct timeval tv = {.tv_sec = 1,.tv_usec = 0};
		char *buffer = calloc(READ_BUFFER_SIZE,sizeof(char));
		pthread_mutex_lock(&thread_info->listenfd_mutex);
		if(likely(!do_shutdown)){
			int select_result = select_socket(thread_info->listenfd,&tv);
			if(select_result==-1 && errno!=EINTR){ /* NOT INTERRUPTED */
				perror("listen select error");
			}else if(select_result>0){
				recv_result = receive_from_socket(thread_info->listenfd,buffer,READ_BUFFER_SIZE);
			}
		}
		pthread_mutex_unlock(&thread_info->listenfd_mutex);

		process_data_received_from_socket(buffer,recv_result);
	}

	return NULL;
}

void main_loop(){
	struct thread_info thread_info;

	thread_info.listenfd = createListenSocket();
	if(thread_info.listenfd == -1)
		exit(-1);

	if(0 != createListenSocketMutex(&thread_info.listenfd_mutex))
		exit(-1);

	assert(global_config.threads>0);
	pthread_t *threads = malloc(sizeof(threads[0])*global_config.threads);

	unsigned int i=0;
	void *consumer_fn = global_config.proto == N2KAFKA_UDP ? main_consumer_loop_udp : main_consumer_loop;
	for(i=0;i<global_config.threads;++i)
		pthread_create(&threads[i],NULL,consumer_fn,&thread_info);

	for(i=0;i<global_config.threads;++i)
		pthread_join(threads[i],NULL);

	free(threads);

	/* safety test */
	if(0!=pthread_mutex_trylock(&thread_info.listenfd_mutex)){
		perror("Error locking mutex, when no other lock must be here.");
	}else{
		pthread_mutex_unlock(&thread_info.listenfd_mutex);
	}

	close(thread_info.listenfd);
	pthread_mutex_destroy(&thread_info.listenfd_mutex);
}
