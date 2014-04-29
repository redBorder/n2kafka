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
#include "global_config.h"

#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>

#define READ_BUFFER_SIZE 4096
static const struct timeval READ_SELECT_TIMEVAL  = {.tv_sec = 1,.tv_usec = 0};
static const struct timeval WRITE_SELECT_TIMEVAL = {.tv_sec = 5,.tv_usec = 0};

int do_shutdown = 0;

static int createListenSocket(){
	int listenfd = global_config.proto == N2KAFKA_UDP ? socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK,0) : socket(AF_INET,SOCK_STREAM,0);
	if(listenfd==-1){
		perror("Error creating socket");
		return -1;
	}

	const int so_reuseaddr_value = 1;
	const int setsockopt_ret = setsockopt(listenfd,SOL_SOCKET, SO_REUSEADDR,&so_reuseaddr_value,sizeof(so_reuseaddr_value));
	if(setsockopt_ret < 0){
		perror("Error setting socket option");
	}

	struct sockaddr_in server_addr;
	memset(&server_addr,0,sizeof(server_addr));

	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr=htonl(INADDR_ANY);
	assert(global_config.listen_port > 0);
	server_addr.sin_port=htons(global_config.listen_port);

	const int bind_ret = bind(listenfd,(struct sockaddr *)&server_addr,sizeof(server_addr));
	if(bind_ret == -1){
		perror("Error binding socket");
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

static uint16_t get_port(const struct sockaddr_in *sa){
	return ntohs(sa->sin_port);
	
}

static void print_accepted_connection_log(const struct sockaddr_in *sa){
	char str[sizeof(INET_ADDRSTRLEN)];
	inet_ntop(AF_INET, &(sa->sin_addr), str, INET_ADDRSTRLEN);

	printf("Accepted connection from %s:%d\n",str,get_port(sa));
}

static int accept_connection(int listenfd){
	struct sockaddr addr;
	socklen_t addrlen = sizeof(addr);
	const int accept_return = accept(listenfd,&addr,&addrlen);
	//	printf("Connection established\n");
	if(accept_return==-1){
		perror("accept error");
		return accept_return;
	}else if(global_config.debug && addr.sa_family == AF_INET){
		print_accepted_connection_log((struct sockaddr_in *)&addr);
	}
	set_nonblock_flag(accept_return);
	return accept_return;
}

static int select_socket(int listenfd,struct timeval *tv){
	fd_set listenfd_set;

	FD_ZERO(&listenfd_set);
	FD_SET(listenfd,&listenfd_set);
	return select(listenfd+1,&listenfd_set,NULL,NULL,tv);
}

static int write_select_socket(int writefd,struct timeval *tv){
	fd_set writefd_set;
	FD_ZERO(&writefd_set);
	FD_SET(writefd,&writefd_set);
	return select(writefd+1,NULL,&writefd_set,NULL,tv);
}

static int receive_from_socket(int fd,char *buffer,const size_t buffer_size){
	return recv(fd,buffer,buffer_size,0);
}

/* return 1: all ok, keep trying to receive data */
/* return 0: error/end-of-data. dont keep asking for it */
static int process_data_received_from_socket(char *buffer,const int recv_result){
	if(recv_result > 0){
		if(unlikely(global_config.debug))
			fprintf(stdout,"[DEBUG] received %d data: %*.*s\n",recv_result,recv_result,recv_result,buffer);

		if(unlikely(only_stdout_output()))
			free(buffer);
		else
			send_to_kafka(buffer,recv_result);
	}else if(recv_result < 0){
		if(errno == EAGAIN){
			// printf("Socket not ready. re-trying");
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

static int send_to_socket(int fd,const char *data,int len){
	struct timeval tv = WRITE_SELECT_TIMEVAL;
	const int select_result = write_select_socket(fd,&tv);
	if(select_result > 0){
		return write(fd,data,len);
	}else if(select_result == 0){
		fprintf(stderr,"Socket not ready for writing in %ld.%6ld. Closing.\n",
			WRITE_SELECT_TIMEVAL.tv_sec,WRITE_SELECT_TIMEVAL.tv_usec);
		return select_result;
	}else{
		perror("Error writing to socket");
		return select_result;
	}
}

static void *process_data_from_socket(void *pfd){
	int fd = *(int *)pfd;
	free(pfd);
	if(NULL!=global_config.response){
		if(global_config.debug)
			printf("receiving first packet\n");
		// We will response one time, and then we will send
		// the data received starting from the next one.
		char buffer[READ_BUFFER_SIZE];
		struct timeval tv = READ_SELECT_TIMEVAL;
		select_socket(fd,&tv);
		const int recv_result = receive_from_socket(fd,buffer,READ_BUFFER_SIZE);
		if(recv_result > 0){
			if(global_config.debug)
				printf("Sending first response\n");
			const int send_ret = send_to_socket(fd,global_config.response,global_config.response_len-1);
			if(send_ret <= 0){
				perror("Cannot send to socket");
				close(fd);
				return NULL;
			}
			if(global_config.debug)
				printf("first response ok\n");
		}else{
			perror("Error receiving first amount of data");
			close(fd);
			return NULL;
		}
	}

	while(likely(!do_shutdown)){
		char *buffer = calloc(READ_BUFFER_SIZE,sizeof(char));
		// struct timeval timeout = {.tv_sec = 5,.tv_usec = 0};

		const int recv_result = receive_from_socket(fd,buffer,READ_BUFFER_SIZE);
		if(0==process_data_received_from_socket(buffer,recv_result))
			break;
	}

	close(fd);
	return NULL;
}

static void main_tcp_loop(int listenfd){
	pthread_attr_t pthread_attr;

	pthread_attr_init(&pthread_attr);
	pthread_attr_setdetachstate(&pthread_attr, PTHREAD_CREATE_DETACHED);

	while(!do_shutdown){
		struct timeval tv = {.tv_sec = 1,.tv_usec = 0};
		int connection_fd = 0;
		if(likely(!do_shutdown)){
			int select_result = select_socket(listenfd,&tv);
			if(select_result==-1 && errno!=EINTR){ /* NOT INTERRUPTED */
				perror("listen select error");
			}else if(select_result>0){
				connection_fd = accept_connection(listenfd);
			}else{
				// printf("timeout\n");
			}
		}

		if(connection_fd > 0){
			int *pconnection_fd = malloc(sizeof(int));
			*pconnection_fd = connection_fd;
			pthread_t thread;
			pthread_create(&thread,&pthread_attr,process_data_from_socket,pconnection_fd);
		}

		connection_fd = 0;
	}

	pthread_attr_destroy(&pthread_attr);
}

static void *main_consumer_loop_udp(void *_thread_info){
	struct udp_thread_info *thread_info = _thread_info;
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

static void main_udp_loop(int listenfd){
	/* Lots of threads listening  and processing*/
	unsigned int i;
	struct udp_thread_info udp_thread_info;
	udp_thread_info.listenfd = listenfd;
	assert(global_config.threads>0);
	pthread_t *threads = malloc(sizeof(threads[0])*global_config.udp_threads);

	if(0 != createListenSocketMutex(&udp_thread_info.listenfd_mutex))
		exit(-1);

	for(i=0;i<global_config.udp_threads;++i)
		pthread_create(&threads[i],NULL,main_consumer_loop_udp,&udp_thread_info);

	for(i=0;i<global_config.udp_threads;++i)
		pthread_join(threads[i],NULL);
	
	pthread_mutex_destroy(&udp_thread_info.listenfd_mutex);
	
	free(threads);
}

void main_loop(){
	int listenfd = createListenSocket();
	if(listenfd == -1)
		exit(-1);

	if(global_config.proto == N2KAFKA_UDP){
		main_udp_loop(listenfd);
	}else{
		main_tcp_loop(listenfd);
	}

	printf("Closing listening socket.\n");
	close(listenfd);
}
