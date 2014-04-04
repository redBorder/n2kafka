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

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define DEFAULT_PORT 2057

static void shutdown_process(){
	printf("Exiting\n");
	do_shutdown=1;
}

int main(void){
	struct thread_info thread_info;
	memset(&thread_info,0,sizeof(thread_info));

	signal(SIGINT,shutdown_process);
	main_loop(&thread_info);

	/* safety test */
	if(0!=pthread_mutex_trylock(&thread_info.listenfd_mutex)){
		perror("Error locking mutex, when no other lock must be here.");
	}else{
		pthread_mutex_unlock(&thread_info.listenfd_mutex);
	}

	close(thread_info.listenfd);
	pthread_mutex_destroy(&thread_info.listenfd_mutex);

	return 0;
}