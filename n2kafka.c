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

#include "version.h"
#include "engine.h"
#include "kafka.h"
#include "global_config.h"

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <jansson.h>

#define DEFAULT_PORT 2057

static int do_reload = 0;

static void shutdown_process(){
	printf("Exiting\n");
	do_shutdown=1;
}

static void show_usage(const char *progname){
	fprintf(stdout,"n2kafka version %s-%s\n",n2kafka_version,n2kafka_revision);
	fprintf(stdout,"Usage: %s <config_file>\n",progname);
	fprintf(stdout,"\n");
	fprintf(stdout,
	        "Where <config_file> is a json file that can contains the \n");
	fprintf(stdout,"the next configurations:\n");
	
	fprintf(stdout,"{\n");
	fprintf(stdout,"\t\"listeners:\":[\n");
	fprintf(stdout,
	        "\t\t{\"proto\":\"http\",\"port\":2057,\"mode\":\"(1)\","
	        "\"threads\":20}\n");
	fprintf(stdout,
	        "\t\t{\"proto\":\"tcp\",\"port\":2056,"
	        "\"tcp_leepalive\":true,\"mode\"},\n");
	fprintf(stdout,"\t\t{\"proto\":\"udp\",\"port\":2058,\"threads\":20}\n");
	fprintf(stdout,"\t],\n");
	fprintf(stdout,"\t\"brokers\":\"kafka brokers\",\n");
	fprintf(stdout,"\t\"topic\":\"kafka topic\",\n");
	fprintf(stdout,"\t\"rdkafka.socket.max.fails\":\"3\",\n");
	fprintf(stdout,"\t\"rdkafka.socket.keepalive.enable\":\"true\",\n");
	fprintf(stdout,"\t\"blacklist\":[\"192.168.101.3\"]\n");
	fprintf(stdout,"}\n\n");
	fprintf(stdout,"(1) Modes can be:\n");
	fprintf(stdout,
	        "\tthread_per_connection: Creates a thread for each connection.\n");
	fprintf(stdout,"\t\tThread argument will be ignored in this mode\n");
	fprintf(stdout,
	        "\tselect,poll,epoll: Fixed number of threads (with threads "
	        "parameter) manages all connections\n");
}

static int is_asking_help(const char *param){
	return 0==strcmp(param,"-h") || 0==strcmp(param,"--help");
}

static void sighup_proc(int signum __attribute__((unused))){
	do_reload = 1;
}

int main(int argc,char *argv[]){
	if(argc != 2 || is_asking_help(argv[1])){
		show_usage(argv[0]);
		exit(1);
	}

	init_global_config();
	parse_config(argv[1]);

	signal(SIGINT,shutdown_process);
	signal(SIGHUP,sighup_proc);

	if(!only_stdout_output())
		init_rdkafka();

	while(!do_shutdown){
		kafka_poll(1000 /* ms */);
		if(do_reload){
			reload_listeners(&global_config);
			do_reload = 0;
		}
	}

	free_global_config();

	return 0;
}
