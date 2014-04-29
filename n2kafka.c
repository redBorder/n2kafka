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
#include "kafka.h"
#include "global_config.h"

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <jansson.h>

#define DEFAULT_PORT 2057

static void shutdown_process(){
	printf("Exiting\n");
	do_shutdown=1;
}

static void show_usage(const char *progname){
	fprintf(stdout,"Usage: %s <config_file>\n",progname);
	fprintf(stdout,"\n");
	fprintf(stdout,"Where <config_file> is a json file that can contains the \n");
	fprintf(stdout,"the next configurations:\n");
	
	fprintf(stdout,"{\n");
	fprintf(stdout,"\t\"proto\":\"tcp\"/\"udp\",");
	fprintf(stdout,"\t\"port\":<listen port>\n");
	fprintf(stdout,"\t\"brokers\":\"kafka brokers\"\n");
	fprintf(stdout,"\t\"topic\":\"kafka topic\"\n");
	fprintf(stdout,"\t\"threads\":<number_of_threads>\n");
	fprintf(stdout,"}\n");
}

static int is_asking_help(const char *param){
	return 0==strcmp(param,"-h") || 0==strcmp(param,"--help");
}

int main(int argc,char *argv[]){
	if(argc != 2 || is_asking_help(argv[1])){
		show_usage(argv[0]);
		exit(1);
	}

	parse_config(argv[1]);

	signal(SIGINT,shutdown_process);

	if(!only_stdout_output())
		init_rdkafka();
	main_loop();
	if(!only_stdout_output()){
		flush_kafka();
		stop_rdkafka();
	}

	free_global_config();

	return 0;
}
