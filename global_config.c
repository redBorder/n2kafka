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
#include "global_config.h"
#include "librd/rdfile.h"

#include <string.h>
#include <jansson.h>

#define CONFIG_PROTO_KEY "proto"
#define CONFIG_THREADS_KEY "threads"
#define CONFIG_TOPIC_KEY "topic"
#define CONFIG_BROKERS_KEY "brokers"
#define CONFIG_PORT_KEY "port"
#define CONFIG_DEBUG_KEY "debug"
#define CONFIG_RESPONSE_KEY "response"
#define CONFIG_KAFKA_KEY "kafka_config"

#define CONFIG_PROTO_TCP "tcp"
#define CONFIG_PROTO_UDP "udp"
#define PROTO_ERROR "Proto must be either TCP or UDP"


struct n2kafka_config global_config;

void init_global_config(){
	memset(&global_config,0,sizeof(global_config));
	global_config.kafka_conf = rd_kafka_conf_new();
	global_config.kafka_topic_conf = rd_kafka_topic_conf_new();
	rb_debug_set_debug_level(LOG_ERR);
}

static const char *assert_json_string(const char *key,const json_t *value){
	if(!json_is_string(value)){
		fatal("%s value must be a string in config file\n",key);
	}
	return json_string_value(value);
}

static int assert_json_integer(const char *key,const json_t *value){
	if(!json_is_integer(value)){
		fatal("%s value must be an integer in config file\n",key);
	}
	return json_integer_value(value);
}

static void parse_response(const char *key,const json_t *value){
	const char *filename = assert_json_string(key,value);
	global_config.response = rd_file_read(filename,&global_config.response_len);
    if(global_config.response == NULL)
    	fatal("Cannot open response file %s\n",assert_json_string(key,value));
}

static void parse_debug(const char *key,const json_t *value){
	global_config.debug = assert_json_integer(key,value);
	if(global_config.debug)
		rb_debug_set_debug_level(LOG_DEBUG);
}

static void parse_kafka_config(const char *key,const json_t *jvalue){
	// Extracted from Magnus Edenhill's kafkacat 
	char *value = strdup(assert_json_string(key,jvalue));
	
	char *name, *val;
	rd_kafka_conf_res_t res;
	char errstr[512];

	name = value;
	if (!(val = strchr(name, '='))) {
		fatal("%% Expected \""CONFIG_KAFKA_KEY"\":\"property=value, not %s, ", name);
		exit(1);
	}

	*val = '\0';
	val++;

	res = RD_KAFKA_CONF_UNKNOWN;
	/* Try "topic." prefixed properties on topic
	 * conf first, and then fall through to global if
	 * it didnt match a topic configuration property. */
	if (!strncmp(name, "topic.", strlen("topic.")))
		res = rd_kafka_topic_conf_set(global_config.kafka_topic_conf,
					      name+strlen("topic."),
					      val,errstr,sizeof(errstr));

	if (res == RD_KAFKA_CONF_UNKNOWN)
		res = rd_kafka_conf_set(global_config.kafka_conf, name, val,
					errstr, sizeof(errstr));

	if (res != RD_KAFKA_CONF_OK)
		fatal("%s", errstr);

	free(value);
}

static void parse_config_keyval(const char *key,const json_t *value){
	if(!strcasecmp(key,CONFIG_TOPIC_KEY)){
		global_config.topic = strdup(assert_json_string(key,value));
	}else if(!strcasecmp(key,CONFIG_BROKERS_KEY)){
		global_config.brokers = strdup(assert_json_string(key,value));
	}else if(!strcasecmp(key,CONFIG_PROTO_KEY)){
		const char *proto = assert_json_string(key,value);
		if(!strcmp(proto,CONFIG_PROTO_TCP)){
			global_config.proto = N2KAFKA_TCP;
		}else if(!strcmp(proto,CONFIG_PROTO_UDP)){
			global_config.proto = N2KAFKA_UDP;
		}
	}else if(!strcasecmp(key,CONFIG_THREADS_KEY)){
		global_config.udp_threads = assert_json_integer(key,value);
	}else if(!strcasecmp(key,CONFIG_DEBUG_KEY)){
		parse_debug(key,value);
	}else if(!strcasecmp(key,CONFIG_PORT_KEY)){
		global_config.listen_port = assert_json_integer(key,value);
	}else if(!strcasecmp(key,CONFIG_RESPONSE_KEY)){
		parse_response(key,value);
	}else if(!strcasecmp(key,CONFIG_KAFKA_KEY)){
		parse_kafka_config(key,value);
	}else{
		fatal("Unknown config key %s\n",key);
	}
}

static void parse_config0(json_t *root){
	const char *key;
	json_t *value;
	json_object_foreach(root, key, value)
		parse_config_keyval(key,value);
}

static void check_config(){
	if(global_config.listen_port == 0){
		fatal("You have to set a port to listen\n");
	}
	if(!only_stdout_output() && global_config.topic == NULL){
		fatal("You have to set a topic to write to\n");
	}
	if(!only_stdout_output() && global_config.brokers == NULL){
		fatal("You have to set a brokers to write to\n");
	}
	if(global_config.proto == 0){
		fatal(PROTO_ERROR "\n");
	}
	if(global_config.udp_threads == 0){
		global_config.udp_threads = 1;
	}
}

void parse_config(const char *config_file_path){
	json_error_t error;
	json_t *root = json_load_file(config_file_path,0,&error);
	if(root==NULL){
		rblog(LOG_ERR,"Error parsing config file, line %d: %s\n",error.line,error.text);
		exit(1);
	}

	if(!json_is_object(root)){
		rblog(LOG_ERR,"JSON config is not an object\n");
		exit(1);
	}

	parse_config0(root);
	json_decref(root);
	check_config();
}

void free_global_config(){
	free(global_config.topic);
	free(global_config.brokers);
	free(global_config.response);
}
