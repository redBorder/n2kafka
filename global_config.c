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

#include "util.h"
#include "global_config.h"
#include "librd/rdfile.h"
#include "librd/rdsysqueue.h"

#ifdef HAVE_LIBMICROHTTPD
#include "http.h"
#endif
#include "socket.h"

#include <errno.h>
#include <librd/rdlog.h>
#include <string.h>
#include <jansson.h>
#include <arpa/inet.h>
#include <assert.h>

#ifndef LIST_FOREACH_SAFE
#define	LIST_FOREACH_SAFE(var, head, field, tvar)			\
	for ((var) = LIST_FIRST((head));				\
	    (var) && ((tvar) = LIST_NEXT((var), field), 1);		\
	    (var) = (tvar))
#endif

#define CONFIG_LISTENERS_ARRAY "listeners"
#define CONFIG_PROTO_KEY "proto"
#define CONFIG_THREADS_KEY "threads"
#define CONFIG_TOPIC_KEY "topic"
#define CONFIG_BROKERS_KEY "brokers"
#define CONFIG_PORT_KEY "port"
#define CONFIG_DEBUG_KEY "debug"
#define CONFIG_RESPONSE_KEY "response"
#define CONFIG_BLACKLIST_KEY "blacklist"
#define CONFIG_MSE_SENSORS_KEY "mse-sensors"
#define CONFIG_MERAKI_SECRETS_KEY "meraki-secrets"
#define CONFIG_RBHTTP2K_CONFIG "rb_http2k_config"
#define CONFIG_RDKAFKA_KEY "rdkafka."
#define CONFIG_TCP_KEEPALIVE "tcp_keepalive"

#define CONFIG_PROTO_TCP  "tcp"
#define CONFIG_PROTO_UDP  "udp"
#define CONFIG_PROTO_HTTP "http"

#define CONFIG_DECODE_AS_NULL           ""
#define CONFIG_DECODE_AS_MSE            "MSE"
#define CONFIG_DECODE_AS_MERAKI         "meraki"
#define CONFIG_DECODE_AS_RBHTTP2K       "rb_http2k"

struct n2kafka_config global_config;

static const struct registered_decoder{
	const char *decode_as;
	const char *config_parameters;
	listener_callback cb;
	listener_opaque_creator opaque_creator;
	listener_opaque_reload opaque_reload;
	listener_opaque_destructor opaque_destructor;
} registered_decoders[] = {
	{CONFIG_DECODE_AS_NULL,NULL,dumb_decoder,NULL,NULL,NULL},
	// @TODO destructors
	{CONFIG_DECODE_AS_MSE,CONFIG_MSE_SENSORS_KEY,mse_decode,mse_opaque_creator,mse_opaque_reload,
	                                                                            mse_opaque_done},
	{CONFIG_DECODE_AS_MERAKI,CONFIG_MERAKI_SECRETS_KEY,meraki_decode,meraki_opaque_creator,
	                                         meraki_opaque_reload,meraki_opaque_destructor},
	{CONFIG_DECODE_AS_RBHTTP2K,CONFIG_RBHTTP2K_CONFIG,rb_decode,rb_opaque_creator,
	                                         rb_opaque_reload,rb_opaque_done}
};

static const struct registered_listener{
	const char *proto;
	listener_creator creator;
} registered_listeners[] = {
#ifdef HAVE_LIBMICROHTTPD
	{CONFIG_PROTO_HTTP, create_http_listener},
#endif
	{CONFIG_PROTO_TCP, create_tcp_listener},
	{CONFIG_PROTO_UDP, create_udp_listener},
};

void init_global_config(){
	memset(&global_config,0,sizeof(global_config));
	global_config.kafka_conf = rd_kafka_conf_new();
	global_config.kafka_topic_conf = rd_kafka_topic_conf_new();
	global_config.blacklist = in_addr_list_new();
	rd_log_set_severity(LOG_INFO);
	LIST_INIT(&global_config.listeners);

	init_mse_database(&global_config.mse.database);
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

static const json_t *assert_json_array(const char *key,const json_t *value){
	if(!json_is_array(value)){
		fatal("%s value must be an array\n",key);
	}
	return value;
}

static void *assert_pton(int af,const char *src,void *dst){
	const int pton_rc = inet_pton(af,src,dst);
	if(pton_rc < 0){
		fatal("pton(%s) error: %s",src,strerror(errno));
	}
	return dst;
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
		rd_log_set_severity(LOG_DEBUG);
}

static void parse_rdkafka_keyval_config(const char *key,const char *value){
	rd_kafka_conf_res_t res;
	char errstr[512];

	const char *name = key + strlen(CONFIG_RDKAFKA_KEY);

	res = RD_KAFKA_CONF_UNKNOWN;
	/* Try "topic." prefixed properties on topic
	 * conf first, and then fall through to global if
	 * it didnt match a topic configuration property. */
	if (!strncmp(name, "topic.", strlen("topic.")))
		res = rd_kafka_topic_conf_set(global_config.kafka_topic_conf,
					      name+strlen("topic."),
					      value,errstr,sizeof(errstr));

	if (res == RD_KAFKA_CONF_UNKNOWN)
		res = rd_kafka_conf_set(global_config.kafka_conf, name, value,
					errstr, sizeof(errstr));

	if (res != RD_KAFKA_CONF_OK)
		fatal("%s", errstr);
}

static void parse_rdkafka_config_json(const char *key,const json_t *jvalue){
	// Extracted from Magnus Edenhill's kafkacat 
	const char *value = assert_json_string(key,jvalue);
	parse_rdkafka_keyval_config(key,value);
}

static void parse_blacklist(const char *key,const json_t *value){
	assert_json_array(key,value);

	const size_t arr_len = json_array_size(value);
	size_t i;
	for(i=0;i<arr_len;++i){
		struct in_addr addr;
		const json_t *json_i = json_array_get(value,i);
		const char *addr_string = assert_json_string("blacklist values",json_i);
		if(global_config.debug)
			rdbg("adding %s address to blacklist",addr_string);
		assert_pton(AF_INET,addr_string,&addr);
		in_addr_list_add(global_config.blacklist,&addr);
	}
}

static const listener_creator *protocol_creator(const char *proto){
	size_t i;
	const size_t listeners_length 
	    = sizeof(registered_listeners)/sizeof(registered_listeners[0]);

	for(i=0;i<listeners_length;++i) {
		if ( NULL==registered_listeners[i].proto ) {
			rdlog(LOG_CRIT,"Error: NULL proto in registered_listeners");
			continue;
		}

		if( 0 == strcmp(registered_listeners[i].proto, proto) ) {
			return &registered_listeners[i].creator;
		}
	}

	return NULL;
}

static const struct registered_decoder *locate_registered_decoder(const char *decode_as) {
	assert(decode_as);

	size_t i;
	const size_t decoders_length 
	    = sizeof(registered_decoders)/sizeof(registered_decoders[0]);

	for(i=0;i<decoders_length;++i) {
		assert( NULL!=registered_decoders[i].decode_as );

		if( 0 == strcmp(registered_decoders[i].decode_as, decode_as) ) {
			return &registered_decoders [i];
		}
	}

	return NULL;
}

static void parse_listener(json_t *config){
	char *proto = NULL,*decode_as="";
	json_error_t json_err;
	char err[BUFSIZ];

	const int unpack_rc = json_unpack_ex(config,&json_err,0,"{s:s,s?s}",
		"proto",&proto,"decode_as",&decode_as);

	if( unpack_rc != 0 ) {
		rdlog(LOG_ERR,"Can't parse listener: %s",json_err.text);
		return;
	}

	if(NULL == proto ) {
		rdlog(LOG_ERR,"Can't create a listener with no proto");
		return;
	}

	const listener_creator *_listener_creator = protocol_creator(proto);
	if ( NULL == _listener_creator ) {
		rdlog(LOG_ERR,"Can't find listener creator for protocol %s",proto);
		return;
	}

	assert(decode_as);
	const struct registered_decoder *decoder = locate_registered_decoder(decode_as);
	if(NULL == decoder){
		rdlog(LOG_ERR,"Can't locate decoder type %s",decode_as);
		exit(-1);
	}

	void *decoder_opaque = NULL;
	if(decoder->opaque_creator) {
		const int opaque_creator_rc = decoder->opaque_creator(config,&decoder_opaque,err,
			sizeof(err));
		if(opaque_creator_rc != 0) {
			rdlog(LOG_ERR,"Can't create opaque for listener %s: %s",proto,err);
			exit(-1);
		}
	}

	struct listener *listener = (*_listener_creator)(config,
		decoder->cb,decoder_opaque,err,sizeof(err));

	if( NULL == listener ) {
		rdlog(LOG_ERR,"Can't create listener for proto %s: %s.",proto,err);
		exit(-1);
	}

	listener->cb.cb_opaque_destructor = decoder->opaque_destructor;
	listener->cb.cb_opaque_reload = decoder->opaque_reload;

	LIST_INSERT_HEAD(&global_config.listeners,listener,entry);
}

static void parse_listeners_array(const char *key,const json_t *array){
	assert_json_array(key,array);

	size_t _index;
	json_t *value;

	json_array_foreach(array,_index,value) {
		parse_listener(value);
	}
}

/// @TODO use json_unpack_ex
static void parse_config_keyval(const char *key,const json_t *value){
	if(!strcasecmp(key,CONFIG_TOPIC_KEY)){
		// Already parsed
	}else if(!strcasecmp(key,CONFIG_BROKERS_KEY)){
		// Already parsed
	}else if(!strcasecmp(key,CONFIG_LISTENERS_ARRAY)){
		parse_listeners_array(key,value);
	}else if(!strcasecmp(key,CONFIG_DEBUG_KEY)){
		parse_debug(key,value);
	}else if(!strcasecmp(key,CONFIG_RESPONSE_KEY)){
		parse_response(key,value);
	}else if(!strncasecmp(key,CONFIG_RDKAFKA_KEY,strlen(CONFIG_RDKAFKA_KEY))){
		// Already parsed
	}else if(!strcasecmp(key,CONFIG_BLACKLIST_KEY)){
		parse_blacklist(key,value);
	/// @TODO replace next entries by a for in decoders
	}else if(!strcasecmp(key,CONFIG_MSE_SENSORS_KEY)){
		// Already parsed
	}else if(!strcasecmp(key,CONFIG_MERAKI_SECRETS_KEY)){
		// Already parsed
	}else if(!strcasecmp(key,CONFIG_RBHTTP2K_CONFIG)){
		// Already parsed
	}else{
		fatal("Unknown config key %s\n",key);
	}
}

static void parse_rdkafka_config_keyval(const char *key,const json_t *value) {
	if(!strcasecmp(key,CONFIG_TOPIC_KEY)){
		global_config.topic = strdup(assert_json_string(key,value));
	} else if(!strcasecmp(key,CONFIG_BROKERS_KEY)) {
		global_config.brokers = strdup(assert_json_string(key,value));
	} else if(!strncasecmp(key,CONFIG_RDKAFKA_KEY,strlen(CONFIG_RDKAFKA_KEY))) {
		// if starts with
		parse_rdkafka_config_json(key,value);
	}
}

static void parse_config0(json_t *root){
	json_error_t jerr;
	json_t *mse=NULL,*meraki=NULL,*rb_http2k=NULL;
	const char *key;
	json_t *value;
	char err[BUFSIZ];

	/// Need to parse kafka stuff before
	json_object_foreach(root, key, value) {
		parse_rdkafka_config_keyval(key,value);
	}

	if(!only_stdout_output()) {
		init_rdkafka();
	}

	/// @TODO replace next unpack by a for loop in decoders struct
	const int unpack_rc = json_unpack_ex(root,&jerr,0,"{s?o,s?o,s?o}",
		CONFIG_MSE_SENSORS_KEY,&mse,
		CONFIG_MERAKI_SECRETS_KEY,&meraki,
		CONFIG_RBHTTP2K_CONFIG,&rb_http2k);

	if(unpack_rc != 0) {
		rdlog(LOG_ERR,"Can't parse config file: %s",jerr.text);
		exit(-1);
	}

	if(mse) {
		const int parse_rc = parse_mse_array(&global_config.mse.database, mse,err,
			                                                                    sizeof(err));
		if(0 != parse_rc) {
			rdlog(LOG_ERR,"Can't parse MSE array: %s",err);
			exit(-1);
		}
	}
	if(meraki) {
		const int parse_rc = parse_meraki_secrets(&global_config.meraki.database, meraki,err,
			                                                                    sizeof(err));
		if(0 != parse_rc) {
			rdlog(LOG_ERR,"Can't parse meraki secrets: %s",err);
			exit(-1);
		}
	}
	if(rb_http2k) {
		const int parse_rc = parse_rb_config(&global_config.rb.database, rb_http2k,err,
			                                                                    sizeof(err));
		if(0 != parse_rc) {
			rdlog(LOG_ERR,"Can't parse rb_http2k config: %s",err);
			exit(-1);
		}
	}

	json_object_foreach(root, key, value)
		parse_config_keyval(key,value);
}

static void check_config(){
	if(!only_stdout_output() && global_config.topic == NULL){
		fatal("You have to set a topic to write to\n");
	}
	if(!only_stdout_output() && global_config.brokers == NULL){
		fatal("You have to set a brokers to write to\n");
	}
}

void parse_config(const char *config_file_path){
	json_error_t error;
	global_config.config_path = strdup(config_file_path);
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

static void shutdown_listener(struct listener *i) {
	if (NULL == i->join) {
		rblog(LOG_CRIT,"One listener does not have join() function.");
	} else {
		rblog(LOG_INFO,"Joining listener on port %d.",i->port);
		i->join(i->private);
		if(i->cb.cb_opaque_destructor)
			i->cb.cb_opaque_destructor(i->cb.cb_opaque);
	}

	free(i);
}

static void shutdown_listeners(struct n2kafka_config *config){
	struct listener *i = NULL,*aux=NULL;
	LIST_FOREACH_SAFE(i,&config->listeners,entry,aux)
		shutdown_listener(i);
}

/// Close listener that are not valid anymore, and reload present ones
static void reload_listeners_check_already_present(json_t *new_listeners,
                                                  struct n2kafka_config *config) {
	json_error_t jerr;
	size_t _index = 0;
	struct listener *i = NULL,*aux=NULL;
	LIST_FOREACH_SAFE(i,&config->listeners,entry,aux) {
		const uint16_t i_port = i->port;

		json_t *found_value = NULL;
		json_t *value = NULL;

		json_array_foreach(new_listeners, _index, value) {
			int port = 0;

			if(NULL != found_value)
				break;

			json_unpack_ex(value,&jerr,0,
				"{s:i}","port",&port);

			if(port > 0 && port == i_port)
				found_value = value;
		}

		if(found_value && i->reload) {
			rdlog(LOG_INFO,"Reloading listener on port %d",i_port);
			i->reload(found_value,i->cb.cb_opaque_reload,i->cb.cb_opaque,i->private);
		} else {
			LIST_REMOVE(i,entry);
			shutdown_listener(i);
		}
	}
}

/// Creating new declared listeners
static void reload_listeners_create_new_ones(json_t *new_listeners_array,
                                   struct n2kafka_config *config) {
	json_error_t jerr;
	size_t _index = 0;
	json_t *new_config_listener = 0;
	struct listener *i = NULL;
	json_array_foreach(new_listeners_array, _index, new_config_listener) {
		uint16_t searched_port = 0;

		struct listener *found_value = NULL;
		i = NULL;

		json_unpack_ex(new_config_listener,&jerr,0,
			"{s:i}","port",&searched_port);

		LIST_FOREACH(i,&config->listeners,entry) {
			uint16_t port = i->port;

			if(NULL != found_value)
				break;

			if(port > 0 && port == searched_port)
				found_value = i;
		}

		if(NULL == found_value) {
			// new listener, need to create
			parse_listener(new_config_listener);
		}
	}
}

static void reload_listeners(json_t *new_config,struct n2kafka_config *config){
	json_error_t jerr;
	
	json_t *listeners_array = NULL;
	const int json_unpack_rc = json_unpack_ex(new_config,&jerr,0,
		"{s:o}","listeners",&listeners_array);

	if(json_unpack_rc != 0) {
		rdlog(LOG_ERR,"Can't extract listeners array: %s",jerr.text);
		return;
	}

	reload_listeners_check_already_present(listeners_array,config);
	reload_listeners_create_new_ones(listeners_array,config);
}

typedef int (*reload_cb)(void *database,const struct json_t *config,char *err,size_t err_size);

static json_t *reload_decoder(struct n2kafka_config *config,const char *decoder_config_key,
	void *database,reload_cb reload_callback) {
	char err[BUFSIZ];
	json_error_t json_err;
	json_t *decoder_config = NULL;
	if(config->config_path==NULL){
		rblog(LOG_ERR,"Have no config file to reload");
	}

	json_t *root = json_load_file(config->config_path,0,&json_err);
	if(root==NULL){
		rblog(LOG_ERR,"Can't reload, Error parsing config file, line %d: %s\n",
			json_err.line,json_err.text);
		return NULL;
	}

	if(!json_is_object(root)){
		rblog(LOG_ERR,"Can't reload, JSON config is not an object\n");
		goto error_free_root;
	}

	const int unpack_rc = json_unpack_ex(root,&json_err,0,"{s?o}",
		decoder_config_key,&decoder_config);
	if(unpack_rc != 0) {
		rdlog(LOG_ERR,"Can't reload, can't parse config file line %d col %d: %s",
			json_err.line,json_err.column,json_err.text);
		goto error_free_root;
	}

	if(NULL != decoder_config) {
		reload_callback(database, decoder_config,err,sizeof(err));
	}

error_free_root:
	json_decref(root);

	return decoder_config;
}

static void reload_mse_config(struct n2kafka_config *config){
	/// @TODO merge with config parse
	
	rblog(LOG_INFO,"Reloading MSE sensors");
	reload_decoder(config,CONFIG_MSE_SENSORS_KEY,&config->mse.database,parse_mse_array);
}

static void reload_meraki_config(struct n2kafka_config *config) {
	rblog(LOG_INFO,"Reloading meraki sensors");
	reload_decoder(config,CONFIG_MERAKI_SECRETS_KEY,&config->meraki.database,parse_meraki_secrets);
}

static void reload_decoders(struct n2kafka_config *config) {
	reload_mse_config(config);
	reload_meraki_config(config);
}

void reload_config(struct n2kafka_config *config) {
	json_error_t jerr;

	assert(config);
	if(config->config_path==NULL){
		rblog(LOG_ERR,"Have no config file to reload");
		return;
	}

	json_t *new_config_file = json_load_file(config->config_path,0,&jerr);
	if(NULL == new_config_file) {
		rdlog(LOG_ERR,"Can't parse new config file: %s at line %d, column %d",
			jerr.text,jerr.line,jerr.column);
	}

	reload_listeners(new_config_file,config);
	reload_decoders(config);
	json_decref(new_config_file);
}

void free_global_config(){
	shutdown_listeners(&global_config);

	free_valid_mse_database(&global_config.mse.database);
	free_valid_rb_database(&global_config.rb.database);

	if(!only_stdout_output()){
		flush_kafka();
		stop_rdkafka();
	}

	in_addr_list_done(global_config.blacklist);
	free(global_config.topic);
	free(global_config.brokers);
	free(global_config.response);
}
