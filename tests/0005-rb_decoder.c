#include "rb_mse.c"

#include "rb_json_tests.c"
#include "rb_mse_tests.h"

#include "../src/decoder/rb_http2k_decoder.c"
#include "../src/listener/http.c"

#include <assert.h>

static const char TEMP_TEMPLATE[] = "n2ktXXXXXX";

static const char CONFIG_TEST[] =
	"{"
		"\"brokers\": \"localhost\","
        "\"rb_http2k_config\": {"
            "\"uuids\" : {"
                    "\"abc\" : {"
                    		"\"sensor_uuid\":\"abc\","
                            "\"a\":1,"
                            "\"b\":\"c\","
                            "\"d\":true,"
                            "\"e\":null"
                    "},"
                    "\"def\" : {"
                    		"\"sensor_uuid\":\"def\","
                            "\"f\":1,"
                            "\"g\":\"w\","
                            "\"h\":false,"
                            "\"i\":null"
                    "}"
            "},"
            "\"topics\" : {"
                    "\"rb_flow\": {"
                            "\"partition_key\":\"client_mac\","
                            "\"partition_algo\":\"mac\""
                    "},"
                    "\"rb_event\": {"
                    "}"
            "}"
        "}"
	"}";

static const char *VALID_URL = "/rbdata/abc/rb_flow";

static void test_validate_uri() {
	json_error_t jerr;
	json_t *config = json_loads(CONFIG_TEST, 0, &jerr);
	if(NULL == config) {
		rdlog(LOG_CRIT,"Couldn't unpack JSON config: %s",jerr.text);
		assert(0);
	}

	const json_t *decoder_config = NULL;
	const int unpack_rc = json_unpack_ex(config, &jerr, 0, "{s:o}", 
		"rb_http2k_config",&decoder_config);
	if(0 != unpack_rc) {
		rdlog(LOG_CRIT,"Can't unpack config: %s",jerr.text);
		assert(0);
	}

	struct rb_database rb_db;
	init_rb_database(&rb_db);
	parse_rb_config(&rb_db,decoder_config);

	int allok = 1;
	char *topic=NULL,*uuid=NULL;
	int validation_rc = rb_http2k_validation(NULL /* @TODO this should change */,VALID_URL,
							&rb_db, &allok,&topic,&uuid,"test_ip");

	assert(MHD_YES == validation_rc);
	assert(0==strcmp(topic,"rb_flow"));
	assert(0==strcmp(uuid,"abc"));

	free(topic);
	free(uuid);

	free_valid_rb_database(&rb_db);
	json_decref(config);
}

static void prepare_args(
        const char *topic,const char *sensor_uuid,const char *client_ip,
        struct pair *mem,size_t memsiz,keyval_list_t *list) {
	assert(3==memsiz);
	memset(mem,0,sizeof(*mem)*3);

	mem[0].key   = "topic";
	mem[0].value = topic;
	mem[1].key   = "sensor_uuid";
	mem[1].value = sensor_uuid;
	mem[2].key   = "client_ip";
	mem[2].value = client_ip;

	add_key_value_pair(list,&mem[0]);
	add_key_value_pair(list,&mem[1]);
	add_key_value_pair(list,&mem[2]);
}

/** This function just checks that session is NULL */
static void check_null_session(struct rb_session **sess,
                    void *unused __attribute__((unused))) {

	assert(NULL != sess);
	assert(NULL == *sess);
}

static void check_rb_decoder_double0(struct rb_session **sess,
                void *unused __attribute__((unused)),size_t expected_size) {
	int i=0;
	rd_kafka_message_t rkm[2];
	json_error_t jerr;
	const char *client_mac,*application_name,*sensor_uuid,*b;
	json_int_t a;
	int d;

	assert(expected_size==rd_kafka_msg_q_size(&(*sess)->msg_queue));
	rd_kafka_msg_q_dump(&(*sess)->msg_queue,rkm);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));

	for(i=0;i<expected_size;++i) {
		json_t *root = json_loadb(rkm[i].payload, rkm[i].len, 0, &jerr);
		if(NULL == root) {
			rdlog(LOG_ERR,"Couldn load file: %s",jerr.text);
			assert(0);
		}

		const int rc = json_unpack_ex(root, &jerr, 0,
			"{s:s,s:s,s:s,s:I,s:s,s:n,s:b}",
			"client_mac",&client_mac,"application_name",&application_name,
			"sensor_uuid",&sensor_uuid,"a",&a,"b",&b,"e","d",&d);

		if(rc != 0) {
			rdlog(LOG_ERR,"Couldn't unpack values: %s",jerr.text);
			assert(0);
		}

		if(i==0) {
			assert(0==strcmp(client_mac,"54:26:96:db:88:01"));
		} else {
			assert(0==strcmp(client_mac,"54:26:96:db:88:02"));
		}
		assert(0==strcmp(application_name,"wwww"));
		assert(0==strcmp(sensor_uuid,"abc"));
		assert(a == 1); /* Enrichment! original message had 5 here */
		assert(0==strcmp(b,"c"));
		assert(d == 1);

		json_decref(root);
		free(rkm[i].payload);
	}
}

static void check_rb_decoder_simple(struct rb_session **sess,void *opaque) {
	check_rb_decoder_double0(sess,opaque,1);
}

static void check_rb_decoder_double(struct rb_session **sess,void *opaque) {
	check_rb_decoder_double0(sess,opaque,2);
}

struct message_in {
	const char *msg;
	size_t size;
};

typedef void (*check_callback_fn)(struct rb_session **,void *opaque);

/** Template for rb_decoder test
	@param args Arguments like client_ip, topic, etc
	@param msgs Input messages
	@param msgs_len Length of msgs
	@param check_callback Array of functions that will be called with each
	session status. It is suppose to be the same length as msgs array.
	@param check_callback_opaque Opaque used in the second parameter of
	check_callback[iteration] call
	*/
static void test_rb_decoder0(keyval_list_t *args, struct message_in *msgs,
            check_callback_fn *check_callback, size_t msgs_len,
            void *check_callback_opaque) {
	json_error_t jerr;
	size_t i;
	json_t *config = json_loads(CONFIG_TEST, 0, &jerr);
	if(NULL == config) {
		rdlog(LOG_CRIT,"Couldn't unpack JSON config: %s",jerr.text);
		assert(0);
	}

	const json_t *decoder_config = NULL;
	const int unpack_rc = json_unpack_ex(config, &jerr, 0, "{s:o}",
		"rb_http2k_config",&decoder_config);
	if(0 != unpack_rc) {
		rdlog(LOG_CRIT,"Can't unpack config: %s",jerr.text);
		assert(0);
	}

	struct rb_database rb_db;
	init_rb_database(&rb_db);
	parse_rb_config(&rb_db,decoder_config);

	struct rb_opaque rb_opaque = {
#ifdef RB_OPAQUE_MAGIC
		.magic = RB_OPAQUE_MAGIC,
#endif
		.rb_config = &global_config.rb,
	};

	struct rb_session *my_session = NULL;

	for(i=0;i<msgs_len;++i) {
		process_rb_buffer(msgs[i].msg, msgs[i].msg ? msgs[i].size : 0, args,
			&rb_opaque, &my_session);
		check_callback[i](&my_session,check_callback_opaque);
	}

	free_valid_rb_database(&rb_db);
	json_decref(config);
}

static void test_rb_decoder_simple() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"abc\", \"a\":5}",  \
		check_rb_decoder_simple)                                              \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(&args, msgs, callbacks_functions, RD_ARRAYSIZE(msgs),
		NULL);

#undef MESSAGES
}

/** Two messages in the same input string */
static void test_rb_decoder_double() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"abc\", \"a\":5}"   \
	  "{\"client_mac\": \"54:26:96:db:88:02\", "                              \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"abc\", \"a\":5}",  \
		check_rb_decoder_double)                                              \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(&args, msgs, callbacks_functions, RD_ARRAYSIZE(msgs),
		NULL);
}

int main() {
	/// @TODO Need to have rdkafka inited. Maybe this plugin should have it owns rdkafka handler.
	init_global_config();
	char temp_filename[sizeof(TEMP_TEMPLATE)];
	strcpy(temp_filename,TEMP_TEMPLATE);
	int temp_fd = mkstemp(temp_filename);
	assert(temp_fd >= 0);
	write(temp_fd, CONFIG_TEST, strlen(CONFIG_TEST));

	parse_config(temp_filename);
	unlink(temp_filename);
	test_validate_uri();
	test_rb_decoder_simple();
	test_rb_decoder_double();

	free_global_config();

	close(temp_fd);
	
	return 0;
}
