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

static void check_rb_decoder_simple(rd_kafka_message_t *msgs,size_t msgs_size) {
	json_error_t jerr;
	const char *client_mac,*application_name,*sensor_uuid,*b;
	json_int_t a;
	int d;

	assert(msgs_size == 1);
	json_t *root = json_loadb(msgs->payload, msgs->len, 0, &jerr);
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

	assert(0==strcmp(client_mac,"54:26:96:db:88:01"));
	assert(0==strcmp(application_name,"wwww"));
	assert(0==strcmp(sensor_uuid,"abc"));
	assert(a == 1); /* Enrichment! original message had 5 here */
	assert(0==strcmp(b,"c"));
	assert(d == 1);

	json_decref(root);
	free(msgs->payload);
}

static void test_rb_decoder_simple() {
	json_error_t jerr;
	rd_kafka_message_t rkm;
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

	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

	struct rb_opaque rb_opaque = {
#ifdef RB_OPAQUE_MAGIC
		.magic = RB_OPAQUE_MAGIC,
#endif

		.rb_config = &global_config.rb,
	};

	struct rb_session *my_session = NULL;
	const char msg[] = "{\"client_mac\": \"54:26:96:db:88:01\", "
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"abc\", \"a\":5}";

	process_rb_buffer(msg, sizeof(msg)-1, &args, &rb_opaque, &my_session);

	assert(1==rd_kafka_msg_q_size(&my_session->msg_queue));
	rd_kafka_msg_q_dump(&my_session->msg_queue,&rkm);
	assert(0==rd_kafka_msg_q_size(&my_session->msg_queue));

	check_rb_decoder_simple(&rkm,1);

	// Free session
	process_rb_buffer(NULL, 0, &args, &rb_opaque, &my_session);

	free_valid_rb_database(&rb_db);
	json_decref(config);
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

	free_global_config();

	close(temp_fd);
	
	return 0;
}
