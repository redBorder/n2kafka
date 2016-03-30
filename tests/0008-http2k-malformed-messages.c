#include "rb_json_tests.c"
#include "rb_http2k_tests.c"

#include "../src/listener/http.c"

#include <setjmp.h>
#include <cmocka.h>
#include <assert.h>

static const char TEMP_TEMPLATE[] = "n2ktXXXXXX";

static const char CONFIG_TEST[] =
    "{"
        "\"brokers\": \"localhost\","
        "\"rb_http2k_config\": {"
            "\"sensors_uuids\" : {"
                    "\"abc\" : {"
                        "\"enrichment\":{"
                            "\"sensor_uuid\":\"abc\","
                            "\"a\":1,"
                            "\"b\":\"c\","
                            "\"d\":true,"
                            "\"e\":null"
                        "}"
                    "},"
                    "\"def\" : {"
                        "\"enrichment\":{"
                            "\"sensor_uuid\":\"def\","
                            "\"f\":1,"
                            "\"g\":\"w\","
                            "\"h\":false,"
                            "\"i\":null,"
                            "\"j\":2.5"
                        "}"
                    "},"
                    "\"ghi\" : {"
                        "\"enrichment\":{"
                            "\"o\": {"
                                "\"a\":90"
                            "}"
                        "}"
                    "},"
                    "\"jkl\" : {"
                        "\"enrichment\":{"
                            "\"v\":[1,2,3,4,5]"
                        "}"
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

static void check_rb_decoder_two_keys(struct rb_session **sess,
                void *unused __attribute__((unused))) {
	rd_kafka_message_t rkm;
	json_error_t jerr;
	const char *client_mac;
	static const char expected_mac[] = "54:26:96:db:88:02";

	assert(1==rd_kafka_msg_q_size(&(*sess)->msg_queue));
	rd_kafka_msg_q_dump(&(*sess)->msg_queue,&rkm);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));

	json_t *root = json_loadb(rkm.payload, rkm.len, 0, &jerr);
	if(NULL == root) {
		rdlog(LOG_ERR,"Couldn load file: %s",jerr.text);
		assert(0);
	}

	const int rc = json_unpack_ex(root, &jerr, 0,
		"{s:s}", "client_mac",&client_mac);

	if(rc != 0) {
		rdlog(LOG_ERR,"Couldn't unpack values: %s",jerr.text);
		assert(0);
	}

	assert(0==strcmp(client_mac,expected_mac));
	assert(0==strncmp(rkm.key,expected_mac,strlen(expected_mac)));

	json_decref(root);
	free(rkm.payload);
}

static void check_rb_decoder_premature_closing(struct rb_session **sess,
                void *opaque) {
	check_rb_decoder_two_keys(sess,opaque);
}

/// Simple decoding with another enrichment
static void test_rb_decoder_two_keys() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","def","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                               \
	X("{\"client_mac\": \"54:26:96:db:88:02\", "                           \
		"\"client_mac\": \"54:26:96:db:88:03\","                       \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true, \"n\":null, \"o\":{}}"                   \
		"{\"client_mac\": \"54:26:96:db:88:02\", "                     \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true}",                                        \
		check_rb_decoder_two_keys)                                     \
	/* Free & Check that session has been freed */                         \
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

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

/// Trying to decode a JSON closing when you still have not open any json
static void test_rb_decoder_closing() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","def","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                            \
	X("}{\"client_mac\": \"54:26:96:db:88:02\", "                       \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", " \
		"\"a\":5, \"u\":true}",                                     \
		check_zero_messages)                                        \
	X("}}{\"client_mac\": \"54:26:96:db:88:02\", "                      \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", " \
		"\"a\":5, \"u\":true}",                                     \
		check_zero_messages)                                        \
	X("}}}{\"client_mac\": \"54:26:96:db:88:02\", "                     \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", " \
		"\"a\":5, \"u\":true}",                                     \
		check_zero_messages)                                        \
	/* Free & Check that session has been freed */                      \
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

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

static void check_invalid_key_type(struct rb_session **sess,
                void *opaque) {
	check_rb_decoder_two_keys(sess,opaque);
}

/** Test that the system is able to skip non-string keys is we are partitioning
    via client-mac */
static void test_invalid_key_type() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","def","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                               \
	X("{\"client_mac\": null, "                                            \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true, \"n\":null, \"o\":{}}"                   \
		"{\"client_mac\": \"54:26:96:db:88:02\", "                     \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true}",                                        \
		check_invalid_key_type)                                        \
	X("{\"client_mac\": true, "                                            \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true, \"n\":null, \"o\":{}}"                   \
		"{\"client_mac\": \"54:26:96:db:88:02\", "                     \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true}",                                        \
		check_invalid_key_type)                                        \
	X("{\"client_mac\": false, "                                           \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true, \"n\":null, \"o\":{}}"                   \
		"{\"client_mac\": \"54:26:96:db:88:02\", "                     \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true}",                                        \
		check_invalid_key_type)                                        \
	X("{\"client_mac\": 3, "                                               \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true, \"n\":null, \"o\":{}}"                   \
		"{\"client_mac\": \"54:26:96:db:88:02\", "                     \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true}",                                        \
		check_invalid_key_type)                                        \
	X("{\"client_mac\": 3.5, "                                             \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true, \"n\":null, \"o\":{}}"                   \
		"{\"client_mac\": \"54:26:96:db:88:02\", "                     \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true}",                                        \
		check_invalid_key_type)                                        \
	X("{\"client_mac\": {\"Im\":\"an object\"}, "                          \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true, \"n\":null, \"o\":{}}"                   \
		"{\"client_mac\": \"54:26:96:db:88:02\", "                     \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true}",                                        \
		check_invalid_key_type)                                        \
	X("{\"client_mac\": [\"I'm an array\"], "                              \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true, \"n\":null, \"o\":{}}"                   \
		"{\"client_mac\": \"54:26:96:db:88:02\", "                     \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "    \
		"\"a\":5, \"u\":true}",                                        \
		check_invalid_key_type)                                        \
	/* Free & Check that session has been freed */                         \
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

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_rb_decoder_two_keys),
		cmocka_unit_test(test_rb_decoder_closing),
		cmocka_unit_test(test_invalid_key_type),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
