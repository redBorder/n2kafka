#include "rb_json_tests.c"
#include "rb_http2k_tests.c"

#include "../src/listener/http.c"

#include <setjmp.h>
#include <cmocka.h>
#include <assert.h>

static const char CONFIG_TEST[] =
    "{"
        "\"brokers\": \"localhost\","
        "\"rb_http2k_config\": {"
            "\"sensors_uuids\" : {"
                "\"abc\" : {"
                    "\"enrichment\": {"
                        "\"sensor_uuid\":\"abc\","
                        "\"a\":1,"
                        "\"b\":\"c\","
                        "\"d\":true,"
                        "\"e\":null"
                    "},"
                    "\"organization_uuid\":\"abc_org\""
                "},"
                "\"def\" : {"
                    "\"enrichment\": {"
                        "\"sensor_uuid\":\"def\","
                        "\"a\":2,"
                        "\"g\":\"w\","
                        "\"h\":false,"
                        "\"i\":null,"
                        "\"j\":2.5"
                    "},"
                    "\"organization_uuid\":\"def_org\""
                "}"
            "},"
            "\"organizations_uuids\":{"
                "\"abc_org\" : {"
                  "\"enrichment\":{"
                    "\"a\":3,"
                    "\"k\":4"
                  "}"
                "},"
                "\"def_org\" : {"
                  "\"enrichment\":{"
                    "\"a\":5,"
                    "\"k\":10"
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

static void check_organization_enrichment_abc(struct rb_session **sess,
                void *unused __attribute__((unused))) {
	rd_kafka_message_t rkm;
	json_error_t jerr;
	const char *client_mac,*application_name,*sensor_uuid,*b;
	json_int_t a,k;
	int d;

	assert(1==rd_kafka_msg_q_size(&(*sess)->msg_queue));
	rd_kafka_msg_q_dump(&(*sess)->msg_queue,&rkm);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));

	json_t *root = json_loadb(rkm.payload, rkm.len, 0, &jerr);
	if(NULL == root) {
		rdlog(LOG_ERR,"Couldn load file: %s",jerr.text);
		assert(0);
	}

	const int rc = json_unpack_ex(root, &jerr, JSON_STRICT,
		"{s:s,s:s,s:s,s:I,s:s,s:n,s:b,s:I}",
		"client_mac",&client_mac,"application_name",&application_name,
		"sensor_uuid",&sensor_uuid,"a",&a,"b",&b,"e","d",&d,"k",&k);

	if (rc != 0) {
		rdlog(LOG_ERR,"Couldn't unpack values: %s",jerr.text);
		assert(0);
	}

	assert(0==strcmp(client_mac,"54:26:96:db:88:01"));
	assert(0==strcmp(application_name,"wwww"));
	assert(0==strcmp(sensor_uuid,"abc"));
	assert(a == 1); /* Enrichment! original message had 5 here,
                           and organization has 3 */
	assert(0==strcmp(b,"c"));
	assert(d == 1);
        assert(k == 4); /* organization enrichment */

	json_decref(root);
	free(rkm.payload);
}

static void test_organization_enrichment() {
        struct pair mem[3];
        keyval_list_t args;
        keyval_list_init(&args);
        prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
        X("{\"client_mac\":\"54:26:96:db:88:01\","                            \
                "\"application_name\":\"wwww\",\"sensor_uuid\":\"abc\","      \
                "\"a\":5}",                                                   \
                check_organization_enrichment_abc)                            \
        /* Free & Check that session has been freed */                        \
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
		cmocka_unit_test(test_organization_enrichment),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
