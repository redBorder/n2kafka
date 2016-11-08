#include "rb_mse_tests.h"
#include "engine/global_config.h"

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

static const time_t NOW = 1446650950;

static const char MSE10_ASSOC[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	            "\"notificationType\":\"association\","
	            "\"subscriptionName\":\"rb-assoc\","
	            "\"entity\":\"WIRELESS_CLIENTS\","
	            "\"deviceId\":\"00:ca:00:05:06:52\","
	            "\"lastSeen\":\"2015-02-24T08:41:50.026+0000\","
	            "\"ssid\":\"SHAW_SABER\","
	            "\"band\":\"IEEE_802_11_B\","
	            "\"apMacAddress\":\"00:ba:20:10:4f:00\","
	            "\"association\":true,"
	            "\"ipAddress\":["
	                "\"25.145.34.131\""
	            "],"
	            "\"status\":3,"
	            "\"username\":\"\","
	            "\"timestamp\":1446650950000"
	        "}"
	    "]"
	"}";
		// *INDENT-ON*

static const char MSE_ARRAY_IN[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": \"rb-assoc\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_TOPIC[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": \"rb-assoc\" \n" \
			", \"topic\": \"topic_test\"\n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"testing\"\n" \
				", \"sensor_id\": 255\n" \

			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static void checkMSE10Decoder_valid_enrich(struct mse_array
        *notifications_array) {
	/* No database -> output == input */
	assert_int_equal(notifications_array->size, 1);
	assert_int_equal(
	    notifications_array->data[0].string_size,
	    strlen(notifications_array->data[0].string)
	);

	const char *subscriptionName = NULL, *sensor_name = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert_non_null(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:[{s:s,s:s,s:i}]}",
	                                     "notifications",
	                                     "subscriptionName", &subscriptionName,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id);

	assert_int_equal(unpack_rc, 0);
	assert_string_equal(subscriptionName, "rb-assoc");
	assert_string_equal(sensor_name, "testing");
	assert_int_equal(sensor_id, 255);
	json_decref(ret);
}

static const char LISTENER_NULL_CONFIG[] = "{}";
static const char LISTENER_OPAQUE[] = \
            "{\"enrichment\":{\"a\":\"b\"}}";
static const char LISTENER_OPAQUE_EMPTY[] = "";

static const char LISTENER_TOPIC[] = \
            "{\"topic\":\"topic_test\",\"enrichment\":{\"a\":\"b\"}}";

static void test1(const char *_listener_config){
	struct mse_config mse_config;
	void *o_mse;
	int ret_creator = 0;
	int ret_reload = 0;
	char err[512];
	const char *value_a = NULL;
	json_error_t jerr;
	init_global_config();


	if (NULL == global_config.rk) {
		if (!(global_config.rk = rd_kafka_new(RD_KAFKA_CONSUMER, global_config.kafka_conf,
								err, sizeof(err)))) {
			fprintf(stderr,
					"%% Failed to create new consumer: %s\n",
					err);
			exit(1);
			}
	}
	global_config.topic = strdup("test");

	memset(&mse_config, 0, sizeof(mse_config));
	json_t *listener_config = json_loads(_listener_config, 0, &jerr);

	ret_creator = mse_opaque_creator(listener_config, &o_mse);
	assert_int_equal(0, ret_creator);


	assert(listener_config);
	const int unpack_rc = json_unpack_ex(listener_config, &jerr, 0,
	                                     "{s:{s:s}}",
	                                     "enrichment",
	                                     "a", &value_a);

	assert(unpack_rc == 0);
	assert(0 == strcmp(value_a, "b"));

	ret_reload = mse_opaque_reload(listener_config, o_mse);
	assert_int_equal(0, ret_reload);

	assert(listener_config);
	const int unpack_rc2 = json_unpack_ex(listener_config, &jerr, 0,
	                           "{s:{s:s}}",
	                           "enrichment",
	                           "a", &value_a);

	assert(unpack_rc2 == 0);
	assert(0 == strcmp(value_a, "b"));

	json_decref(listener_config);

	mse_opaque_done(o_mse);

	free_valid_mse_database(&global_config.mse.database);
	rd_kafka_topic_conf_destroy(global_config.kafka_topic_conf);
	in_addr_list_done(global_config.blacklist);

	rd_kafka_destroy(global_config.rk);
	free(global_config.topic);
}


static void test2(const char *_listener_config){
	struct mse_config mse_config;
	void *o_mse;
	int ret_creator = 0;
	int ret_reload = 0;
	char err[512];
	const char *value_a = NULL;
	json_error_t jerr;
	init_global_config();


	if (NULL == global_config.rk) {
		if (!(global_config.rk = rd_kafka_new(RD_KAFKA_CONSUMER, global_config.kafka_conf,
								err, sizeof(err)))) {
			fprintf(stderr,
					"%% Failed to create new consumer: %s\n",
					err);
			exit(1);
			}
	}
	global_config.topic = strdup("test");

	memset(&mse_config, 0, sizeof(mse_config));
	json_t *listener_config = json_loads(_listener_config, 0, &jerr);

	ret_creator = mse_opaque_creator(listener_config, &o_mse);
	assert_int_equal(-1, ret_creator);
	assert_null(listener_config);

	assert_null(o_mse);
//	mse_opaque_done(o_mse);
	free_valid_mse_database(&global_config.mse.database);
	rd_kafka_topic_conf_destroy(global_config.kafka_topic_conf);
	in_addr_list_done(global_config.blacklist);

	rd_kafka_destroy(global_config.rk);
	free(global_config.topic);
}

static void test3(const char *_listener_config){
	struct mse_config mse_config;
	void *o_mse;
	int ret_creator = 0;
	int ret_reload = 0;
	char err[512];
	const char *value_a = NULL;
	json_error_t jerr;
	init_global_config();


	if (NULL == global_config.rk) {
		if (!(global_config.rk = rd_kafka_new(RD_KAFKA_CONSUMER, global_config.kafka_conf,
								err, sizeof(err)))) {
			fprintf(stderr,
					"%% Failed to create new consumer: %s\n",
					err);
			exit(1);
			}
	}

	/* NO topic config */
	//global_config.topic = strdup("test");

	memset(&mse_config, 0, sizeof(mse_config));
	json_t *listener_config = json_loads(_listener_config, 0, &jerr);

	ret_creator = mse_opaque_creator(listener_config, &o_mse);
	/* ret_creator -1  And  opaque->rkt  = NULL */
	assert_int_equal(-1, ret_creator);

	assert(listener_config);
	const int unpack_rc = json_unpack_ex(listener_config, &jerr, 0,
	                                     "{s:{s:s}}",
	                                     "enrichment",
	                                     "a", &value_a);

	assert(unpack_rc == 0);
	assert(0 == strcmp(value_a, "b"));

	ret_reload = mse_opaque_reload(listener_config, o_mse);
	assert_int_equal(-1, ret_reload);

	assert(listener_config);
	const int unpack_rc2 = json_unpack_ex(listener_config, &jerr, 0,
	                           "{s:{s:s}}",
	                           "enrichment",
	                           "a", &value_a);

	assert_int_equal(unpack_rc2, -1);

	json_decref(listener_config);

	assert_null(o_mse);
	//mse_opaque_done(o_mse);

	free_valid_mse_database(&global_config.mse.database);
	rd_kafka_topic_conf_destroy(global_config.kafka_topic_conf);
	in_addr_list_done(global_config.blacklist);

	rd_kafka_destroy(global_config.rk);
}
static void test4_reload(const char *_listener_config){
	struct mse_config mse_config;
	void *o_mse;
	int ret_creator = 0;
	int ret_reload = 0;
	char err[512];
	const char *value_a = NULL;
	json_error_t jerr;
	init_global_config();


	if (NULL == global_config.rk) {
		if (!(global_config.rk = rd_kafka_new(RD_KAFKA_CONSUMER, global_config.kafka_conf,
								err, sizeof(err)))) {
			fprintf(stderr,
					"%% Failed to create new consumer: %s\n",
					err);
			exit(1);
			}
	}

	/* NO topic config */
	//global_config.topic = strdup("test");

	memset(&mse_config, 0, sizeof(mse_config));
	json_t *listener_config = json_loads(_listener_config, 0, &jerr);

	ret_creator = mse_opaque_creator(listener_config, &o_mse);
	/* ret_creator -1  And  opaque->rkt  = NULL */
	assert_int_equal(-1, ret_creator);

	assert(listener_config);
	const int unpack_rc = json_unpack_ex(listener_config, &jerr, 0,
	                                     "{s:{s:s}}",
	                                     "enrichment",
	                                     "a", &value_a);

	assert(unpack_rc == 0);
	assert(0 == strcmp(value_a, "b"));

	ret_reload = mse_opaque_reload(NULL, o_mse);
	assert_int_equal(-1, ret_reload);

	assert(listener_config);
	const int unpack_rc2 = json_unpack_ex(listener_config, &jerr, 0,
	                           "{s:{s:s}}",
	                           "enrichment",
	                           "a", &value_a);

	assert_int_equal(unpack_rc2, -1);

	meraki_opaque_reload(listener_config, &o_mse);

	json_decref(listener_config);

	assert_null(o_mse);
	//mse_opaque_done(o_mse);
	free_valid_mse_database(&global_config.mse.database);
	rd_kafka_topic_conf_destroy(global_config.kafka_topic_conf);
	in_addr_list_done(global_config.blacklist);

	rd_kafka_destroy(global_config.rk);
}

//static void test0(){
//	init_global_config();
//	free_global_config();
//}
//
//static void check_global_config(){
//	test0();
//}
static void check_opaque_empty(){
	test2(LISTENER_OPAQUE_EMPTY);
}

static void check_opaque_no_topic(){
	test3(LISTENER_NULL_CONFIG);
}

static void check_opaque_reload(){
	test4_reload(LISTENER_NULL_CONFIG);
}

static void check_opaque(){
	test1(LISTENER_OPAQUE);
}

static void check_topic(){
	test1(LISTENER_TOPIC);
}

static void testMSE10Decoder_valid_enrich() {
	testMSE10Decoder(MSE_ARRAY_IN,
	                 LISTENER_NULL_CONFIG,
	                 MSE10_ASSOC,
	                 NOW,
	                 checkMSE10Decoder_valid_enrich);
}

static void testMSE10Decoder_listener_topic() {
	testMSE10Decoder(MSE_ARRAY_TOPIC,
	                 LISTENER_TOPIC,
	                 MSE10_ASSOC,
	                 NOW,
	                 checkMSE10Decoder_valid_enrich);
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(testMSE10Decoder_valid_enrich),
		// cmocka_unit_test(check_global_config), /* TODO: WIP */
		cmocka_unit_test(check_opaque),
		cmocka_unit_test(check_opaque_empty),
		cmocka_unit_test(check_opaque_no_topic),
		cmocka_unit_test(check_opaque_reload),
		cmocka_unit_test(testMSE10Decoder_listener_topic),
		cmocka_unit_test(check_topic)
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
