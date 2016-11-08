#include "engine/global_config.h"
#include "rb_mse_tests.h"
#include "rb_mem_tests.h"

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

static const char MSE8_DEVICE_ID_AS_INT[] =
    // *INDENT-OFF*
	"{\"StreamingNotification\":{"
		"\"subscriptionName\":\"MSE_SanCarlos\","
		"\"entity\":\"WIRELESS_CLIENTS\","
//		"\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
		"\"deviceId\":1111111111,"
		"\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
		"\"floorRefId\":0,"
		"\"timestampMillis\":1426496270000,"
		"\"location\":{"
			"\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
			"\"currentlyTracked\":true,"
			"\"confidenceFactor\":88.0,"
			"\"userName\":\"\","
			"\"ssId\":\"\","
			"\"band\":\"UNKNOWN\","
			"\"apMacAddress\":\"\","
			"\"dot11Status\":\"PROBING\","
			"\"guestUser\":false,"
			"\"MapInfo\":{\"mapHierarchyString\":\"SantaClara Convention>Hyatt>HSC-FL8\","
			"\"floorRefId\":4698041283715793160,"
			"\"Dimension\":{\"length\":136.0,"
			"\"width\":408.0,"
			"\"height\":10.0,"
			"\"offsetX\":0.0,"
			"\"offsetY\":0.0,"
			"\"unit\":\"FEET\"},"
			"\"Image\":{\"imageName\":\"domain_0_1365721057045.png\"}"
		"},"
		"\"MapCoordinate\":{"
			"\"x\":116.18053,"
			"\"y\":85.699814,"
			"\"unit\":\"FEET\""
		"},"
		"\"Statistics\":{"
			"\"currentServerTime\":\"2015-03-16T01:57:50.000-0700\","
			"\"firstLocatedTime\":\"2015-03-16T01:57:49.991-0700\","
			"\"lastLocatedTime\":\"2015-03-16T01:57:49.991-0700\"}},"
			"\"timestamp\":\"2015-03-16T01:57:50.000-0700\""
		"}"
	"}";

static const char MSE10_2[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	        "}"
	    "]"
	"}";
static const char LISTENER_CONFIG[] = \
            "{\"enrichment\":{\"a\":\"b\"}}";


static const char LISTENER_NULL_CONFIG[] = "{}";
static const char MSE_NOT_ARRAY_IN[] = "{}";


static const char MSE_ARRAY_IN[] = "[]";
	// *INDENT-ON*

static const char MSE_EMPTY[] = "";

static void mem_test(void (*cb)(const char *_listener_config),
	                 const char *_listener_config) {
	size_t i = 1;
	do {
		mem_wrap_fail_in = i++;
		cb(_listener_config);
	} while (0 == mem_wrap_fail_in);
	mem_wrap_fail_in = 0;
}

static void testMSE10Decoder14_null_mse(const char *mse_array_str,
                             const char *_listener_config) {
	json_error_t jerr;
//	size_t i;
	const char *topic_name;
	struct mse_config mse_config;
	struct mse_decoder_info decoder_info;

	memset(&mse_config, 0, sizeof(mse_config));
	mse_decoder_info_create(&decoder_info);

	json_t *listener_config = json_loads(_listener_config, 0, &jerr);
	const int opaque_creator_rc = parse_decoder_info(&decoder_info,
													 listener_config,&topic_name);

	assert_true(0 == opaque_creator_rc);
	json_decref(listener_config);

	/* Currently, uses global_config */
	decoder_info.mse_config = &mse_config;

	// mse_array == NULL
	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	assert_true(mse_array);
	const int parse_rc = parse_mse_array(&decoder_info.mse_config->database,
										 mse_array);
	assert_true(parse_rc != 0);

	mse_decoder_info_destroy(&decoder_info);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

static void testMSE10Decoder14(const char *mse_array_str) {
	json_error_t jerr;
	//size_t i;
	const char *topic_name;
	struct mse_config mse_config;
	struct mse_decoder_info decoder_info;

	memset(&mse_config, 0, sizeof(mse_config));
	mse_decoder_info_create(&decoder_info);

	json_t *listener_config = json_loads(LISTENER_CONFIG, 0, &jerr);
	const int opaque_creator_rc = parse_decoder_info(&decoder_info,
													 listener_config,&topic_name);

	assert_true(0 == opaque_creator_rc);
	json_decref(listener_config);

	/* Currently, uses global_config */
	decoder_info.mse_config = &mse_config;

	// mse_array == NULL
	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	assert_true(mse_array);
	const int parse_rc = parse_mse_array(&decoder_info.mse_config->database,
										 mse_array);
	assert_true(parse_rc == 0);

	mse_decoder_info_destroy(&decoder_info);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

static void testMSE10Decoder14_process_mse_buffer(const char *mse_array_str,
                             const char *mse_input, const time_t now) {
	json_error_t jerr;
	size_t i;
	const char *topic_name;
	struct mse_config mse_config;
	struct mse_decoder_info decoder_info;

	memset(&mse_config, 0, sizeof(mse_config));
	mse_decoder_info_create(&decoder_info);

	json_t *listener_config = json_loads(LISTENER_NULL_CONFIG, 0, &jerr);
	const int opaque_creator_rc = parse_decoder_info(&decoder_info,
													 listener_config,&topic_name);

	assert_true(0 == opaque_creator_rc);
	json_decref(listener_config);

	/* Currently, uses global_config */
	decoder_info.mse_config = &mse_config;

	// mse_array == NULL
	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	assert_true(mse_array);
	const int parse_rc = parse_mse_array(&decoder_info.mse_config->database,
										 mse_array);
	assert_true(parse_rc == 0);

	struct mse_array *notifications_array = process_mse_buffer(
	        mse_input,
	        strlen(mse_input),
	        "127.0.0.1",
	        &decoder_info,
	        now);

	for (i = 0; notifications_array && i < notifications_array->size; ++i)
		free(notifications_array->data[i].string);

	free(notifications_array);

	mse_decoder_info_destroy(&decoder_info);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

static void testMSE10Decoder14_process_mse_buffer_2(const char *mse_array_str,
                             const char *mse_input, const time_t now) {
	json_error_t jerr;
	size_t i;
	const char *topic_name;
	struct mse_config mse_config;
	struct mse_decoder_info decoder_info;

	memset(&mse_config, 0, sizeof(mse_config));
	mse_decoder_info_create(&decoder_info);

	json_t *listener_config = json_loads(LISTENER_NULL_CONFIG, 0, &jerr);
	const int opaque_creator_rc = parse_decoder_info(&decoder_info,
													listener_config,&topic_name);

	assert_true(0 == opaque_creator_rc);
	json_decref(listener_config);

	/* Currently, uses global_config */
	decoder_info.mse_config = &mse_config;

	// mse_array == NULL
	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	// mse_array null
	assert_null(mse_array);
	const int parse_rc = parse_mse_array(&decoder_info.mse_config->database,
										 mse_array);
	assert(parse_rc != 0);

	struct mse_array *notifications_array = process_mse_buffer(
	        mse_input,
	        strlen(mse_input),
	        "127.0.0.1",
	        &decoder_info,
	        now);

	for (i = 0; notifications_array && i < notifications_array->size; ++i)
		free(notifications_array->data[i].string);

	free(notifications_array);

	mse_decoder_info_destroy(&decoder_info);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

static void testMSE10Decoder14_process_mse_buffer_3(const char *mse_array_str,
                             const char *mse_input, const time_t now) {
	json_error_t jerr;
	size_t i;
	const char *topic_name;
	struct mse_config mse_config;
	struct mse_decoder_info decoder_info;

	memset(&mse_config, 0, sizeof(mse_config));
	mse_decoder_info_create(&decoder_info);

	json_t *listener_config = json_loads(LISTENER_NULL_CONFIG, 0, &jerr);
	const int opaque_creator_rc = parse_decoder_info(&decoder_info,
													 listener_config,&topic_name);

	assert_true(0 == opaque_creator_rc);
	json_decref(listener_config);

	/* Currently, uses global_config */
	decoder_info.mse_config = &mse_config;

	// mse_array == NULL
	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	// mse_array null
	assert_non_null(mse_array);
	const int parse_rc = parse_mse_array(&decoder_info.mse_config->database,
										 mse_array);
	assert(parse_rc != 0);

// Librando cosas:

	struct mse_array *notifications_array = process_mse_buffer(
	        mse_input,
	        strlen(mse_input),
	        "127.0.0.1",
	        &decoder_info,
	        now);

	for (i = 0; notifications_array && i < notifications_array->size; ++i)
		free(notifications_array->data[i].string);

	free(notifications_array);

	mse_decoder_info_destroy(&decoder_info);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}


static const char NO_SUBSCRIPTION_NAME_MSE10_ASSOC[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	            "\"notificationType\":\"association\","
//	            "\"subscriptionName\":\"rb-assoc\","
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

static const char INCORRECT_SUBSCRIPTION_NAME_MSE10_ASSOC[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	            "\"notificationType\":\"association\","
	            "\"subscriptionName\":\"\","
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

static const char ERROR_MAC_MSE10_ASSOC[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	            "\"notificationType\":\"association\","
	            "\"subscriptionName\":\"rb-assoc\","
	            "\"entity\":\"WIRELESS_CLIENTS\","
	            "\"deviceId\":\"00:ca:00:05:06\","
//	            "\"deviceId\":\"00:ca:00:05:06:52\","
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

static const char NOT_MSE10_ASSOC[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	            "\"notificationType\":\"association\","
	            "\"subscriptionName\":\"rb-assoc\","
	        "}"
	    "]"
	"}";

static const char MSE10_ASSOC_NOT_ARRAY[] =
	// *INDENT-OFF*
	"{\"notifications\": \"aaaa\"}";

static const char MSE10_ASSOC_NOT_ARRAY2[] =
	// *INDENT-OFF*
	"{\"notifications\"";

static const char MSE10_ASSOC_NOT_ARRAY3[] =
	// *INDENT-OFF*
	"{\"notifications\": \"aaaa\",\"notifications\": \"aaaa\"}";

static const char MSE10_ASSOC_NO_JSON[] = "";

static const char MSE10_ASSOC_EMPTY_JSON[] = "{}";

static void mytest_mse10_not_array(const char *buffer) {

	json_error_t err;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_non_null(json);

	struct mse_array *notifications = NULL;
	notifications = extract_mse_data(buffer, json);
	assert_null(notifications);

	free(notifications);
	json_decref(json);
}

static void mytest_mse10_not_json(const char *buffer) {

	json_error_t err;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_null(json);

	struct mse_array *notifications = NULL;
	notifications = extract_mse_data(buffer, json);
	assert_null(notifications);

	free(notifications);
	json_decref(json);
}

static void mytest_mse10_not_json2(const char *buffer) {

	json_error_t err;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_non_null(json);

	struct mse_array *notifications = NULL;
	notifications = extract_mse_data(buffer, json);
	assert_null(notifications);

	free(notifications);
	json_decref(json);
}

static void test_extract_mse_data_notification_null(const char *buffer) {

	json_error_t err;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_null(json);

	struct mse_array *notifications = NULL;
	notifications = extract_mse_data(buffer, json);
	assert_null(notifications);

	free(notifications);
	json_decref(json);
}

static void test_extract_mse_data_notification_non_null(const char *buffer) {
	json_error_t err;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_non_null(json);

	struct mse_array *notifications = NULL;
	notifications = extract_mse_data(buffer, json);
	assert_non_null(notifications);

	free(notifications);
	json_decref(json);

}

static void mytest8(const char *buffer) {
	json_error_t err;
	int i;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_non_null(json);
	struct mse_array *mse_ar = NULL;
	mse_ar = extract_mse8_rich_data(json, &i);

//	notifications = extract_mse_data(buffer, json);
	assert_non_null(mse_ar);
	json_decref(json);
	free(mse_ar);
}

static void testMSE10Decoder14_test_men(const char *mse_array_str,
                             const char *_listener_config) {
	json_error_t jerr;
//	size_t i;
	const char *topic_name;
	struct mse_config mse_config;
	struct mse_decoder_info decoder_info;

	memset(&mse_config, 0, sizeof(mse_config));
	mse_decoder_info_create(&decoder_info);

	json_t *listener_config = json_loads(_listener_config, 0, &jerr);
	const int opaque_creator_rc = parse_decoder_info(&decoder_info,
				listener_config,&topic_name);

	assert_true(0 == opaque_creator_rc);
	json_decref(listener_config);

	/* Currently, uses global_config */
	decoder_info.mse_config = &mse_config;

	// mse_array == NULL
	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	assert_true(mse_array);
	const int parse_rc = parse_mse_array(&decoder_info.mse_config->database,
										 mse_array);

	mse_decoder_info_destroy(&decoder_info);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}


static void test_process_mse_buffer_mse_notification_empty() {
	testMSE10Decoder14_process_mse_buffer_3(MSE10_2,
	                NO_SUBSCRIPTION_NAME_MSE10_ASSOC, NOW);
}

static void test_process_mse_buffer_mse_empty() {
	testMSE10Decoder14_process_mse_buffer_2(MSE_EMPTY,
	                NO_SUBSCRIPTION_NAME_MSE10_ASSOC, NOW);
}

static void test_deviceid_as_int() {
	mytest8(MSE8_DEVICE_ID_AS_INT);
}


static void test_mse_buffer_incorrect_subscription_name() {
	testMSE10Decoder14_process_mse_buffer(MSE_ARRAY_IN,
	                INCORRECT_SUBSCRIPTION_NAME_MSE10_ASSOC, NOW);
}

static void test_process_mse_buffer_no_subscription_name() {
	testMSE10Decoder14_process_mse_buffer(MSE_ARRAY_IN,
	                NO_SUBSCRIPTION_NAME_MSE10_ASSOC, NOW);
}


static void test_error_mac_mse10() {
	test_extract_mse_data_notification_non_null(ERROR_MAC_MSE10_ASSOC);
}

static void test_empty_json() {
	mytest_mse10_not_json2(MSE10_ASSOC_EMPTY_JSON);
}

static void test_no_json() {
	test_extract_mse_data_notification_null(MSE10_ASSOC_NO_JSON);
}

static void test_not_mse10_assoc() {
	test_extract_mse_data_notification_null(NOT_MSE10_ASSOC);
}

static void test_mse10_not_array() {
	mytest_mse10_not_array(MSE10_ASSOC_NOT_ARRAY);
}

static void test_mse10_not_json() {
	mytest_mse10_not_json(MSE10_ASSOC_NOT_ARRAY2);
}

static void test_mse10_not_json2() {
	mytest_mse10_not_json2(MSE10_ASSOC_NOT_ARRAY3);
}

static void test_mse_empty() {
	testMSE10Decoder14(MSE_ARRAY_IN);
}

static void test_null_mse() {
	testMSE10Decoder14_null_mse(LISTENER_NULL_CONFIG,
	                MSE_NOT_ARRAY_IN);
}

static void check_test1() {
	testMSE10Decoder14_test_men(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG);
}

static void test1_mem() {
	mem_test(check_test1,LISTENER_NULL_CONFIG);
}

static void testMSE8ConfigInitGlobal() {
	init_global_config();
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(testMSE8ConfigInitGlobal),
		cmocka_unit_test(test_null_mse),
		cmocka_unit_test(test_mse_empty),
		cmocka_unit_test(test_not_mse10_assoc),
		cmocka_unit_test(test_error_mac_mse10),
		cmocka_unit_test(test_process_mse_buffer_no_subscription_name),
		cmocka_unit_test(test_mse_buffer_incorrect_subscription_name),
		cmocka_unit_test(test_deviceid_as_int),
		cmocka_unit_test(test_mse10_not_array),
		cmocka_unit_test(test_mse10_not_json),
		cmocka_unit_test(test_mse10_not_json2),
		cmocka_unit_test(test_no_json),
		cmocka_unit_test(test_empty_json),
		cmocka_unit_test(test_process_mse_buffer_mse_empty),
		cmocka_unit_test(test_process_mse_buffer_mse_notification_empty),
		cmocka_unit_test(test1_mem),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
