
#include "rb_mse_tests.h"

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

static const char MSE10_ASSOC_COVE[] =
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
	            "\"username\":\"AAAAAAA\","
	            "\"timestamp\":1446650950000"
	        "}"
	    "]"
	"}";
		// *INDENT-ON*


static const char MSE10_ASSOC_OLD[] =
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
	            "\"timestamp\":1446640950000"
	        "}"
	    "]"
	"}";
		// *INDENT-ON*

// line 249-255 of rb_mse.c
static const char MSE10_ASSOC_OLD_OLD[] =
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
	            "\"timestamp\":950000"
	        "}"
	    "]"
	"}";
		// *INDENT-ON*

static const char MSE10_LOCKUP[] =
	// *INDENT-OFF*
	    "\"notifications\":["
	        "{"
	            "\"notificationType\":\"locationupdate\","
	            "\"subscriptionName\":\"rb-loc\","
	            "\"entity\":\"WIRELESS_CLIENTS\","
	            "\"deviceId\":\"00:bb:00:02:19:fd\","
	            "\"lastSeen\":\"2015-02-24T08:45:48.154+0000\","
	            "\"ssid\":\"snow-ball-lab\","
	            "\"band\":null,"
	            "\"apMacAddress\":\"00:a2:00:01:dd:00\","
	            "\"locationMapHierarchy\":\"Cisco-Test>Building test>Test-Floor6\","
	            "\"locationCoordinate\":{"
	                "\"x\":147.49353,"
	                "\"y\":125.65644,"
	                "\"z\":0.0,"
	                "\"unit\":\"FEET\""
	            "},"
	            "\"confidenceFactor\":24.0,"
	            "\"timestamp\":1424767548154"
	        "}"
	    "]"
	"}";
		// *INDENT-ON*

static const char MSE10_MANY[] =
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
	            "\"timestamp\":1424767310026"
	        "},{"
	            "\"notificationType\":\"locationupdate\","
	            "\"subscriptionName\":\"rb-loc\","
	            "\"entity\":\"WIRELESS_CLIENTS\","
	            "\"deviceId\":\"00:bb:00:02:19:fd\","
	            "\"lastSeen\":\"2015-02-24T08:45:48.154+0000\","
	            "\"ssid\":\"snow-ball-lab\","
	            "\"band\":null,"
	            "\"apMacAddress\":\"00:a2:00:01:dd:00\","
	            "\"locationMapHierarchy\":\"Cisco-Test>Building test>Test-Floor6\","
	            "\"locationCoordinate\":{"
	                "\"x\":147.49353,"
	                "\"y\":125.65644,"
	                "\"z\":0.0,"
	                "\"unit\":\"FEET\""
	            "},"
	            "\"confidenceFactor\":24.0,"
	            "\"timestamp\":1424767548154"
	        "}"
	    "]"
	"}";
		// *INDENT-ON*

static const char MSE10_ZERO_NOTIFICATIONS[] = "{\"notifications\":[]}";

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

static const char MSE_ARRAY_IN_COVE[] = \
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


static const char MSE_ARRAY_OUT[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": \"rb-assoc0\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_MANY_IN[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": \"rb-assoc\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"},{\n" \
			"\"stream\": \"rb-loc\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char LISTENER_NULL_CONFIG[] = "{}";

#if 0
#define CHECKDATA_BASE(bytes,pkts) { \
	{.key = "type", .value="netflowv10"}, \
	{.key = "src", .value="10.13.122.44"}, \
	{.key = "dst", .value="66.220.152.19"}, \
	{.key = "ip_protocol_version", .value="4"}, \
	{.key = "l4_proto", .value="6"}, \
	{.key = "src_port", .value="54713"}, \
	{.key = "dst_port", .value="443"}, \
	{.key = "flow_end_reason", .value="end of flow"}, \
	{.key = "biflow_direction", .value="initiator"}, \
	{.key = "application_id", .value=NULL}, \
	\
	{.key = "sensor_ip", .value="4.3.2.1"}, \
	{.key = "sensor_name", .value="FlowTest"}, \
	{.key = "bytes", .value=bytes}, \
	{.key = "pkts", .value=pkts}, \
	{.key = "first_switched", .value="1382636953"}, \
	{.key = "timestamp", .value="1382637021"}, \
}
#endif

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

static void testMSE10Decoder_valid_enrich() {
	testMSE10Decoder(MSE_ARRAY_IN,
	                 LISTENER_NULL_CONFIG,
	                 MSE10_ASSOC,
	                 NOW,
	                 checkMSE10Decoder_valid_enrich);
}

static void checkMSE10Decoder_novalid_enrich(struct mse_array
        *notifications_array) {
	/* No database -> output == input */
	assert_int_equal(notifications_array->size, 1);
	/* But invalid MSE => No string */
	assert_null(notifications_array->data[0].string);
}

static void testMSE10Decoder_novalid_enrich() {
	testMSE10Decoder(MSE_ARRAY_OUT,
	                 LISTENER_NULL_CONFIG,
	                 MSE10_ASSOC,
	                 NOW,
	                 checkMSE10Decoder_novalid_enrich);
}

static void checkMSE10Decoder_multi(struct mse_array *notifications_array) {
	const char *subscriptionName1 = NULL, *sensor_name1 = NULL;
	json_int_t sensor_id1 = 0;
	const char *subscriptionName2 = NULL, *sensor_name2 = NULL;
	json_int_t sensor_id2 = 0;
	json_error_t jerr;

	/* No database -> output == input */
	assert_int_equal(notifications_array->size, 2);

	assert_non_null(notifications_array->data[0].string);
	assert_int_equal(
	    notifications_array->data[0].string_size,
	    strlen(notifications_array->data[0].string)
	);
	json_t *ret1 = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert_non_null(ret1);
	const int unpack_rc1 = json_unpack_ex(ret1, &jerr, 0,
	                                      "{s:[{s:s,s:s,s:i}]}",
	                                      "notifications",
	                                      "subscriptionName", &subscriptionName1,
	                                      "sensor_name", &sensor_name1,
	                                      "sensor_id", &sensor_id1);

	assert_non_null(notifications_array->data[1].string);
	assert_int_equal(
	    notifications_array->data[1].string_size,
	    strlen(notifications_array->data[1].string)
	);

	json_t *ret2 = json_loads(notifications_array->data[1].string, 0, &jerr);
	assert_non_null(ret2);
	const int unpack_rc2 = json_unpack_ex(ret2, &jerr, 0,
	                                      "{s:[{s:s,s:s,s:i}]}",
	                                      "notifications",
	                                      "subscriptionName", &subscriptionName2,
	                                      "sensor_name", &sensor_name2,
	                                      "sensor_id", &sensor_id2);

	assert_int_equal(unpack_rc1, 0);
	assert_int_equal(unpack_rc2, 0);
	assert_string_equal(subscriptionName1, "rb-assoc");
	assert_string_equal(sensor_name1, "testing");
	assert_int_equal(sensor_id1, 255);
	assert_string_equal(subscriptionName2, "rb-loc");
	assert_string_equal(sensor_name2, "testing");
	assert_int_equal(sensor_id2, 255);
	json_decref(ret1);
	json_decref(ret2);
}

static void checkMSE8_valid_timestamp(struct mse_array *notifications_array) {
	/* No database -> output == input */

	assert_int_equal(notifications_array->size, 1);
	assert_int_equal(
	    notifications_array->data[0].string_size,
	    strlen(notifications_array->data[0].string)
	);

	json_int_t timestamp = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert_non_null(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:[{s:I}]}",
	                                     "notifications",
	                                     "timestamp", &timestamp);
	assert_int_equal(unpack_rc, 0);
	assert_true(timestamp / 1000 == NOW);
	assert_true(notifications_array->data[0].timestamp_warnings == 0);

	json_decref(ret);
}

static void checkMSE8_invalid_timestamp(struct mse_array *notifications_array) {
	/* No database -> output == input */

	assert_int_equal(notifications_array->size, 1);
	assert_int_equal(
	    notifications_array->data[0].string_size,
	    strlen(notifications_array->data[0].string)
	);

	json_int_t timestamp = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert_non_null(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:[{s:I}]}",
	                                     "notifications",
	                                     "timestamp", &timestamp);
	assert_int_equal(unpack_rc, 0);
	assert_true(timestamp / 1000 != NOW);
	assert_true(notifications_array->data[0].timestamp_warnings > 0);

	json_decref(ret);
}

/// @TODO use a for loop
static void checkMSE10Decoder_empty_array(struct mse_array
        *notifications_array) {
	/* No database -> output == input */
	assert_true(!notifications_array ||  notifications_array->size == 0);
}

static void testMSE10Decoder_valid_enrich_multi() {
	testMSE10Decoder(MSE_ARRAY_MANY_IN,
	                 LISTENER_NULL_CONFIG,
	                 MSE10_MANY,
	                 NOW,
	                 checkMSE10Decoder_multi);
}

static void testMSE10Decoder_empty_array() {
	testMSE10Decoder(MSE_ARRAY_IN,
	                 LISTENER_NULL_CONFIG,
	                 MSE10_ZERO_NOTIFICATIONS,
	                 NOW,
	                 checkMSE10Decoder_empty_array);
}

static void testMSE8Decoder_valid_timestamp() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE10_ASSOC,
	                NOW,
	                checkMSE8_valid_timestamp);
}

static void testMSE8Decoder_invalid_timestamp() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE10_ASSOC_OLD,
	                NOW,
	                checkMSE8_invalid_timestamp);
}

static void testMSE8Decoder_null_values_cove() {
	testMSE8Decoder(MSE_ARRAY_IN_COVE,
	                LISTENER_NULL_CONFIG,
	                MSE10_ASSOC_COVE,
	                NOW,
	                checkMSE8_valid_timestamp);
}

static void testMSE10Decoder_invalid_enrich() {
	testMSE10Decoder(MSE_ARRAY_IN_COVE,
	                 LISTENER_NULL_CONFIG,
	                 MSE10_ASSOC_COVE,
	                 NOW,
	                 checkMSE10Decoder_valid_enrich);
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(testMSE10Decoder_valid_enrich),
		cmocka_unit_test(testMSE10Decoder_novalid_enrich),
		cmocka_unit_test(testMSE10Decoder_valid_enrich_multi),
		cmocka_unit_test(testMSE10Decoder_empty_array),
		cmocka_unit_test(testMSE8Decoder_valid_timestamp),
		cmocka_unit_test(testMSE8Decoder_invalid_timestamp),
		//cmocka_unit_test(testMSE8Decoder_null_values_cove),
		cmocka_unit_test(testMSE10Decoder_invalid_enrich)
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
