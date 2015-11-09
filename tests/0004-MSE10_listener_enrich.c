#include "../src/decoder/rb_mse.c"

#include "rb_json_tests.c"
#include "rb_mse_tests.h"

/// @TODO test behaviour with overlap + default

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
	            "\"timestamp\":1424767310026"
	        "}"
	    "]"
	"}";
		// *INDENT-ON*

static const char MSE10_LOCUP[] =
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

static const char MSE_ARRAY_DEFAULT_IN[] = \
	// *INDENT-OFF*
	"[\n"
		"{\n"
			"\"stream\": \"rb-assoc\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"testing\"\n"
				", \"sensor_id\": 255\n"
			"}\n"
		"},{\n"
			"\"stream\": \"rb-loc\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"testing\"\n"
				", \"sensor_id\": 255\n"
			"}\n"
		"},{\n"
			"\"stream\": \"*\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"default_stream\"\n"
				", \"sensor_id\": 254\n"
			"}\n"
		"}\n"
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_DEFAULT_OUT[] = \
	// *INDENT-OFF*
	"[\n"
		"{\n"
			"\"stream\": \"rb-assoc0\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"testing\"\n"
				", \"sensor_id\": 255\n"
			"}\n"
		"},{\n"
			"\"stream\": \"rb-loc0\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"testing\"\n"
				", \"sensor_id\": 255\n"
			"}\n"
		"},{\n"
			"\"stream\": \"*\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"default_stream\"\n"
				", \"sensor_id\": 254\n"
			"}\n"
		"}\n"
	"]";
	// *INDENT-ON*

static const char LISTENER_CONFIG_NO_OVERLAP[] = \
        "{\"enrichment\":{\"a\":\"b\"}}";

static const char LISTENER_CONFIG_OVERLAP[] = \
        "{\"enrichment\":{\"sensor_name\":\"sensor_listener\",\"a\":\"b\"}}";

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

static void checkMSE10Decoder_no_overlap(struct mse_array
        *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(
	           notifications_array->data[0].string));

	const char *subscriptionName = NULL, *sensor_name = NULL, *a_value = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:[{s:s,s:s,s:i,s:s}]}",
	                                     "notifications",
	                                     "subscriptionName", &subscriptionName,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id,
	                                     "a", &a_value);

	assert(unpack_rc == 0);
	assert(0 == strcmp(subscriptionName, "rb-assoc"));
	assert(0 == strcmp(sensor_name, "testing"));
	assert(0 == strcmp(a_value, "b"));
	assert(255 == sensor_id);
	json_decref(ret);
}

static void testMSE10Decoder_no_overlap() {
	testMSE10Decoder(MSE_ARRAY_IN,
	                 LISTENER_CONFIG_NO_OVERLAP,
	                 MSE10_ASSOC,
	                 NOW,
	                 checkMSE10Decoder_no_overlap);
}

static void checkMSE10Decoder_overlap(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(
	           notifications_array->data[0].string));

	const char *subscriptionName = NULL, *sensor_name = NULL, *a_value = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:[{s:s,s:s,s:i,s:s}]}",
	                                     "notifications",
	                                     "subscriptionName", &subscriptionName,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id,
	                                     "a", &a_value);

	assert(unpack_rc == 0);
	assert(0 == strcmp(subscriptionName, "rb-assoc"));
	assert(0 == strcmp(sensor_name, "sensor_listener"));
	assert(0 == strcmp(a_value, "b"));
	assert(255 == sensor_id);
	json_decref(ret);
}

static void testMSE10Decoder_overlap() {
	testMSE10Decoder(MSE_ARRAY_IN,
	                 LISTENER_CONFIG_OVERLAP,
	                 MSE10_ASSOC,
	                 NOW,
	                 checkMSE10Decoder_overlap);
}

/**
 * Testing with default enrichment, the stream inserted does contains a
 * registered MSE stream so it should not enrich with default but with valid
 * enrich.
 */
static void testMSE10Decoder_default_hit() {
	// Same check as if they were no default
	testMSE10Decoder(MSE_ARRAY_DEFAULT_IN,
	                 LISTENER_CONFIG_NO_OVERLAP,
	                 MSE10_ASSOC,
	                 NOW,
	                 checkMSE10Decoder_no_overlap);
}

static void checkMSE10Decoder_default_miss(struct mse_array
        *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(
	           notifications_array->data[0].string));

	const char *subscriptionName = NULL, *sensor_name = NULL, *a_value = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:[{s:s,s:s,s:i,s:s}]}",
	                                     "notifications",
	                                     "subscriptionName", &subscriptionName,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id,
	                                     "a", &a_value);

	assert(unpack_rc == 0);
	assert(0 == strcmp(subscriptionName, "rb-assoc"));
	assert(0 == strcmp(sensor_name, "default_stream"));
	assert(0 == strcmp(a_value, "b"));
	assert(254 == sensor_id);
	json_decref(ret);
}

/**
 * Testing with default enrichment, the stream inserted does not contains a
 * registered MSE stream so it should enrich with default.
 */
static void testMSE10Decoder_default_miss() {
	testMSE10Decoder(MSE_ARRAY_DEFAULT_OUT,
	                 LISTENER_CONFIG_NO_OVERLAP,
	                 MSE10_ASSOC,
	                 NOW,
	                 checkMSE10Decoder_default_miss);
}

int main() {
	testMSE10Decoder_no_overlap();
	testMSE10Decoder_overlap();
	testMSE10Decoder_default_hit();
	testMSE10Decoder_default_miss();

	return 0;
}
