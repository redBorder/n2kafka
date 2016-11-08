#include "rb_mse_tests.h"
#include "rb_mem_tests.h"

static const time_t NOW = 1446650950;

/// @TODO test behaviour with overlap + default

static const char MSE8_PROBING[] =
	// *INDENT-OFF*
	"{\"StreamingNotification\":{"
		"\"subscriptionName\":\"MSE_SanCarlos\","
		"\"entity\":\"WIRELESS_CLIENTS\","
		"\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
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
		// *INDENT-ON*

static const char MSE8_GEO_PROBING[] =
	// *INDENT-OFF*
    "{"
        "\"StreamingNotification\":{"
            "\"subscriptionName\":\"MSE_Auditorio\","
            "\"entity\":\"WIRELESS_CLIENTS\","
            "\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
            "\"mseUdi\":\"AIR-MSE-VA-K9:V01:AS1-vMSE-8-CAS_af9752da-b07c-11e4-88d6-000c29defead\","
            "\"floorRefId\":0,"
            "\"timestampMillis\":1428807811488,"
            "\"location\":{"
                "\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
                "\"currentlyTracked\":true,"
                "\"confidenceFactor\":184.0,"
                "\"band\":\"UNKNOWN\","
                "\"dot11Status\":\"PROBING\","
                "\"guestUser\":false,"
                "\"MapInfo\":{"
                    "\"mapHierarchyString\":\"Auditorio Nacional>Interior>Piso 1 / Balcones\","
                    "\"floorRefId\":748832332651168276,"
                    "\"Dimension\":{"
                        "\"length\":103.3,"
                        "\"width\":146.3,"
                        "\"height\":12.0,"
                        "\"offsetX\":0.0,"
                        "\"offsetY\":0.0,"
                        "\"unit\":\"METER\""
                    "},"
                    "\"Image\":{"
                        "\"imageName\":\"domain_0_1419284962106.jpg\""
                    "}"
                "},"
                "\"MapCoordinate\":{"
                    "\"x\":73.93687,"
                    "\"y\":101.646225,"
                    "\"unit\":\"METER\""
                "},"
                "\"Statistics\":{"
                    "\"currentServerTime\":\"2015-04-11T22:03:31.490-0500\","
                    "\"firstLocatedTime\":\"2015-04-11T22:03:09.284-0500\","
                    "\"lastLocatedTime\":\"2015-04-11T22:03:31.486-0500\""
                "},"
                "\"GeoCoordinate\":{"
                    "\"latitude\":59.42418269038639,"
                    "\"longitude\":-89.19495114405473,"
                    "\"unit\":\"DEGREES\""
                "}"
            "},"
            "\"timestamp\":\"2015-04-11T22:03:31.488-0500\""
        "}"
    "}";
    	// *INDENT-ON*

static const char MSE8_ASSOC[] =
	// *INDENT-OFF*
	"{"
	    "\"StreamingNotification\":{"
	        "\"subscriptionName\":\"MSE_SanCarlos\","
	        "\"entity\":\"WIRELESS_CLIENTS\","
	        "\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
	        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
	        "\"floorRefId\":0,"
	        "\"timestampMillis\":1426496270000,"
	        "\"location\":{"
	            "\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
	            "\"currentlyTracked\":true,"
	            "\"confidenceFactor\":200.0,"
	            "\"ipAddress\":["
	                "\"10.71.13.105\","
	                "\"fe80:0000:0000:0000:452d:c075:65a1:454d\""
	            "],"
	            "\"ssId\":\"Hyatt\","
	            "\"band\":\"UNKNOWN\","
	            "\"apMacAddress\":\"bb:bb:bb:bb:bb:bb\","
	            "\"dot11Status\":\"ASSOCIATED\","
	            "\"guestUser\":false,"
	            "\"MapInfo\":{"
	                "\"mapHierarchyString\":\"SantaClara Convention>Hyatt>HSC-FL06\","
	                "\"floorRefId\":4698041283715793158,"
	                "\"Dimension\":{"
	                    "\"length\":125.6,"
	                    "\"width\":407.8,"
	                    "\"height\":10.0,"
	                    "\"offsetX\":0.0,"
	                    "\"offsetY\":0.0,"
	                    "\"unit\":\"FEET\""
	                "},"
	                "\"Image\":{"
	                    "\"imageName\":\"domain_0_1383773918953.png\""
	                "}"
	            "},"
	            "\"MapCoordinate\":{"
	                "\"x\":214.93187,"
	                "\"y\":38.06076,"
	                "\"unit\":\"FEET\""
	            "},"
	            "\"Statistics\":{"
	                "\"currentServerTime\":\"2015-03-16T01:57:50.001-0700\","
	                "\"firstLocatedTime\":\"2015-03-15T19:44:43.207-0700\","
	                "\"lastLocatedTime\":\"2015-03-16T01:57:49.993-0700\""
	            "}"
	        "},"
	        "\"timestamp\":\"2015-03-16T01:57:50.000-0700\""
	    "}"
	"}";
		// *INDENT-ON*

static const char MSE_ARRAY_IN[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": \"MSE_SanCarlos\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"MSE_testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_IN_UNKNOWN_STREAM[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": \"MSE_SanCarlosTEST\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"MSE_testingTEST\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_ORDER[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": \"MSE_SanCarlos\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"MSE_MSE\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_IN_MSE[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": \"MSE_SanCarlos\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"MSE_testing\"\n" \
				", \"entity\":\"WIRELESS_CLIENTS_MSE\""
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_OUT[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": \"MSE_SanCarlos0\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"MSE_testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_IN_WITH_DEFAULT[] =
	// *INDENT-OFF*
	"[\n"
		"{\n"
			"\"stream\": \"MSE_SanCarlos\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"MSE_testing\"\n"
				", \"sensor_id\": 255\n"
			"}\n"
		"},{\n"
			"\"stream\": \"*\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"MSE_default\"\n"
				", \"sensor_id\": 254\n"
			"}\n"
		"}\n"
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_OUT_WITH_DEFAULT[] =
	// *INDENT-OFF*
	"[\n"
		"{\n"
			"\"stream\": \"MSE_SanCarlos0\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"MSE_testing\"\n"
				", \"sensor_id\": 255\n"
			"}\n"
		"},{\n"
			"\"stream\": \"*\" \n"
			",\"enrichment\":{\n"
				"\"sensor_name\": \"MSE_default\"\n"
				", \"sensor_id\": 254\n"
			"}\n"
		"}\n"
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_IN_INVALID_STREAM[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": 123\n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"testing\"\n" \
				", \"sensor_id\": 2255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_IN_NULL_STREAM[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"stream\": null\n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"testing\"\n" \
				", \"sensor_id\": 2255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_ARRAY_NO_STREAM[] = \
	// *INDENT-OFF*
	"[\n" \
		"{\n" \
			"\"streamTEST\": \"MSE_SanCarlos\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"MSE_testing\"\n" \
				", \"entity\":\"WIRELESS_CLIENTS_MSE\""
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";
	// *INDENT-ON*

static const char MSE_NULL_CONFIG[] = "{}";

static const char LISTENER_NULL_CONFIG[] = "{}";

static const char LISTENER_CONFIG_NO_OVERLAP[] = \
        "{\"enrichment\":{\"a\":\"b\"}}";

static const char LISTENER_CONFIG_OVERLAP[] = \
        "{\"enrichment\":{\"sensor_name\":\"sensor_listener\",\"a\":\"b\"}}";

static const char LISTENER_CONFIG_OVERLAP_WITH_MSE8_PROBING[] = \
        "{\"enrichment\":{\"sensor_name\":\"sensor_listener\",\"a\":\"b\",\"entity\":\"WIRELESS_CLIENTS_LISTENER\"}}";

static const char LISTENER_CONFIG_ORDER[] = \
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

static void checkMSE8Enrichment_no_overlap(struct mse_array
        *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(
	           notifications_array->data[0].string));

	const char *subscriptionName = NULL, *sensor_name = NULL, *a_value = NULL;
	const char *entity_value = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:{s:s,s:s,s:s,s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "entity",&entity_value,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id,
	                                     "a", &a_value
	                                     );
	assert(unpack_rc == 0);
	assert(0 == strcmp(subscriptionName, "MSE_SanCarlos"));
	assert(0 == strcmp(sensor_name, "MSE_testing"));
	assert(0 == strcmp(a_value, "b"));
	assert_non_null(entity_value);
	assert(0 == strcmp(entity_value, "WIRELESS_CLIENTS"));

	assert(255 == sensor_id);
	json_decref(ret);
}

static void testMSE8Enrichment_no_overlap() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_CONFIG_NO_OVERLAP,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8Enrichment_no_overlap);
}

static void checkMSE8Enrichment_with_mse8_sensor_name_mse(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(
	           notifications_array->data[0].string));

	const char *subscriptionName = NULL, *sensor_name = NULL, *a_value = NULL, *entity_value = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:{s:s,s:s,s:s,s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "entity",&entity_value,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id,
	                                     "a", &a_value
	                                     );

	assert(unpack_rc == 0);
	assert(0 == strcmp(subscriptionName, "MSE_SanCarlos"));
	assert(0 == strcmp(sensor_name, "MSE_MSE"));
	assert(0 == strcmp(a_value, "b"));
	assert_non_null(entity_value);
	assert(0 != strcmp(entity_value, "WIRELESS_CLIENTS_LISTENER"));
	assert(255 == sensor_id);
	json_decref(ret);
}

static void checkMSE8Enrichment_overlap_with_mse8_probing(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(
	           notifications_array->data[0].string));

	const char *subscriptionName = NULL, *sensor_name = NULL, *a_value = NULL, *entity_value = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:{s:s,s:s,s:s,s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "entity",&entity_value,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id,
	                                     "a", &a_value
	                                     );

	assert(unpack_rc == 0);
	assert(0 == strcmp(subscriptionName, "MSE_SanCarlos"));
	assert(0 == strcmp(sensor_name, "sensor_listener"));
	assert(0 == strcmp(a_value, "b"));
	assert_non_null(entity_value);
	assert(0 != strcmp(entity_value, "WIRELESS_CLIENTS_LISTENER"));
	assert(255 == sensor_id);
	json_decref(ret);
}
static void checkMSE8Enrichment_hit_overlap_default(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(
	           notifications_array->data[0].string));

	const char *subscriptionName = NULL, *sensor_name = NULL, *a_value = NULL, *entity_value = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:{s:s,s:s,s:s,s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "entity",&entity_value,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id,
	                                     "a", &a_value
	                                     );

	assert(unpack_rc == 0);
	assert(0 == strcmp(subscriptionName, "MSE_SanCarlos"));
	assert(0 == strcmp(sensor_name, "sensor_listener")); /* from LISTENER_CONFIG_OVERLAP */
	assert(0 == strcmp(a_value, "b"));
	assert_non_null(entity_value);
	assert(0 != strcmp(entity_value, "WIRELESS_CLIENTS_LISTENER"));
	assert(254 == sensor_id); /* from MSE_ARRAY_OUT_WITH_DEFAULT */
	json_decref(ret);
}

static void checkMSE8Enrichment_overlap(struct mse_array *notifications_array) {
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
	                                     "{s:{s:s,s:s,s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id,
	                                     "a", &a_value);

	assert(unpack_rc == 0);
	assert(0 == strcmp(subscriptionName, "MSE_SanCarlos"));
	assert(0 == strcmp(sensor_name, "sensor_listener"));
	assert(0 == strcmp(a_value, "b"));
	assert(255 == sensor_id);
	json_decref(ret);
}

static void test0003MSE10Decoder_invalid_mse(const char *mse_array_str,
						const char *_listener_config,
						const char *mse_input,
						const time_t now,
						void (*check_result)(struct mse_array *)) {
	json_error_t jerr;
	size_t i;
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

	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	assert_true(mse_array);
	const int parse_rc = parse_mse_array(&decoder_info.mse_config->database,
								mse_array);
	assert_true(parse_rc != 0);   /* */

	struct mse_array *notifications_array = process_mse_buffer(
			mse_input,
			strlen(mse_input),
			"127.0.0.1",
			&decoder_info,
			now);

	check_result(notifications_array);

	for (i = 0; notifications_array && i < notifications_array->size; ++i)
		free(notifications_array->data[i].string);

	free(notifications_array);
	mse_decoder_info_destroy(&decoder_info);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

static void checkMSEDecoder_null_config(struct mse_array
					*notifications_array) {
	/* No database -> output == input */
	assert_int_equal(notifications_array->size, 1);
	/* But invalid MSE => No string */
	assert_null(notifications_array->data[0].string);
}

static void testMSE8Enrichment_overlap() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_CONFIG_OVERLAP,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8Enrichment_overlap);
}

/**
 * Listener enrichment: EMPTY
 * MSE enrichment: 		\"sensor_name\":\"sensor_listener\",
 * Message without sensor_name element.
 * RESULT: sensor_name : sensor_listener
 */
static void testMSE8Enrichment_overlap_with_mse8_No_listener() {
	testMSE8Decoder(MSE_ARRAY_ORDER,
	                LISTENER_CONFIG_NO_OVERLAP,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8Enrichment_with_mse8_sensor_name_mse);
}

/**
 * Listener enrichment: "\"sensor_name\": \"sensor_listener\"\n"
 * MSE enrichment: 		\"sensor_name\":\"sensor_mse\",
 * Message without sensor_name element.
 * RESULT: sensor_name : sensor_listener
 */
static void testMSE8Enrichment_overlap_with_mse8_probing_Order() {
	testMSE8Decoder(MSE_ARRAY_ORDER,
	                LISTENER_CONFIG_ORDER,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8Enrichment_overlap_with_mse8_probing);
}

/**
 * Testing with Listener enrichment, the stream inserted does not contains a
 * Overlap Listener enrichment. The MSE contains also the "\"entity\":\"WIRELESS_CLIENTS\","
 * entity_value contains "WIRELESS_CLIENTS" instead of entity_value, "WIRELESS_CLIENTS_LISTENER"
 * The stream does not update with this new value.
 * Entity is in the original message
 * Listener enrichment: "entity":"WIRELESS_CLIENTS_LISTENERS"
 * MSE enrichment: 		"entity":"WIRELESS_CLIENTS_MSE"
 * Message: "entity":"WIRELESS_CLIENTS"
 * RESULT: "entity":"WIRELESS_CLIENTS"
 */
static void testMSE8Enrichment_overlap_with_mse8_probing_MSE() {
	testMSE8Decoder(MSE_ARRAY_IN_MSE,
	                LISTENER_CONFIG_OVERLAP_WITH_MSE8_PROBING,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8Enrichment_overlap_with_mse8_probing);
}


/**
 * Testing with Listener enrichment, the stream inserted does not contains a
 * Overlap Listener enrichment.
 * entity_value contains "WIRELESS_CLIENTS" instead of entity_value, "WIRELESS_CLIENTS_LISTENER"
 * The stream does not update with this new value.
 * Entity is in the original message
 */
static void testMSE8Enrichment_overlap_with_mse8_probing() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_CONFIG_OVERLAP_WITH_MSE8_PROBING,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8Enrichment_overlap_with_mse8_probing);
}

static void checkMSE8DefaultEnrichment_miss(struct mse_array
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
	                                     "{s:{s:s,s:s,s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id,
	                                     "a", &a_value);

	assert(unpack_rc == 0);
	assert(0 == strcmp(subscriptionName, "MSE_SanCarlos"));
	assert(0 == strcmp(sensor_name, "MSE_default"));
	assert(0 == strcmp(a_value, "b"));
	assert(254 == sensor_id);
	json_decref(ret);
}

/**
 * Testing with default enrichment, the stream inserted does contains a
 * registered MSE stream so it should not enrich with default but with valid
 * enrich.
 */
static void testMSE8DefaultEnrichment_hit() {
	testMSE8Decoder(MSE_ARRAY_IN_WITH_DEFAULT,
	                LISTENER_CONFIG_NO_OVERLAP,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8Enrichment_no_overlap);
}

/**
 * Testing with default enrichment, the stream inserted does not contains a
 * registered MSE stream so it should enrich with default.
 */
static void testMSE8DefaultEnrichment_miss() {
	testMSE8Decoder(MSE_ARRAY_OUT_WITH_DEFAULT,
	                LISTENER_CONFIG_NO_OVERLAP,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8DefaultEnrichment_miss);
}

/**
 * Listener enrichment: "\"sensor_name\": \"sensor_listener\""
 * MSE enrichment: 		\"sensor_name\":\"sensor_mse\",
 * Message: Correct message.
 * RESULT: sensor_name : sensor_listener
 */

static void testMSE8DefaultEnrichment_hit_overlap() {
	testMSE8Decoder(MSE_ARRAY_IN_WITH_DEFAULT,
		                LISTENER_CONFIG_OVERLAP,
		                MSE8_PROBING,
		                NOW,
		                checkMSE8Enrichment_overlap_with_mse8_probing);
}

/**
 * Listener enrichment: "\"sensor_name\": \"sensor_listener\""
 * MSE enrichment: 		\"sensor_name\":\"sensor_mse\",
 * Message: Correct message
 * RESULT: sensor_name : sensor_listener
 */
static void testMSE8DefaultEnrichment_hit_overlap_sensor_name_int() {
	test0003MSE10Decoder_invalid_mse(MSE_ARRAY_IN_INVALID_STREAM,
									 LISTENER_CONFIG_OVERLAP,
									 MSE8_PROBING,
									 NOW,
									 checkMSEDecoder_null_config);
}

/**
 * Listener enrichment: "\"sensor_name\": \"sensor_listener\"\n"
 * MSE enrichment: 		\"sensor_name\":\"sensor_defaul\","
 *						\"sensor_id\": 254\n"
 *						\"stream\": \"*\" \n"
 * Message Correct message
 * RESULT: sensor_name : sensor_listener
 */

static void testMSE8DefaultEnrichment_hit_overlap_default() {
	testMSE8Decoder(MSE_ARRAY_OUT_WITH_DEFAULT,
		                LISTENER_CONFIG_OVERLAP,
		                MSE8_PROBING,
		                NOW,
		                checkMSE8Enrichment_hit_overlap_default);
}

/**
 * Listener enrichment: ""
 * MSE enrichment: 		""
 * Message: sensor_name element.
 * RESULT: No valid message
 */
static void testMSE8_mse_null_config() {
	test0003MSE10Decoder_invalid_mse(MSE_NULL_CONFIG,
			LISTENER_NULL_CONFIG,
			MSE8_PROBING,
			NOW,
			checkMSEDecoder_null_config);
}

/**
 * Listener enrichment: ""
 * MSE enrichment: 		unknown_stream
 * Message: Correct
 * RESULT: No valid message
 */
static void testMSE8Enrichment_unknown_stream() {
	testMSE8Decoder(MSE_ARRAY_IN_UNKNOWN_STREAM,
	                LISTENER_CONFIG_NO_OVERLAP,
	                MSE8_PROBING,
	                NOW,
	                checkMSEDecoder_null_config);
}

/**
 * Listener enrichment: ""
 * MSE enrichment: 		null_stream
 * Message: Correct
 * RESULT: No valid message
 */
static void testMSE8Enrichment_null_stream() {
	test0003MSE10Decoder_invalid_mse(MSE_ARRAY_IN_NULL_STREAM,
	                LISTENER_CONFIG_NO_OVERLAP,
	                MSE8_PROBING,
	                NOW,
	                checkMSEDecoder_null_config);
}

/**
 * Listener enrichment: ""
 * MSE enrichment: 		MSE_ARRAY_NO_STREAM
 * Message: Correct
 * RESULT: No valid message
 */
static void testMSE8Enrichment_no_stream() {
	test0003MSE10Decoder_invalid_mse(MSE_ARRAY_NO_STREAM,
	                LISTENER_CONFIG_NO_OVERLAP,
	                MSE8_PROBING,
	                NOW,
	                checkMSEDecoder_null_config);
}
int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(testMSE8Enrichment_no_overlap),
		cmocka_unit_test(testMSE8Enrichment_overlap),
		cmocka_unit_test(testMSE8DefaultEnrichment_hit),
		cmocka_unit_test(testMSE8DefaultEnrichment_miss),
		cmocka_unit_test(testMSE8Enrichment_overlap_with_mse8_probing),
		cmocka_unit_test(testMSE8Enrichment_overlap_with_mse8_probing_MSE),
		cmocka_unit_test(testMSE8Enrichment_overlap_with_mse8_probing_Order),
		cmocka_unit_test(testMSE8Enrichment_overlap_with_mse8_No_listener),
		cmocka_unit_test(testMSE8DefaultEnrichment_hit_overlap),
		cmocka_unit_test(testMSE8DefaultEnrichment_hit_overlap_default),
		cmocka_unit_test(testMSE8DefaultEnrichment_hit_overlap_sensor_name_int),
		cmocka_unit_test(testMSE8_mse_null_config),
		cmocka_unit_test(testMSE8Enrichment_unknown_stream),
		cmocka_unit_test(testMSE8Enrichment_null_stream),
		cmocka_unit_test(testMSE8Enrichment_no_stream)
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
