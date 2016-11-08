#include "rb_json_tests.c"
#include "rb_mse_tests.h"

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

static const time_t NOW = 1446650950;

static const char MSE8_PROBING[] =
    // *INDENT-OFF*
    "{\"StreamingNotification\":{"
        "\"subscriptionName\":\"MSE_SanCarlos\","
        "\"entity\":\"WIRELESS_CLIENTS\","
        "\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
        "\"floorRefId\":0,"
        "\"timestampMillis\":1446650950000,"
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

static const char MSE8_EMPTY_SUBSCRIPTION_NAME[] =
    // *INDENT-OFF*
    "{\"StreamingNotification\":{"
        "\"subscriptionName\":\"\","
        "\"entity\":\"WIRELESS_CLIENTS\","
        "\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
        "\"floorRefId\":0,"
        "\"timestampMillis\":1446650950000,"
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

static const char MSE8_EMPTY_DEVICEID_AND_MAC[] =
    // *INDENT-OFF*
    "{\"StreamingNotification\":{"
        "\"subscriptionName\":\"MSE_SanCarlos\","
        "\"entity\":\"WIRELESS_CLIENTS\","
        "\"deviceId\":\"\","
        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
        "\"floorRefId\":0,"
        "\"timestampMillis\":1446650950000,"
        "\"location\":{"
            "\"macAddress\":\"\","
            "\"currentlyTracked\":true,"
            "\"confidenceFactor\":88.0,"
            "\"userName\":\"cc:aa:aa:aa:aa:ab\","
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

static const char MSE8_EMPTY_DEVICEID[] =
    // *INDENT-OFF*
    "{\"StreamingNotification\":{"
        "\"subscriptionName\":\"MSE_SanCarlos\","
        "\"entity\":\"WIRELESS_CLIENTS\","
        "\"deviceId\":\"\","
        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
        "\"floorRefId\":0,"
        "\"timestampMillis\":1446650950000,"
        "\"location\":{"
            "\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
            "\"currentlyTracked\":true,"
            "\"confidenceFactor\":88.0,"
            "\"userName\":\"cc:aa:aa:aa:aa:ab\","
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

static const char MSE8_EMPTY_TIMESTAMP[] =
    // *INDENT-OFF*
    "{\"StreamingNotification\":{"
        "\"subscriptionName\":\"MSE_SanCarlos\","
        "\"entity\":\"WIRELESS_CLIENTS\","
        "\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
        "\"floorRefId\":0,"
        "\"timestampMillis\":0,"
        "\"location\":{"
            "\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
            "\"currentlyTracked\":true,"
            "\"confidenceFactor\":88.0,"
            "\"userName\":\"cc:aa:aa:aa:aa:ab\","
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
            "\"lastLocatedTime\":\"2015-03-16T01:57:49.991-0700\"}}"
            //"\"timestamp\":\"2015-03-16T01:57:50.000-0700\""
        "}"
    "}";
    // *INDENT-ON*

static const char MSE8_CORRECT[] =
    // *INDENT-OFF*
    "{\"StreamingNotification\":{"
        "\"subscriptionName\":\"MSE_SanCarlos\","
        "\"entity\":\"WIRELESS_CLIENTS\","
        "\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
        "\"floorRefId\":0,"
        "\"timestampMillis\":1446650950000,"
        "\"location\":{"
            "\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
            "\"currentlyTracked\":true,"
            "\"confidenceFactor\":88.0,"
            "\"userName\":\"cc:aa:aa:aa:aa:ab\","
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
static const char MSE8_CORRECT_TWO[] =
    // *INDENT-OFF*
    "{\"StreamingNotification\":{"
        "\"subscriptionName\":\"MSE_SanCarlos\","
        "\"entity\":\"WIRELESS_CLIENTS\","
        "\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
        "\"floorRefId\":0,"
        "\"timestampMillis\":1000000950000,"
        "\"location\":{"
            "\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
            "\"currentlyTracked\":true,"
            "\"confidenceFactor\":88.0,"
            "\"userName\":\"cc:aa:aa:aa:aa:ab\","
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
            "\"currentServerTime\":\"2000-03-16T01:57:50.000-0700\","
            "\"firstLocatedTime\":\"2000-03-16T01:57:49.991-0700\","
            "\"lastLocatedTime\":\"2000-03-16T01:57:49.991-0700\"}},"
            "\"timestamp\":\"2000-03-16T01:57:50.000-0700\""
        "}"
    "}";
    // *INDENT-ON*


static const char MSE8_PROBING_OLD[] =
    // *INDENT-OFF*
    "{\"StreamingNotification\":{"
        "\"subscriptionName\":\"MSE_SanCarlos\","
        "\"entity\":\"WIRELESS_CLIENTS\","
        "\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
        "\"floorRefId\":0,"
        "\"timestampMillis\":1446640950000,"
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

static void testMSE10Decoder_two(const char *mse_array_str,
                             const char *_listener_config,
                             const char *mse_input1,
                             const char *mse_input2,
                             const time_t now) {
	json_error_t jerr;
	size_t i;
	const char *topic_name;
	struct mse_config mse_config;
	struct mse_decoder_info decoder_info;

	init_global_config();

	memset(&mse_config, 0, sizeof(mse_config));
	mse_decoder_info_create(&decoder_info);

	json_t *listener_config = json_loads(_listener_config, 0, &jerr);
	const int opaque_creator_rc = parse_decoder_info(&decoder_info,
		listener_config,&topic_name);

	assert_true(0 == opaque_creator_rc);
	json_decref(listener_config);

	/* Currently, uses global_config */
	decoder_info.mse_config = &mse_config;

	struct mse_database *db = &decoder_info.mse_config->database;

	init_mse_database(db);

	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	assert_true(mse_array);
	const int parse_rc = parse_mse_array(&decoder_info.mse_config->database,
		mse_array);
	assert_true(parse_rc == 0);

	struct mse_array *notifications_array = process_mse_buffer(
	        mse_input1,
	        strlen(mse_input1),
	        "127.0.0.1",
	        &decoder_info,
	        now);

	struct mse_array *notifications_array2 = process_mse_buffer(mse_input2,
	                                                            strlen(mse_input2),
	                                                            "127.0.0.1",
	                                                            &decoder_info,
	                                                            now);


	for (i = 0; notifications_array && i < notifications_array->size; ++i)
		free(notifications_array->data[i].string);
	free(notifications_array);

	for (i = 0; notifications_array2 && i < notifications_array2->size; ++i)
		free(notifications_array2->data[i].string);

	free(notifications_array2);

	mse_decoder_info_destroy(&decoder_info);
	free_valid_mse_database(&mse_config.database);
	free_valid_mse_database(&global_config.mse.database);

	rd_kafka_topic_conf_destroy(global_config.kafka_topic_conf);
	in_addr_list_done(global_config.blacklist);

	json_decref(mse_array);
}

static void checkMSE8_valid_result(struct mse_array *notifications_array) {
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
	                                     "{s:{s:s,s:s,s:i}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "sensor_name", &sensor_name,
	                                     "sensor_id", &sensor_id);

	assert_int_equal(unpack_rc, 0);
	assert_string_equal(subscriptionName, "MSE_SanCarlos");
	assert_string_equal(sensor_name, "MSE_testing");
	assert_int_equal(sensor_id, 255);
	json_decref(ret);
}

static void checkMSE8_valid_empty_deviceid(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert_int_equal(notifications_array->size, 1);
	assert_int_equal(
	    notifications_array->data[0].string_size,
	    strlen(notifications_array->data[0].string)
	);

	const char *subscriptionName = NULL, *sensor_name = NULL;
	const char *deviceId = NULL;
	json_int_t timestamp = 0;
	const char *macAddress = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert_non_null(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:{s:s,s:I,s:s,s:{s:s},s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "timestampMillis", &timestamp,
	                                     "deviceId", &deviceId,
	                                     "location",
	                                     "macAddress", &macAddress,
	                                     "sensor_id", &sensor_id,
	                                     "sensor_name", &sensor_name
	                                     );

	assert_int_equal(unpack_rc, 0);
	assert_string_equal(subscriptionName, "MSE_SanCarlos");
	assert_string_equal(deviceId, "");
	assert_int_equal(timestamp, 1446650950000);
	assert_string_equal(macAddress, "cc:aa:aa:aa:aa:ab");
	assert_string_equal(sensor_name, "MSE_testing");
	json_decref(ret);
}

static void checkMSE8_valid_empty_deviceid_and_mac(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert_int_equal(notifications_array->size, 1);
	assert_int_equal(
	    notifications_array->data[0].string_size,
	    strlen(notifications_array->data[0].string)
	);

	const char *subscriptionName = NULL, *sensor_name = NULL;
	const char *deviceId = NULL;
	json_int_t timestamp = 0;
	const char *macAddress = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert_non_null(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:{s:s,s:I,s:s,s:{s:s},s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "timestampMillis", &timestamp,
	                                     "deviceId", &deviceId,
	                                     "location",
	                                     "macAddress", &macAddress,
	                                     "sensor_id", &sensor_id,
	                                     "sensor_name", &sensor_name
	                                     );

	assert_int_equal(unpack_rc, 0);
	assert_string_equal(subscriptionName, "MSE_SanCarlos");
	assert_string_equal(deviceId, "");
	assert_int_equal(timestamp, 1446650950000);
	assert_string_equal(macAddress, "");
	assert_string_equal(sensor_name, "MSE_testing");
	json_decref(ret);
}

static void checkMSE8_empty_timestamp(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert_int_equal(notifications_array->size, 1);
	assert_int_equal(
	    notifications_array->data[0].string_size,
	    strlen(notifications_array->data[0].string)
	);

	const char *subscriptionName = NULL, *sensor_name = NULL;
	const char *deviceId = NULL;
	json_int_t timestamp = 0;
	const char *macAddress = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert_non_null(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:{s:s,s:I,s:s,s:{s:s},s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "timestampMillis", &timestamp,
	                                     "deviceId", &deviceId,
	                                     "location",
	                                     "macAddress", &macAddress,
	                                     "sensor_id", &sensor_id,
	                                     "sensor_name", &sensor_name
	                                     );

	assert_int_equal(unpack_rc, 0);
	assert_string_equal(subscriptionName, "MSE_SanCarlos");
	assert_string_equal(deviceId, "cc:aa:aa:aa:aa:ab");
	assert_int_equal(timestamp, 0);
	assert_string_equal(macAddress, "cc:aa:aa:aa:aa:ab");
	assert_string_equal(sensor_name, "MSE_testing");
	json_decref(ret);
}

static void checkMSE8_correct(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert_int_equal(notifications_array->size, 1);
	assert_int_equal(
	    notifications_array->data[0].string_size,
	    strlen(notifications_array->data[0].string)
	);

	const char *subscriptionName = NULL, *sensor_name = NULL;
	const char *deviceId = NULL;
	json_int_t timestamp = 0;
	const char *macAddress = NULL;
	json_int_t sensor_id = 0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string, 0, &jerr);
	assert_non_null(ret);
	const int unpack_rc = json_unpack_ex(ret, &jerr, 0,
	                                     "{s:{s:s,s:I,s:s,s:{s:s},s:i,s:s}}",
	                                     "StreamingNotification",
	                                     "subscriptionName", &subscriptionName,
	                                     "timestampMillis", &timestamp,
	                                     "deviceId", &deviceId,
	                                     "location",
	                                     "macAddress", &macAddress,
	                                     "sensor_id", &sensor_id,
	                                     "sensor_name", &sensor_name
	                                     );

	assert_int_equal(unpack_rc, 0);
	assert_string_equal(subscriptionName, "MSE_SanCarlos");
	assert_string_equal(deviceId, "cc:aa:aa:aa:aa:ab");
	assert_int_equal(timestamp, 1446650950000);
	assert_string_equal(macAddress, "cc:aa:aa:aa:aa:ab");
	assert_string_equal(sensor_name, "MSE_testing");
	json_decref(ret);
}
static void checkMSE8_empty_subscription_name(struct mse_array *notifications_array) {
	assert_int_equal(notifications_array->size, 1);
	assert_int_equal(notifications_array->data[0].string_size, 0);
	assert_null(notifications_array->data[0].subscriptionName);
	assert_null(notifications_array->data[0].client_mac);
	assert_null(notifications_array->data[0].string);
	assert_null(notifications_array->data[0].json);
	assert_null(notifications_array->data[0].timestamp);
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
	                                     "{s:{s:I}}",
	                                     "StreamingNotification",
	                                     "timestampMillis", &timestamp);

	printf("%lld\n", timestamp);
	printf("%ld\n", NOW);
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
	                                     "{s:{s:I}}",
	                                     "StreamingNotification",
	                                     "timestampMillis", &timestamp);

	assert_int_equal(unpack_rc, 0);
	assert_true(timestamp / 1000 != NOW);
	assert_true(notifications_array->data[0].timestamp_warnings == 1);

	json_decref(ret);
}

static void checkMSE8_no_valid_result(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert_int_equal(notifications_array->size, 1);
	/* But invalid MSE => No string */
	assert_null(notifications_array->data[0].string);
}

static void testMSE8Decoder_valid_enrich() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8_valid_result);

}

static void testMSE8Decoder_novalid_enrich() {
	testMSE8Decoder(MSE_ARRAY_OUT,
	                LISTENER_NULL_CONFIG,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8_no_valid_result);
}

static void testMSE8Decoder_valid_timestamp() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_PROBING,
	                NOW,
	                checkMSE8_valid_timestamp);
}

static void testMSE8Decoder_invalid_timestamp() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_PROBING_OLD,
	                NOW,
	                checkMSE8_invalid_timestamp);
}

static void testMSE8Decoder_invalid_timestamp_two_message() {
	testMSE10Decoder_two(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_CORRECT_TWO,
	                MSE8_CORRECT_TWO,
	                NOW);
}

static void testMSE8Decoder_invalid_timestamp_two_message2() {
	testMSE10Decoder_two(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_PROBING,
	                MSE8_PROBING,
	                NOW);
}

///* MSE with an empty subscription_name */
static void testMSE8Decoder_empty_subscription_name() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_EMPTY_SUBSCRIPTION_NAME,
	                NOW,
	                checkMSE8_empty_subscription_name);
}

///* Correct MSE */
static void testMSE8Decoder_correct() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_CORRECT,
	                NOW,
	                checkMSE8_correct);
}

static void testMSE8Decoder_empty_deviceId() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_EMPTY_DEVICEID,
	                NOW,
	                checkMSE8_valid_empty_deviceid);
}

static void testMSE8Decoder_empty_deviceId_and_mac() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_EMPTY_DEVICEID_AND_MAC,
	                NOW,
	                checkMSE8_valid_empty_deviceid_and_mac);
}

static void testMSE8Decoder_empty_timestamp() {
	testMSE8Decoder(MSE_ARRAY_IN,
	                LISTENER_NULL_CONFIG,
	                MSE8_EMPTY_TIMESTAMP,
	                NOW,
	                checkMSE8_invalid_timestamp);
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(testMSE8Decoder_valid_enrich),
		cmocka_unit_test(testMSE8Decoder_novalid_enrich),
		cmocka_unit_test(testMSE8Decoder_valid_timestamp),
		cmocka_unit_test(testMSE8Decoder_invalid_timestamp),
		cmocka_unit_test(testMSE8Decoder_invalid_timestamp_two_message),
		cmocka_unit_test(testMSE8Decoder_invalid_timestamp_two_message2),
		cmocka_unit_test(testMSE8Decoder_empty_subscription_name),
		cmocka_unit_test(testMSE8Decoder_correct),
		cmocka_unit_test(testMSE8Decoder_empty_deviceId),
		cmocka_unit_test(testMSE8Decoder_empty_deviceId_and_mac),
		cmocka_unit_test(testMSE8Decoder_empty_timestamp)
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
