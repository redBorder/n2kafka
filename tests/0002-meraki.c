#include "rb_meraki_tests.h"
#include "rb_mem_tests.h"

static const char MERAKI_MSG[] =
  // *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                "\"ipv4\":\"/10.1.3.38\","
	                "\"location\":{"
	                    "\"lat\":37.42205275787813,"
	                    "\"lng\":-122.20766382990405,"
	                    "\"unc\":49.0,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"78:3a:84:11:22:33\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "},"
	            "{"
	                "\"ipv4\":null,"
	                "\"location\":{"
	                    "\"lat\":37.42200897584358,"
	                    "\"lng\":-122.20751219778322,"
	                    "\"unc\":23.641346501668412,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:30Z\","
	                "\"ssid\":null,"
	                "\"os\":null,"
	                "\"clientMac\":\"80:56:f2:44:55:66\","
	                "\"seenEpoch\":1432020630,"
	                "\"rssi\":13,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Hon Hai/Foxconn\""
	            "},"
	            "{"
	                "\"ipv4\":\"/10.1.3.41\","
	                "\"location\":{"
	                    "\"lat\":37.42205737322192,"
	                    "\"lng\":-122.20762896118686,"
	                    "\"unc\":37.49420236988837,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"3c:ab:8e:77:88:99\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";
  // *INDENT-ON*

static const char MERAKI_MSG_NO_OBSERVATION[] =
	// *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	"}";
	// *INDENT-ON*


static const char MERAKI_MSG_OBSERVATION_NO_CLIENT_MAC[] =
	// *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                "\"ipv4\":\"/10.1.3.38\","
	                "\"location\":{"
	                    "\"lat\":37.42205275787813,"
	                    "\"lng\":-122.20766382990405,"
	                    "\"unc\":49.0,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                //"\"clientMac\":\"78:3a:84:11:22:33\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";
  // *INDENT-ON*

static const char MERAKI_MSG_OBSERVATION_LOCATION_STRING[] =
	// *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                "\"ipv4\":\"/10.1.3.38\","
	                "\"location\":{"
	                    "\"lat\":\"37.42205275787813\","
	                    "\"lng\":\"-122.20766382990405\","
	                    "\"unc\":\"49.0\","
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"78:3a:84:11:22:33\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";
  // *INDENT-ON*

static const char MERAKI_MSG_OBSERVATION_LOCATION_LON_ZERO[] =
	// *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                "\"ipv4\":\"/10.1.3.38\","
	                "\"location\":{"
	                    "\"lat\":37.42205275787813,"
	                    "\"lng\":0.0,"
	                    "\"unc\":\"49.0\","
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"78:3a:84:11:22:33\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";
  // *INDENT-ON*
static const char MERAKI_MSG_OBSERVATION_EMPTY_LOCATION[] =
	// *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                "\"ipv4\":\"/10.1.3.38\","
	                "\"location\":{},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"78:3a:84:11:22:33\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";
  // *INDENT-ON*

static const char MERAKI_MSG_OBSERVATION_NO_LOCATION[] =
	// *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                "\"ipv4\":\"/10.1.3.38\","
	                //"\"location\":{},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"78:3a:84:11:22:33\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";

static const char MERAKI_MSG_OBSERVATION_NO_TIMESTAMP[] =
  // *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                "\"ipv4\":\"/10.1.3.38\","
	                "\"location\":{"
	                    "\"lat\":37.42205275787813,"
	                    "\"lng\":-122.20766382990405,"
	                    "\"unc\":49.0,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"78:3a:84:11:22:33\","
	                //"\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";
  // *INDENT-ON*

static const char MERAKI_MSG_OBSERVATION_NO_SRC_IPV4[] =
  // *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                //"\"ipv4\":\"/10.1.3.38\","
	                "\"location\":{"
	                    "\"lat\":37.42205275787813,"
	                    "\"lng\":-122.20766382990405,"
	                    "\"unc\":49.0,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"78:3a:84:11:22:33\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";
  // *INDENT-ON*



static const char MERAKI_MSG_NO_APMAC[] =
  // *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        //"\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                "\"ipv4\":\"/10.1.3.38\","
	                "\"location\":{"
	                    "\"lat\":37.42205275787813,"
	                    "\"lng\":-122.20766382990405,"
	                    "\"unc\":49.0,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"78:3a:84:11:22:33\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "},"
	            "{"
	                "\"ipv4\":null,"
	                "\"location\":{"
	                    "\"lat\":37.42200897584358,"
	                    "\"lng\":-122.20751219778322,"
	                    "\"unc\":23.641346501668412,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:30Z\","
	                "\"ssid\":null,"
	                "\"os\":null,"
	                "\"clientMac\":\"80:56:f2:44:55:66\","
	                "\"seenEpoch\":1432020630,"
	                "\"rssi\":13,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Hon Hai/Foxconn\""
	            "},"
	            "{"
	                "\"ipv4\":\"/10.1.3.41\","
	                "\"location\":{"
	                    "\"lat\":37.42205737322192,"
	                    "\"lng\":-122.20762896118686,"
	                    "\"unc\":37.49420236988837,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"3c:ab:8e:77:88:99\","
	                "\"seenEpoch\":1432020634,"
	                "\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";
  // *INDENT-ON*



static const char MERAKI_MSG_OBSERVATION_NO_RSSI[] =
	// *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	                "\"ipv4\":\"/10.1.3.38\","
	                "\"location\":{"
	                    "\"lat\":37.42205275787813,"
	                    "\"lng\":-122.20766382990405,"
	                    "\"unc\":49.0,"
	                    "\"x\":["
	                    "],"
	                    "\"y\":["
	                    "]"
	                "},"
	                "\"seenTime\":\"2015-05-19T07:30:34Z\","
	                "\"ssid\":\"Trinity\","
	                "\"os\":\"Apple iOS\","
	                "\"clientMac\":\"78:3a:84:11:22:33\","
	                "\"seenEpoch\":1432020634,"
	                //"\"rssi\":0,"
	                "\"ipv6\":null,"
	                "\"manufacturer\":\"Apple\""
	            "}"
	        "]"
	    "}"
	"}";
	// *INDENT-ON*


static const char MERAKI_MSG_OBSERVATION_NULL[] =
  // *INDENT-OFF*
	"{"
	    "\"version\":2,"
	    "\"secret\": 2,"
	    "\"type\":2,"
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":["
	            "{"
	            "}"
	        "]"
	    "}"
	"}";
  // *INDENT-ON*

static const char MERAKI_MSG_OBSERVATION_INVALID[] =
  // *INDENT-OFF*
	"{"
	    "\"version\":2,"
	    "\"secret\": 2,"
	    "\"type\":2,"
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\": null"
	    "}"
	"}";
  // *INDENT-ON*

static const char MERAKI_MSG_EMPTY[] = "{}";
static const char MERAKI_MSG_INVALID[] = "{";

const char MERAKI_EMPTY_OBSERVATIONS_MSG[] =
  // *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":[]"
	    "}"
	"}";
  // *INDENT-ON*


static const char MERAKI_MSG_INVALID_OBSERVATION[] =
	// *INDENT-OFF*
	"{"
	    "\"version\":\"2.0\","
	    "\"secret\":\"r3dB0rder\","
	    "\"type\":\"DevicesSeen\","
	    "\"data\":{"
	        "\"apMac\":\"55:55:55:55:55:55\","
	        "\"apFloors\":[],"
	        "\"apTags\":[],"
	        "\"observations\":\"AAAAAA_TEST\""
	    "}"
	"}";
	// *INDENT-ON*

static const char MERAKI_SECRETS_IN[] = \
  // *INDENT-OFF*
	"{"
		/* "\"meraki-secrets\": {" */
	        "\"r3dB0rder\": { "
	          "\"sensor_name\": \"meraki1\" "
	          ", \"sensor_id\": 2"
	        "},"
	        "\"r3dB0rder2\": { "
	          "\"sensor_name\": \"meraki2\" "
	          ", \"sensor_id\": 3"
	        "}"
	    /* "}" */
	"}";
  // *INDENT-ON*

static const char MERAKI_SECRETS_OBJECT_EMPTY[] = \
  // *INDENT-OFF*
	"{"
		/* "\"meraki-secrets\": {" */
	        "\"r3dB0rder\": { "
//	          "\"sensor_name\": \"meraki1\" "
//	          ", \"sensor_id\": 2"
	        "},"
	        "\"r3dB0rder2\": { "
//	          "\"sensor_name\": \"meraki2\" "
//	          ", \"sensor_id\": 3"
	        "}"
	    /* "}" */
	"}";
  // *INDENT-ON*

static const char MERAKI_SECRETS_IN_INVALID_JSON[] = \
	// *INDENT-OFF*
	"{"
		/* "\"meraki-secrets\": {" */
	        "\"r3dB0rder\": "
	    /* "}" */
	"}";
	// *INDENT-ON*

static const char MERAKI_SECRETS_IN_INVALID[] = \
  // *INDENT-OFF*
	"{"
		/* "\"meraki-secrets\": {" */
	        "\"r3dB0rder\": { "
	          "\"sensor_name\": \"meraki1\" "
	          ", \"sensor_id\": 2"
//	        "},"
	        "}"
	        "\"r3dB0rder2\": { "
	          "\"sensor_name\": \"meraki2\" "
	          ", \"sensor_id\": 3"
	        "}"
	    /* "}" */
	"}";
  // *INDENT-ON*

static const char MERAKI_SECRETS_DEFAULT_IN[] = \
  // *INDENT-OFF*
	"{"
		/* "\"meraki-secrets\": {" */
	        "\"r3dB0rder\": { "
	          "\"sensor_name\": \"meraki1\" "
	          ", \"sensor_id\": 2"
	        "},"
	        "\"*\": { "
	          "\"sensor_name\": \"default\" "
	          ", \"sensor_id\": 3"
	        "}"
	    /* "}" */
	"}";
  // *INDENT-ON*

static const char MERAKI_SECRETS_EMPTY[] = "{}";

static const char MERAKI_SECRETS_OUT[] = \
  // *INDENT-OFF*
	"{"
		/* "\"meraki-secrets\": {" */
	        "\"r3dB0rder3\": { "
	          "\"sensor_name\": \"meraki1\" "
	          ", \"sensor_id\": 2"
	        "},"
	        "\"r3dB0rder2\": { "
	          "\"sensor_name\": \"meraki2\" "
	          ", \"sensor_id\": 3"
	        "}"
	    /* "}" */
	"}";
  // *INDENT-ON*

static const char MERAKI_SECRETS_DEFAULT_OUT[] = \
  // *INDENT-OFF*
	"{"
		/* "\"meraki-secrets\": {" */
	        "\"r3dB0rder3\": { "
	          "\"sensor_name\": \"meraki1\" "
	          ", \"sensor_id\": 2"
	        "},"
	        "\"*\": { "
	          "\"sensor_name\": \"default\" "
	          ", \"sensor_id\": 3"
	        "}"
	    /* "}" */
	"}";
  // *INDENT-ON*

CHECKDATA(check1,
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.38"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "78:3a:84:11:22:33"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "-95"},
	{.key = "client_latlong", .value = "37.42205,-122.20766"},
	{.key = "wireless_id", .value = "Trinity"}
);

CHECKDATA(check2,
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = NULL},
	{.key = "client_os", .value = NULL},
	{.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
	{.key = "client_mac", .value = "80:56:f2:44:55:66"},
	{.key = "timestamp", .value = "1432020630"},
	{.key = "client_rssi_num", .value = "-82"},
	{.key = "client_latlong", .value = "37.42201,-122.20751"},
	{.key = "wireless_id", .value = NULL}
);

CHECKDATA(check3,
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.41"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "-95"},
	{.key = "client_latlong", .value = "37.42206,-122.20763"},
	{.key = "wireless_id", .value = "Trinity"}
);

static void MerakiDecoder_test_meraki_null(const char *config_str, const char *secrets,
		const char *msg, const struct checkdata_array *checkdata) {
	size_t i;
	const char *topic_name = NULL;
	json_error_t jerr;
	struct meraki_config meraki_config;
	struct meraki_decoder_info decoder_info;
	json_t *config = NULL;

	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	meraki_decoder_info_create(&decoder_info);

	if (config_str) {
		config = json_loads(config_str, 0, NULL);
		assert_true(config);
		parse_meraki_decoder_info(&decoder_info, &topic_name, config);
		assert_true(decoder_info.per_listener_enrichment);
	}

	// Workaround
	decoder_info.meraki_config = &meraki_config;

	json_t *meraki_secrets_array = json_loadb(secrets, strlen(secrets), 0,
									&jerr);
	assert_false(meraki_secrets_array);

	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets_array);

	assert_true(parse_rc != 0);
	//json_decref(meraki_secrets_array);

	struct kafka_message_array *notifications_array = process_meraki_buffer(
		msg, strlen(msg), "127.0.0.1", &decoder_info);

	assert_null(notifications_array);

	meraki_decoder_info_destructor(&decoder_info);
	if (config) {
		json_decref(config);
	}
	meraki_database_done(&meraki_config.database);
}

static void MerakiDecoder_test_notificacion_array_null(const char *config_str, const char *secrets,
		const char *msg, const struct checkdata_array *checkdata) {
	size_t i;
	const char *topic_name = NULL;
	json_error_t jerr;
	struct meraki_config meraki_config;
	struct meraki_decoder_info decoder_info;
	json_t *config = NULL;

	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	meraki_decoder_info_create(&decoder_info);

	if (config_str) {
		config = json_loads(config_str, 0, NULL);
		assert_true(config);
		parse_meraki_decoder_info(&decoder_info, &topic_name, config);
		assert_true(decoder_info.per_listener_enrichment);
	}

	// Workaround
	decoder_info.meraki_config = &meraki_config;

	json_t *meraki_secrets_array = json_loadb(secrets, strlen(secrets), 0,
									&jerr);
	assert_true(meraki_secrets_array);

	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets_array);

	assert_true(parse_rc == 0);
	json_decref(meraki_secrets_array);

	struct kafka_message_array *notifications_array = process_meraki_buffer(
		msg, strlen(msg), "127.0.0.1", &decoder_info);

	assert_null(notifications_array);

	meraki_decoder_info_destructor(&decoder_info);
	if (config) {
		json_decref(config);
	}
	meraki_database_done(&meraki_config.database);
}

static void MerakiDecoder_test_notificacion_array_not_null(const char *config_str, const char *secrets,
		const char *msg, const struct checkdata_array *checkdata) {
	size_t i;
	const char *topic_name = NULL;
	json_error_t jerr;
	struct meraki_config meraki_config;
	struct meraki_decoder_info decoder_info;
	json_t *config = NULL;

	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	meraki_decoder_info_create(&decoder_info);

	if (config_str) {
		config = json_loads(config_str, 0, NULL);
		assert_true(config);
		parse_meraki_decoder_info(&decoder_info, &topic_name, config);
		assert_true(decoder_info.per_listener_enrichment);
	}

	// Workaround
	decoder_info.meraki_config = &meraki_config;

	json_t *meraki_secrets_array = json_loadb(secrets, strlen(secrets), 0,
									&jerr);
	assert_true(meraki_secrets_array);

	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets_array);

	assert_true(parse_rc == 0);
	json_decref(meraki_secrets_array);

	struct kafka_message_array *notifications_array = process_meraki_buffer(
		msg, strlen(msg), "127.0.0.1", &decoder_info);

	assert_non_null(notifications_array);

	if (notifications_array->count > 0) {
		if (checkdata) {
			for (i = 0; i < notifications_array->count; ++i)
				free(notifications_array->msgs[i].payload);
			free(notifications_array);
		} else {
			assert_true(0==notifications_array);
		}
	} else {
		free(notifications_array);
	}

	meraki_decoder_info_destructor(&decoder_info);
	if (config) {
		json_decref(config);
	}
	meraki_database_done(&meraki_config.database);
}

static void mem_test(void (*cb)()) {
	size_t i = 1;
	do {
		mem_wrap_fail_in = i++;
		cb();
	} while (0 == mem_wrap_fail_in);
	mem_wrap_fail_in = 0;
}

static void MerakiDecoder_msg_no_apmac() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_NO_APMAC, &checkdata);
}

static void MerakiDecoder_msg_observation_no_client_mac() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_NO_CLIENT_MAC, &checkdata);
}

static void MerakiDecoder_msg_empty_location() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_not_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_EMPTY_LOCATION, &checkdata);
}

static void MerakiDecoder_msg_no_location() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_not_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_NO_LOCATION, &checkdata);
}

static void MerakiDecoder_location_string() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_LOCATION_STRING, &checkdata);
}

static void MerakiDecoder_location_lon_zero() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_not_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_LOCATION_LON_ZERO, &checkdata);
}

static void MerakiDecoder_msg_observation_no_timestamp() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_NO_TIMESTAMP, &checkdata);
}

static void MerakiDecoder_msg_observation_no_rssi() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_not_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_NO_RSSI, &checkdata);
}

static void MerakiDecoder_msg_observation_no_src_ipv4() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_not_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_NO_SRC_IPV4, &checkdata);
}

static void MerakiDecoder_valid_enrich_meraki_msg_empty() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_EMPTY, &checkdata);
}

static void MerakiDecoder_valid_enrich_meraki_msg_invalid() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_INVALID, &checkdata);
}

static void MerakiDecoder_msg_observation_empty() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_NULL, &checkdata);
}

static void MerakiDecoder_msg_observation_invalid() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_base(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_OBSERVATION_INVALID, &checkdata);
}

static void MerakiDecoder_msg_invalid_observation() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_INVALID_OBSERVATION, &checkdata);
}

static void MerakiDecoder_no_enrichment() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	const char MERAKI_CONFIG_ENRICHMENT[] = "{\"topic\":\"topic_test\"}";
	MerakiDecoder_test_base(NULL, MERAKI_SECRETS_OBJECT_EMPTY,
		MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_msg_no_observation() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_notificacion_array_null(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG_NO_OBSERVATION, &checkdata);
}

static void MerakiDecoder_test_config(const char *config_str, const char *secrets,
		const char *msg, const struct checkdata_array *checkdata) {
	const char *topic_name = NULL;
	json_error_t jerr;
	struct meraki_config meraki_config;
	struct meraki_decoder_info decoder_info;
	json_t *config = NULL;

	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	meraki_decoder_info_create(&decoder_info);

	assert(config_str);
	config = json_loads(config_str, 0, NULL);
	assert_true(config);
	int ret_config = parse_meraki_decoder_info(&decoder_info, &topic_name, config);
	assert_null(topic_name);
	assert_null(decoder_info.per_listener_enrichment);
	assert_int_equal(ret_config, -1);

	meraki_decoder_info_destructor(&decoder_info);
	if (config) {
		json_decref(config);
	}
	meraki_database_done(&meraki_config.database);
}

static void MerakiDecoder_valid_enrich_config() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	const char MERAKI_CONFIG_EMPTY[] = "{}";
	MerakiDecoder_test_config(MERAKI_CONFIG_EMPTY, MERAKI_SECRETS_IN,
		MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_valid_enrich_invalid_config() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	const char MERAKI_CONFIG_ENRICHMENT[] = "{\"topic\":\"topic_test\", \"enrichment\":{\"a\":1,\"b\":\"c\"}}";
	MerakiDecoder_test_base(MERAKI_CONFIG_ENRICHMENT, MERAKI_SECRETS_IN,
		MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_empty_secret() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_base(NULL, MERAKI_SECRETS_OBJECT_EMPTY,
		MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_valid_enrich() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_base(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_valid_enrich_invalid_secret() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_base(NULL, MERAKI_SECRETS_IN,
		MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_valid_enrich_invalid_secret_json() {
	CHECKDATA_ARRAY(checkdata, &check1, &check2, &check3);
	MerakiDecoder_test_meraki_null(NULL, MERAKI_SECRETS_IN_INVALID_JSON,
		MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_novalid_enrich() {
	struct checkdata_array *checkdata = NULL;
	MerakiDecoder_test_base(NULL, MERAKI_SECRETS_OUT, MERAKI_MSG,
								checkdata);
}

static void MerakiDecoder_empty_enrich() {
	struct checkdata_array *checkdata = NULL;
	MerakiDecoder_test_base(NULL, MERAKI_SECRETS_EMPTY, MERAKI_MSG,
								checkdata);
}


static void MerakiDecoder_valid_enrich_mem(){
	mem_test(MerakiDecoder_valid_enrich);
}

static void MerakiDecoder_empty_observations() {
	struct checkdata_array *checkdata = NULL;
	MerakiDecoder_test_base(NULL, MERAKI_SECRETS_IN,
		MERAKI_EMPTY_OBSERVATIONS_MSG, checkdata);
}

CHECKDATA(check1_listener_enrich,
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.38"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "78:3a:84:11:22:33"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "-95"},
	{.key = "client_latlong", .value = "37.42205,-122.20766"},
	{.key = "wireless_id", .value = "Trinity"},
	{.key = "a", .value = "1"},
	{.key = "b", .value = "c"}
);

CHECKDATA(check2_listener_enrich,
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = NULL},
	{.key = "client_os", .value = NULL},
	{.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
	{.key = "client_mac", .value = "80:56:f2:44:55:66"},
	{.key = "timestamp", .value = "1432020630"},
	{.key = "client_rssi_num", .value = "-82"},
	{.key = "client_latlong", .value = "37.42201,-122.20751"},
	{.key = "wireless_id", .value = NULL},
	{.key = "a", .value = "1"},
	{.key = "b", .value = "c"}
);

CHECKDATA(check3_listener_enrich,
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.41"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "-95"},
	{.key = "client_latlong", .value = "37.42206,-122.20763"},
	{.key = "wireless_id", .value = "Trinity"},
	{.key = "a", .value = "1"},
	{.key = "b", .value = "c"}
);

static void MerakiDecoder_valid_enrich_per_listener() {
	CHECKDATA_ARRAY(checkdata, &check1_listener_enrich,
		&check2_listener_enrich, &check3_listener_enrich);

	MerakiDecoder_test_base("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}",
		MERAKI_SECRETS_IN, MERAKI_MSG, &checkdata);
}

CHECKDATA(check_default1,
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.38"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "78:3a:84:11:22:33"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "-95"},
	{.key = "client_latlong", .value = "37.42205,-122.20766"},
	{.key = "wireless_id", .value = "Trinity"},
	{.key = "sensor_name", .value = "default"}
);

CHECKDATA(check_default2,
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = NULL},
	{.key = "client_os", .value = NULL},
	{.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
	{.key = "client_mac", .value = "80:56:f2:44:55:66"},
	{.key = "timestamp", .value = "1432020630"},
	{.key = "client_rssi_num", .value = "-82"},
	{.key = "client_latlong", .value = "37.42201,-122.20751"},
	{.key = "wireless_id", .value = NULL},
	{.key = "sensor_name", .value = "default"}
);

CHECKDATA(check_default3,
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.41"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "-95"},
	{.key = "client_latlong", .value = "37.42206,-122.20763"},
	{.key = "wireless_id", .value = "Trinity"},
	{.key = "sensor_name", .value = "default"}
);

static void MerakiDecoder_default_secret_hit() {
	CHECKDATA_ARRAY(checkdata, &check1_listener_enrich,
		&check2_listener_enrich, &check3_listener_enrich);

	MerakiDecoder_test_base("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}",
		MERAKI_SECRETS_DEFAULT_IN, MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_default_secret_miss() {
	CHECKDATA_ARRAY(checkdata,
		&check_default1, &check_default2, &check_default3);

	MerakiDecoder_test_base("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}",
		MERAKI_SECRETS_DEFAULT_OUT, MERAKI_MSG, &checkdata);
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(MerakiDecoder_valid_enrich),
		cmocka_unit_test(MerakiDecoder_valid_enrich_invalid_secret),
		cmocka_unit_test(MerakiDecoder_novalid_enrich),
		cmocka_unit_test(MerakiDecoder_valid_enrich_per_listener),
		cmocka_unit_test(MerakiDecoder_empty_observations),
		cmocka_unit_test(MerakiDecoder_default_secret_hit),
		cmocka_unit_test(MerakiDecoder_default_secret_miss),
		cmocka_unit_test(MerakiDecoder_valid_enrich_mem),
		cmocka_unit_test(MerakiDecoder_valid_enrich_invalid_secret_json),
		cmocka_unit_test(MerakiDecoder_valid_enrich_meraki_msg_empty),
		cmocka_unit_test(MerakiDecoder_valid_enrich_meraki_msg_invalid),
		cmocka_unit_test(MerakiDecoder_msg_observation_empty),
		cmocka_unit_test(MerakiDecoder_msg_observation_invalid),
		cmocka_unit_test(MerakiDecoder_msg_no_observation),
		cmocka_unit_test(MerakiDecoder_msg_invalid_observation),
		cmocka_unit_test(MerakiDecoder_msg_no_apmac),
		cmocka_unit_test(MerakiDecoder_msg_observation_no_client_mac),
		cmocka_unit_test(MerakiDecoder_msg_observation_no_timestamp),
		cmocka_unit_test(MerakiDecoder_msg_observation_no_src_ipv4),
		cmocka_unit_test(MerakiDecoder_msg_observation_no_rssi),
		cmocka_unit_test(MerakiDecoder_valid_enrich_invalid_config),
		cmocka_unit_test(MerakiDecoder_valid_enrich_config),
		cmocka_unit_test(MerakiDecoder_empty_enrich),
		cmocka_unit_test(MerakiDecoder_empty_secret),
		cmocka_unit_test(MerakiDecoder_msg_empty_location),
		cmocka_unit_test(MerakiDecoder_msg_no_location),
		cmocka_unit_test(MerakiDecoder_location_lon_zero),
		cmocka_unit_test(MerakiDecoder_location_string),
		cmocka_unit_test(MerakiDecoder_no_enrichment),

	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
