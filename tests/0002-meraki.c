#include <microhttpd.h>

#include "../src/decoder/rb_meraki.c"

#include "rb_json_tests.c"

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

static const struct checkdata_value checks1[] = {
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.38"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "78:3a:84:11:22:33"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "0"},
	{.key = "client_latlong", .value = "37.42205,-122.20766"},
	{.key = "wireless_id", .value = "Trinity"}
};

static const struct checkdata_value checks2[] = {
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = NULL},
	{.key = "client_os", .value = NULL},
	{.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
	{.key = "client_mac", .value = "80:56:f2:44:55:66"},
	{.key = "timestamp", .value = "1432020630"},
	{.key = "client_rssi_num", .value = "13"},
	{.key = "client_latlong", .value = "37.42201,-122.20751"},
	{.key = "wireless_id", .value = NULL}
};

static const struct checkdata_value checks3[] = {
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.41"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "0"},
	{.key = "client_latlong", .value = "37.42206,-122.20763"},
	{.key = "wireless_id", .value = "Trinity"}
};

static const struct checkdata check1 = {
	.size = sizeof(checks1) / sizeof(checks1[0]), .checks = checks1
};

static const struct checkdata check2 = {
	.size = sizeof(checks2) / sizeof(checks2[0]), .checks = checks2
};

static const struct checkdata check3 = {
	.size = sizeof(checks3) / sizeof(checks3[0]), .checks = checks3
};

/// @TODO join with others tests functions
static void MerakiDecoder_valid_enrich() {
	size_t i;
	json_error_t jerr;

	struct meraki_config meraki_config;
	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	struct meraki_decoder_info decoder_info;
	memset(&decoder_info, 0, sizeof(decoder_info));
	decoder_info.meraki_config = &meraki_config;
	decoder_info.per_listener_enrichment = NULL;

	json_t *meraki_secrets_array = json_loadb(MERAKI_SECRETS_IN,
	                               strlen(MERAKI_SECRETS_IN), 0, &jerr);
	assert(meraki_secrets_array);
	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets_array);
	assert(parse_rc == 0);
	json_decref(meraki_secrets_array);

	char *aux = strdup(MERAKI_MSG);
	struct kafka_message_array *notifications_array = process_meraki_buffer(aux,
	        strlen(MERAKI_MSG), "127.0.0.1", &decoder_info);
	free(aux);

	static const struct checkdata *checkdata_array[] = {
		&check1, &check2, &check3
	};

	static const struct checkdata_array checkdata = {
		.checks = checkdata_array,
		.size = sizeof(checkdata_array) / sizeof(checkdata_array[0]),
	};

	rb_assert_json_array(notifications_array->msgs,
	                     notifications_array->count, &checkdata);

	for (i = 0; i < notifications_array->count; ++i)
		free(notifications_array->msgs[i].payload);
	free(notifications_array);

	meraki_database_done(&meraki_config.database);
}

static void MerakiDecoder_novalid_enrich() {
	json_error_t jerr;
	struct meraki_config meraki_config;
	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	struct meraki_decoder_info decoder_info;
	memset(&decoder_info, 0, sizeof(decoder_info));
	decoder_info.meraki_config = &meraki_config;
	decoder_info.per_listener_enrichment = NULL;

	json_t *meraki_secrets = json_loadb(MERAKI_SECRETS_OUT, strlen(MERAKI_SECRETS_OUT), 0, &jerr);
	assert(meraki_secrets);
	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets);
	assert(parse_rc == 0);

	char *aux = strdup(MERAKI_MSG);
	struct kafka_message_array *notifications_array = process_meraki_buffer(aux,
	                        strlen(MERAKI_MSG), "127.0.0.1", &decoder_info);
	free(aux);

	assert(0==notifications_array);

	json_decref(meraki_secrets);
	meraki_database_done(&meraki_config.database);
}

static void MerakiDecoder_empty_observations() {
	json_error_t jerr;
	struct meraki_config meraki_config;
	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	struct meraki_decoder_info decoder_info;
	memset(&decoder_info, 0, sizeof(decoder_info));
	decoder_info.meraki_config = &meraki_config;
	decoder_info.per_listener_enrichment = NULL;

	json_t *meraki_secrets = json_loadb(MERAKI_SECRETS_IN, strlen(MERAKI_SECRETS_IN), 0, &jerr);
	assert(meraki_secrets);
	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets);
	assert(parse_rc == 0);

	char *aux = strdup(MERAKI_EMPTY_OBSERVATIONS_MSG);
	struct kafka_message_array *notifications_array = process_meraki_buffer(aux,
	                        strlen(MERAKI_EMPTY_OBSERVATIONS_MSG), "127.0.0.1", &decoder_info);
	free(aux);

	assert(0==notifications_array);

	json_decref(meraki_secrets);
	meraki_database_done(&meraki_config.database);
}

static const struct checkdata_value checks1_listener_enrich[] = {
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.38"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "78:3a:84:11:22:33"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "0"},
	{.key = "client_latlong", .value = "37.42205,-122.20766"},
	{.key = "wireless_id", .value = "Trinity"},
	{.key = "a", .value = "1"},
	{.key = "b", .value = "c"}
};

static const struct checkdata_value checks2_listener_enrich[] = {
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = NULL},
	{.key = "client_os", .value = NULL},
	{.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
	{.key = "client_mac", .value = "80:56:f2:44:55:66"},
	{.key = "timestamp", .value = "1432020630"},
	{.key = "client_rssi_num", .value = "13"},
	{.key = "client_latlong", .value = "37.42201,-122.20751"},
	{.key = "wireless_id", .value = NULL},
	{.key = "a", .value = "1"},
	{.key = "b", .value = "c"}
};

static const struct checkdata_value checks3_listener_enrich[] = {
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.41"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "0"},
	{.key = "client_latlong", .value = "37.42206,-122.20763"},
	{.key = "wireless_id", .value = "Trinity"},
	{.key = "a", .value = "1"},
	{.key = "b", .value = "c"}
};

static const struct checkdata check1_listener_enrich = {
	.size = sizeof(checks1_listener_enrich) / sizeof(checks1_listener_enrich[0]),
	.checks = checks1_listener_enrich
};

static const struct checkdata check2_listener_enrich = {
	.size = sizeof(checks2_listener_enrich) / sizeof(checks2_listener_enrich[0]),
	.checks = checks2_listener_enrich
};

static const struct checkdata check3_listener_enrich = {
	.size = sizeof(checks3_listener_enrich) / sizeof(checks3_listener_enrich[0]),
	.checks = checks3_listener_enrich
};

static void MerakiDecoder_valid_enrich_per_listener() {
	size_t i;
	const char *topic_name = NULL;
	json_error_t jerr;

	struct meraki_config meraki_config;
	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	struct meraki_decoder_info decoder_info;
	memset(&decoder_info,0,sizeof(decoder_info));
	json_t *config_str = json_loads("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}", 0,
	                                NULL);
	assert(config_str);
	parse_meraki_decoder_info(&decoder_info, &topic_name, config_str);
	assert(decoder_info.per_listener_enrichment);
	// Workaround
	decoder_info.meraki_config = &meraki_config;

	json_t *meraki_secrets_array = json_loadb(MERAKI_SECRETS_IN,
	                               strlen(MERAKI_SECRETS_IN), 0, &jerr);
	assert(meraki_secrets_array);
	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets_array);
	assert(parse_rc == 0);
	json_decref(meraki_secrets_array);

	char *aux = strdup(MERAKI_MSG);
	struct kafka_message_array *notifications_array = process_meraki_buffer(aux,
	        strlen(MERAKI_MSG), "127.0.0.1", &decoder_info);
	free(aux);

	static const struct checkdata *checkdata_array[] = {
		&check1_listener_enrich, &check2_listener_enrich, &check3_listener_enrich
	};

	static const struct checkdata_array checkdata = {
		.checks = checkdata_array,
		.size = sizeof(checkdata_array) / sizeof(checkdata_array[0]),
	};

	rb_assert_json_array(notifications_array->msgs,
	                     notifications_array->count, &checkdata);

	for (i = 0; i < notifications_array->count; ++i)
		free(notifications_array->msgs[i].payload);
	free(notifications_array);

	meraki_decoder_info_destructor(&decoder_info);
	json_decref(config_str);
	meraki_database_done(&meraki_config.database);
}

static const struct checkdata_value default1[] = {
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.38"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "78:3a:84:11:22:33"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "0"},
	{.key = "client_latlong", .value = "37.42205,-122.20766"},
	{.key = "wireless_id", .value = "Trinity"},
	{.key = "sensor_name", .value = "default"}
};

static const struct checkdata_value default2[] = {
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = NULL},
	{.key = "client_os", .value = NULL},
	{.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
	{.key = "client_mac", .value = "80:56:f2:44:55:66"},
	{.key = "timestamp", .value = "1432020630"},
	{.key = "client_rssi_num", .value = "13"},
	{.key = "client_latlong", .value = "37.42201,-122.20751"},
	{.key = "wireless_id", .value = NULL},
	{.key = "sensor_name", .value = "default"}
};

static const struct checkdata_value default3[] = {
	{.key = "type", .value = "meraki"},
	{.key = "wireless_station", .value = "55:55:55:55:55:55"},
	{.key = "src", .value = "10.1.3.41"},
	{.key = "client_os", .value = "Apple iOS"},
	{.key = "client_mac_vendor", .value = "Apple"},
	{.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
	{.key = "timestamp", .value = "1432020634"},
	{.key = "client_rssi_num", .value = "0"},
	{.key = "client_latlong", .value = "37.42206,-122.20763"},
	{.key = "wireless_id", .value = "Trinity"},
	{.key = "sensor_name", .value = "default"}
};

static const struct checkdata check_default1 = {
	.size = sizeof(default1) / sizeof(default1[0]), .checks = default1
};

static const struct checkdata check_default2 = {
	.size = sizeof(default2) / sizeof(default2[0]), .checks = default2
};

static const struct checkdata check_default3 = {
	.size = sizeof(default3) / sizeof(default3[0]), .checks = default3
};

static void MerakiDecoder_default_secret_hit() {
	const char *topic_name = NULL;
	size_t i;
	json_error_t jerr;

	struct meraki_config meraki_config;
	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	struct meraki_decoder_info decoder_info;
	memset(&decoder_info, 0, sizeof(decoder_info));
	json_t *config_str = json_loads("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}", 0,
	                                NULL);
	assert(config_str);
	parse_meraki_decoder_info(&decoder_info,&topic_name,config_str);
	assert(decoder_info.per_listener_enrichment);
	// Workaround
	decoder_info.meraki_config = &meraki_config;

	json_t *meraki_secrets_array = json_loadb(MERAKI_SECRETS_DEFAULT_IN,
	                               strlen(MERAKI_SECRETS_DEFAULT_IN), 0, &jerr);
	assert(meraki_secrets_array);
	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets_array);
	assert(parse_rc == 0);
	json_decref(meraki_secrets_array);

	char *aux = strdup(MERAKI_MSG);
	struct kafka_message_array *notifications_array = process_meraki_buffer(aux,
	        strlen(MERAKI_MSG), "127.0.0.1", &decoder_info);
	free(aux);

	static const struct checkdata *checkdata_array[] = {
		&check1_listener_enrich, &check2_listener_enrich, &check3_listener_enrich
	};

	static const struct checkdata_array checkdata = {
		.checks = checkdata_array,
		.size = sizeof(checkdata_array) / sizeof(checkdata_array[0]),
	};

	rb_assert_json_array(notifications_array->msgs,
	                     notifications_array->count, &checkdata);

	for (i = 0; i < notifications_array->count; ++i)
		free(notifications_array->msgs[i].payload);
	free(notifications_array);

	meraki_decoder_info_destructor(&decoder_info);
	json_decref(config_str);
	meraki_database_done(&meraki_config.database);
}

static void MerakiDecoder_default_secret_miss() {
	const char *topic = NULL;
	size_t i;
	json_error_t jerr;

	struct meraki_config meraki_config;
	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	struct meraki_decoder_info decoder_info;
	memset(&decoder_info, 0, sizeof(decoder_info));
	json_t *config_str = json_loads("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}", 0,
	                                NULL);
	assert(config_str);
	parse_meraki_decoder_info(&decoder_info,&topic,config_str);
	assert(decoder_info.per_listener_enrichment);
	// Workaround
	decoder_info.meraki_config = &meraki_config;

	json_t *meraki_secrets_array = json_loadb(MERAKI_SECRETS_DEFAULT_OUT,
	                               strlen(MERAKI_SECRETS_DEFAULT_OUT), 0, &jerr);
	assert(meraki_secrets_array);
	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets_array);
	assert(parse_rc == 0);
	json_decref(meraki_secrets_array);

	char *aux = strdup(MERAKI_MSG);
	struct kafka_message_array *notifications_array = process_meraki_buffer(aux,
	        strlen(MERAKI_MSG), "127.0.0.1", &decoder_info);
	free(aux);

	static const struct checkdata *checkdata_array[] = {
		&check_default1, &check_default2, &check_default3
	};

	static const struct checkdata_array checkdata = {
		.checks = checkdata_array,
		.size = sizeof(checkdata_array) / sizeof(checkdata_array[0]),
	};

	rb_assert_json_array(notifications_array->msgs,
	                     notifications_array->count, &checkdata);

	for (i = 0; i < notifications_array->count; ++i)
		free(notifications_array->msgs[i].payload);
	free(notifications_array);

	meraki_decoder_info_destructor(&decoder_info);
	json_decref(config_str);
	meraki_database_done(&meraki_config.database);
}

int main() {
	MerakiDecoder_valid_enrich();
	MerakiDecoder_novalid_enrich();
	MerakiDecoder_valid_enrich_per_listener();
	MerakiDecoder_empty_observations();

	MerakiDecoder_default_secret_hit();
	MerakiDecoder_default_secret_miss();

	return 0;
}
