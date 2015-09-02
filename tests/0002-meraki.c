#include <microhttpd.h>

#include "rb_meraki.c"

#include "rb_json_tests.c"

static const char MERAKI_MSG[] =
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


static const char MERAKI_SECRETS_IN[] = \
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

static const char MERAKI_SECRETS_OUT[] = \
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

static const struct checkdata_value checks1[] = {
		{.key = "type", .value ="meraki"},
		{.key = "wireless_station", .value = "55:55:55:55:55:55"},
		{.key = "src", .value ="10.1.3.38"},
		{.key = "client_os", .value = "Apple iOS"},
		{.key = "client_mac_vendor", .value = "Apple"},
		{.key = "client_mac", .value = "78:3a:84:11:22:33"},
		{.key = "timestamp", .value ="1432020634"},
		{.key = "client_rssi_num", .value = "0"},
		{.key = "client_latlong", .value = "37.42205,-122.20766"},
		{.key = "wireless_id", .value = "Trinity"}
	};

static const struct checkdata_value checks2[] = {
		{.key = "type", .value ="meraki"},
		{.key = "wireless_station", .value = "55:55:55:55:55:55"},
		{.key = "src", .value =NULL},
		{.key = "client_os", .value = NULL},
		{.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
		{.key = "client_mac", .value = "80:56:f2:44:55:66"},
		{.key = "timestamp", .value ="1432020630"},
		{.key = "client_rssi_num", .value = "13"},
		{.key = "client_latlong", .value = "37.42201,-122.20751"},
		{.key = "wireless_id", .value = NULL}
	};

static const struct checkdata_value checks3[] = {
		{.key = "type", .value ="meraki"},
		{.key = "wireless_station", .value = "55:55:55:55:55:55"},
		{.key = "src", .value ="10.1.3.41"},
		{.key = "client_os", .value = "Apple iOS"},
		{.key = "client_mac_vendor", .value = "Apple"},
		{.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
		{.key = "timestamp", .value ="1432020634"},
		{.key = "client_rssi_num", .value = "0"},
		{.key = "client_latlong", .value = "37.42206,-122.20763"},
		{.key = "wireless_id", .value = "Trinity"}
	};

static const struct checkdata check1 = {
	.size = sizeof(checks1)/sizeof(checks1[0]), .checks = checks1
};

static const struct checkdata check2 = {
	.size = sizeof(checks2)/sizeof(checks2[0]), .checks = checks2
};

static const struct checkdata check3 = {
	.size = sizeof(checks3)/sizeof(checks3[0]), .checks = checks3
};

static void MerakiDecoder_valid_enrich() {
	size_t i;
	json_error_t jerr;
	
	struct meraki_config meraki_config;
	memset(&meraki_config,0,sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	struct meraki_opaque meraki_opaque;
	memset(&meraki_opaque,0,sizeof(meraki_opaque));
	meraki_opaque.meraki_config = &meraki_config;
	meraki_opaque.per_listener_enrichment = NULL;
	
	json_t *meraki_secrets_array = json_loadb(MERAKI_SECRETS_IN,strlen(MERAKI_SECRETS_IN),0,&jerr);
	assert(meraki_secrets_array);
	const int parse_rc = parse_meraki_secrets(&meraki_config.database, meraki_secrets_array);
	assert(parse_rc == 0);
	json_decref(meraki_secrets_array);

	char *aux = strdup(MERAKI_MSG);
	struct kafka_message_array *notifications_array = process_meraki_buffer(aux,
		strlen(MERAKI_MSG),"127.0.0.1",&meraki_opaque);
	free(aux);

	static const struct checkdata *checkdata_array[] = {
		&check1,&check2,&check3
	};

	static const struct checkdata_array checkdata = {
		.checks = checkdata_array,
		.size = sizeof(checkdata_array)/sizeof(checkdata_array[0]),
	};

	rb_assert_json_array(notifications_array->msgs,
		notifications_array->count,&checkdata);

	for(i=0;i<notifications_array->count;++i)
		free(notifications_array->msgs[i].payload);
	free(notifications_array);	

	meraki_database_done(&meraki_config.database);
}

#if 0
static void testMSE10Decoder_novalid_enrich() {
	json_error_t jerr;
	char err[BUFSIZ];
	struct mse_config mse_config;
	memset(&mse_config,0,sizeof(mse_config));
	// init_mse_database(&mse_config.database);
	
	json_t *mse_array = json_loadb(MSE_ARRAY_OUT,strlen(MSE_ARRAY_OUT),0,&jerr);
	assert(mse_array);
	const int parse_rc = parse_mse_array(&mse_config.database, mse_array,err,sizeof(err));
	assert(parse_rc == 0);

	char *aux = strdup(MSE10_ASSOC);
	struct mse_array *notifications_array = process_mse_buffer(aux,
		strlen(MSE10_ASSOC),&mse_config.database);

	/* No database -> output == input */
	assert(notifications_array->size == 1);
	/* But invalid MSE => No string */
	assert(notifications_array->data[0].string == NULL);

	free(aux);
	free(notifications_array);	
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

/// @TODO use a for loop
static void testMSE10Decoder_valid_enrich_multi() {
	const char *subscriptionName1=NULL,*sensor_name1=NULL;
	json_int_t sensor_id1=0;
	const char *subscriptionName2=NULL,*sensor_name2=NULL;
	json_int_t sensor_id2=0;

	json_error_t jerr;
	char err[BUFSIZ];
	struct mse_config mse_config;
	memset(&mse_config,0,sizeof(mse_config));
	// init_mse_database(&mse_config.database);
	
	json_t *mse_array = json_loadb(MSE_ARRAY_MANY_IN,strlen(MSE_ARRAY_MANY_IN),0,&jerr);
	assert(mse_array);
	const int parse_rc = parse_mse_array(&mse_config.database, mse_array,err,sizeof(err));
	assert(parse_rc == 0);

	char *aux = strdup(MSE10_MANY);
	struct mse_array *notifications_array = process_mse_buffer(aux,
		strlen(MSE10_MANY),&mse_config.database);

	/* No database -> output == input */
	assert(notifications_array->size == 2);

	json_t *ret1 = json_loads(notifications_array->data[0].string,0,&jerr);
	assert(ret1);
	const int unpack_rc1 = json_unpack_ex(ret1,&jerr,0,
		"{s:[{s:s,s:s,s:i}]}",
		"notifications",
		"subscriptionName",&subscriptionName1,
		"sensor_name",&sensor_name1,
		"sensor_id",&sensor_id1);

	json_t *ret2 = json_loads(notifications_array->data[1].string,0,&jerr);
	assert(ret2);
	const int unpack_rc2 = json_unpack_ex(ret2,&jerr,0,
		"{s:[{s:s,s:s,s:i}]}",
		"notifications",
		"subscriptionName",&subscriptionName2,
		"sensor_name",&sensor_name2,
		"sensor_id",&sensor_id2);

	assert(unpack_rc1 == 0);
	assert(unpack_rc2 == 0);
	assert(0==strcmp(subscriptionName1,"rb-assoc"));
	assert(0==strcmp(sensor_name1,"testing"));
	assert(255 == sensor_id1);
	assert(0==strcmp(subscriptionName2,"rb-loc"));
	assert(0==strcmp(sensor_name2,"testing"));
	assert(255 == sensor_id2);
	
	free(aux);
	json_decref(ret1);
	json_decref(ret2);
	free(notifications_array->data[0].string);
	free(notifications_array->data[1].string);
	free(notifications_array);	
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

static void testMSE10Decoder_empty_array() {
	json_error_t jerr;
	char err[BUFSIZ];
	struct mse_config mse_config;
	memset(&mse_config,0,sizeof(mse_config));
	// init_mse_database(&mse_config.database);
	
	json_t *mse_array = json_loadb(MSE_ARRAY_IN,strlen(MSE_ARRAY_IN),0,&jerr);
	assert(mse_array);
	const int parse_rc = parse_mse_array(&mse_config.database, mse_array,err,sizeof(err));
	assert(parse_rc == 0);

	char *aux = strdup(MSE10_ZERO_NOTIFICATIONS);
	struct mse_array *notifications_array = process_mse_buffer(aux,
		strlen(MSE10_ZERO_NOTIFICATIONS),&mse_config.database);

	/* No database -> output == input */
	assert(!notifications_array ||  notifications_array->size == 0);

	free(aux);
	free(notifications_array);	
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

#endif

/*
 *
 */

static const struct checkdata_value checks1_listener_enrich[] = {
		{.key = "type", .value ="meraki"},
		{.key = "wireless_station", .value = "55:55:55:55:55:55"},
		{.key = "src", .value ="10.1.3.38"},
		{.key = "client_os", .value = "Apple iOS"},
		{.key = "client_mac_vendor", .value = "Apple"},
		{.key = "client_mac", .value = "78:3a:84:11:22:33"},
		{.key = "timestamp", .value ="1432020634"},
		{.key = "client_rssi_num", .value = "0"},
		{.key = "client_latlong", .value = "37.42205,-122.20766"},
		{.key = "wireless_id", .value = "Trinity"},
		{.key = "a", .value = "1"},
		{.key = "b", .value = "c"}
	};

static const struct checkdata_value checks2_listener_enrich[] = {
		{.key = "type", .value ="meraki"},
		{.key = "wireless_station", .value = "55:55:55:55:55:55"},
		{.key = "src", .value =NULL},
		{.key = "client_os", .value = NULL},
		{.key = "client_mac_vendor", .value = "Hon Hai/Foxconn"},
		{.key = "client_mac", .value = "80:56:f2:44:55:66"},
		{.key = "timestamp", .value ="1432020630"},
		{.key = "client_rssi_num", .value = "13"},
		{.key = "client_latlong", .value = "37.42201,-122.20751"},
		{.key = "wireless_id", .value = NULL},
		{.key = "a", .value = "1"},
		{.key = "b", .value = "c"}
	};

static const struct checkdata_value checks3_listener_enrich[] = {
		{.key = "type", .value ="meraki"},
		{.key = "wireless_station", .value = "55:55:55:55:55:55"},
		{.key = "src", .value ="10.1.3.41"},
		{.key = "client_os", .value = "Apple iOS"},
		{.key = "client_mac_vendor", .value = "Apple"},
		{.key = "client_mac", .value = "3c:ab:8e:77:88:99"},
		{.key = "timestamp", .value ="1432020634"},
		{.key = "client_rssi_num", .value = "0"},
		{.key = "client_latlong", .value = "37.42206,-122.20763"},
		{.key = "wireless_id", .value = "Trinity"},
		{.key = "a", .value = "1"},
		{.key = "b", .value = "c"}
	};

static const struct checkdata check1_listener_enrich = {
	.size = sizeof(checks1_listener_enrich)/sizeof(checks1_listener_enrich[0]), 
	.checks = checks1_listener_enrich
};

static const struct checkdata check2_listener_enrich = {
	.size = sizeof(checks2_listener_enrich)/sizeof(checks2_listener_enrich[0]), 
	.checks = checks2_listener_enrich
};

static const struct checkdata check3_listener_enrich = {
	.size = sizeof(checks3_listener_enrich)/sizeof(checks3_listener_enrich[0]), 
	.checks = checks3_listener_enrich
};

static void MerakiDecoder_valid_enrich_per_listener() {
	size_t i;
	json_error_t jerr;
	
	struct meraki_config meraki_config;
	memset(&meraki_config,0,sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	struct meraki_opaque *meraki_opaque;
	json_t *config_str = json_loads("{\"enrichment\":{\"a\":1,\"b\":\"c\"}}",0,NULL);
	assert(config_str);
	meraki_opaque_creator(config_str,(void **)&meraki_opaque);
	assert(meraki_opaque->per_listener_enrichment);
	// Workaround
	meraki_opaque->meraki_config = &meraki_config;
	
	json_t *meraki_secrets_array = json_loadb(MERAKI_SECRETS_IN,strlen(MERAKI_SECRETS_IN),0,&jerr);
	assert(meraki_secrets_array);
	const int parse_rc = parse_meraki_secrets(&meraki_config.database, meraki_secrets_array);
	assert(parse_rc == 0);
	json_decref(meraki_secrets_array);

	char *aux = strdup(MERAKI_MSG);
	struct kafka_message_array *notifications_array = process_meraki_buffer(aux,
		strlen(MERAKI_MSG),"127.0.0.1",meraki_opaque);
	free(aux);

	static const struct checkdata *checkdata_array[] = {
		&check1_listener_enrich,&check2_listener_enrich,&check3_listener_enrich
	};

	static const struct checkdata_array checkdata = {
		.checks = checkdata_array,
		.size = sizeof(checkdata_array)/sizeof(checkdata_array[0]),
	};

	rb_assert_json_array(notifications_array->msgs,
		notifications_array->count,&checkdata);

	for(i=0;i<notifications_array->count;++i)
		free(notifications_array->msgs[i].payload);
	free(notifications_array);	

	meraki_opaque_destructor(meraki_opaque);
	json_decref(config_str);
	meraki_database_done(&meraki_config.database);
}

int main() {
	MerakiDecoder_valid_enrich();
	// testMSE10Decoder_novalid_enrich();
	// testMSE10Decoder_valid_enrich_multi();
	// testMSE10Decoder_empty_array();
	MerakiDecoder_valid_enrich_per_listener();
	
	return 0;
}
