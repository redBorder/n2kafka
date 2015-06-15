#include "rb_mse.c"

#include "rb_json_tests.c"

static const char MSE8_PROBING[] =
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

static const char MSE8_GEO_PROBING[] =
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

static const char MSE8_ASSOC[] =
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

static const char MSE_ARRAY_IN[] = \
	"[\n" \
		"{\n" \
			"\"stream\": \"MSE_SanCarlos\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"MSE_testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";

static const char MSE_ARRAY_OUT[] = \
	"[\n" \
		"{\n" \
			"\"stream\": \"MSE_SanCarlos0\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"MSE_testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";

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

/// @TODO keep in sync with testMSE10Decoder
static void testMSE8Decoder(const char *mse_array_str,const char *_listener_config,
	             const char *mse_input,void (*check_result)(struct mse_array *)) {
	char err[BUFSIZ];
	json_error_t jerr;
	size_t i;

	struct mse_config mse_config;
	memset(&mse_config,0,sizeof(mse_config));

	// init_mse_database(&mse_config.database);
	
	struct mse_opaque *opaque = NULL;
	json_t *listener_config = json_loads(_listener_config,0,&jerr);
	const int opaque_creator_rc = mse_opaque_creator(listener_config,(void **)&opaque,err,sizeof(err));
	assert(0==opaque_creator_rc);
	json_decref(listener_config);
	
	json_t *mse_array = json_loads(mse_array_str,0,&jerr);
	assert(mse_array);
	const int parse_rc = parse_mse_array(&opaque->mse_config->database, mse_array,err,sizeof(err));
	assert(parse_rc == 0);

	char *aux = strdup(mse_input);
	struct mse_array *notifications_array = process_mse_buffer(aux,
		strlen(mse_input),opaque);

	assert(notifications_array);
	check_result(notifications_array);
	
	free(aux);
	for(i=0;i<notifications_array->size;++i)
		free(notifications_array->data[i].string);
	
	free(notifications_array);
	mse_opaque_done((void **)&opaque);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

static void checkMSE8Enrichment_no_overlap(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(notifications_array->data[0].string));

	const char *subscriptionName=NULL,*sensor_name=NULL,*a_value=NULL;
	json_int_t sensor_id=0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string,0,&jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret,&jerr,0,
		"{s:{s:s,s:s,s:i,s:s}}",
		"StreamingNotification",
		"subscriptionName",&subscriptionName,
		"sensor_name",&sensor_name,
		"sensor_id",&sensor_id,
		"a",&a_value);

	assert(unpack_rc == 0);
	assert(0==strcmp(subscriptionName,"MSE_SanCarlos"));
	assert(0==strcmp(sensor_name,"MSE_testing"));
	assert(0==strcmp(a_value,"b"));
	assert(255 == sensor_id);
	json_decref(ret);
}

static void testMSE8Enrichment_no_overlap() {
	testMSE8Decoder(MSE_ARRAY_IN,LISTENER_CONFIG_NO_OVERLAP,MSE8_PROBING,checkMSE8Enrichment_no_overlap);
}

static void checkMSE8Enrichment_overlap(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(notifications_array->data[0].string));

	const char *subscriptionName=NULL,*sensor_name=NULL,*a_value=NULL;
	json_int_t sensor_id=0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string,0,&jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret,&jerr,0,
		"{s:{s:s,s:s,s:i,s:s}}",
		"StreamingNotification",
		"subscriptionName",&subscriptionName,
		"sensor_name",&sensor_name,
		"sensor_id",&sensor_id,
		"a",&a_value);

	assert(unpack_rc == 0);
	assert(0==strcmp(subscriptionName,"MSE_SanCarlos"));
	assert(0==strcmp(sensor_name,"sensor_listener"));
	assert(0==strcmp(a_value,"b"));
	assert(255 == sensor_id);
	json_decref(ret);
}

static void testMSE8Enrichment_overlap() {
	testMSE8Decoder(MSE_ARRAY_IN,LISTENER_CONFIG_OVERLAP,MSE8_PROBING,checkMSE8Enrichment_overlap);
}

int main() {
	testMSE8Enrichment_no_overlap();
	testMSE8Enrichment_overlap();
	
	return 0;
}
