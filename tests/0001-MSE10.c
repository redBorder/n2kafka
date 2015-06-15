#include "rb_mse.c"

#include "rb_json_tests.c"

static const char MSE10_ASSOC[] =
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

static const char MSE10_LOCUP[] = 
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

static const char MSE10_MANY[] =
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

static const char MSE10_ZERO_NOTIFICATIONS[] = "{\"notifications\":[]}";

static const char MSE_ARRAY_IN[] = \
	"[\n" \
		"{\n" \
			"\"stream\": \"rb-assoc\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";

static const char MSE_ARRAY_OUT[] = \
	"[\n" \
		"{\n" \
			"\"stream\": \"rb-assoc0\" \n" \
			",\"enrichment\":{\n" \
				"\"sensor_name\": \"testing\"\n" \
				", \"sensor_id\": 255\n" \
			"}\n" \
		"}\n" \
	"]";

static const char MSE_ARRAY_MANY_IN[] = \
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
static void testMSE10Decoder(const char *mse_array_str,const char *mse_input,void (*check_result)(struct mse_array *)){
	char err[BUFSIZ];
	json_error_t jerr;
	size_t i;

	struct mse_config mse_config;
	memset(&mse_config,0,sizeof(mse_config));

	// init_mse_database(&mse_config.database);
	
	struct mse_opaque *opaque = NULL;
	json_t *listener_config = json_object();
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

	check_result(notifications_array);
	
	free(aux);
	for(i=0;notifications_array && i<notifications_array->size;++i)
		free(notifications_array->data[i].string);
	
	free(notifications_array);
	mse_opaque_done((void **)&opaque);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

static void checkMSE10Decoder_valid_enrich(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	assert(notifications_array->data[0].string_size == strlen(notifications_array->data[0].string));

	const char *subscriptionName=NULL,*sensor_name=NULL;
	json_int_t sensor_id=0;
	json_error_t jerr;

	json_t *ret = json_loads(notifications_array->data[0].string,0,&jerr);
	assert(ret);
	const int unpack_rc = json_unpack_ex(ret,&jerr,0,
		"{s:[{s:s,s:s,s:i}]}",
		"notifications",
		"subscriptionName",&subscriptionName,
		"sensor_name",&sensor_name,
		"sensor_id",&sensor_id);

	assert(unpack_rc == 0);
	assert(0==strcmp(subscriptionName,"rb-assoc"));
	assert(0==strcmp(sensor_name,"testing"));
	assert(255 == sensor_id);
	json_decref(ret);
}

static void testMSE10Decoder_valid_enrich() {
	testMSE10Decoder(MSE_ARRAY_IN,MSE10_ASSOC,checkMSE10Decoder_valid_enrich);
}

static void checkMSE10Decoder_novalid_enrich(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert(notifications_array->size == 1);
	/* But invalid MSE => No string */
	assert(notifications_array->data[0].string == NULL);
}

static void testMSE10Decoder_novalid_enrich() {
	testMSE10Decoder(MSE_ARRAY_OUT,MSE10_ASSOC,checkMSE10Decoder_novalid_enrich);
}

static void checkMSE10Decoder_multi(struct mse_array *notifications_array) {
	const char *subscriptionName1=NULL,*sensor_name1=NULL;
	json_int_t sensor_id1=0;
	const char *subscriptionName2=NULL,*sensor_name2=NULL;
	json_int_t sensor_id2=0;
	json_error_t jerr;

	/* No database -> output == input */
	assert(notifications_array->size == 2);

	assert(notifications_array->data[0].string);
	assert(notifications_array->data[0].string_size == strlen(notifications_array->data[0].string));
	json_t *ret1 = json_loads(notifications_array->data[0].string,0,&jerr);
	assert(ret1);
	const int unpack_rc1 = json_unpack_ex(ret1,&jerr,0,
		"{s:[{s:s,s:s,s:i}]}",
		"notifications",
		"subscriptionName",&subscriptionName1,
		"sensor_name",&sensor_name1,
		"sensor_id",&sensor_id1);

	assert(notifications_array->data[1].string);
	assert(notifications_array->data[1].string_size == strlen(notifications_array->data[1].string));
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
	json_decref(ret1);
	json_decref(ret2);
}

/// @TODO use a for loop
static void testMSE10Decoder_valid_enrich_multi() {
	testMSE10Decoder(MSE_ARRAY_MANY_IN,MSE10_MANY,checkMSE10Decoder_multi);
}

static void checkMSE10Decoder_empty_array(struct mse_array *notifications_array) {
	/* No database -> output == input */
	assert(!notifications_array ||  notifications_array->size == 0);
}

static void testMSE10Decoder_empty_array() {
	testMSE10Decoder(MSE_ARRAY_IN,MSE10_ZERO_NOTIFICATIONS,checkMSE10Decoder_empty_array);
}

int main() {
	testMSE10Decoder_valid_enrich();
	testMSE10Decoder_novalid_enrich();
	testMSE10Decoder_valid_enrich_multi();
	testMSE10Decoder_empty_array();
	
	return 0;
}
