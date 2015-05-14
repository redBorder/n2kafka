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

static void testMSE10Decoder_valid_enrich() {
	const char *subscriptionName=NULL,*sensor_name=NULL;
	json_int_t sensor_id=0;

	json_error_t jerr;
	char err[BUFSIZ];
	struct mse_config mse_config;
	memset(&mse_config,0,sizeof(mse_config));
	// init_mse_database(&mse_config.database);
	
	json_t *mse_array = json_loadb(MSE_ARRAY_IN,strlen(MSE_ARRAY_IN),0,&jerr);
	assert(mse_array);
	const int parse_rc = parse_mse_array(&mse_config.database, mse_array,err,sizeof(err));
	assert(parse_rc == 0);

	char *aux = strdup(MSE10_ASSOC);
	struct mse_array *notifications_array = process_mse_buffer(aux,
		strlen(MSE10_ASSOC),&mse_config.database);

	/* No database -> output == input */
	assert(notifications_array->size == 1);

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
	
	free(aux);
	json_decref(ret);
	free(notifications_array->data[0].string);
	free(notifications_array);	
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

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

#if 0
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
#endif

int main() {
	testMSE10Decoder_valid_enrich();
	testMSE10Decoder_novalid_enrich();
	testMSE10Decoder_valid_enrich_multi();
	
	return 0;
}
