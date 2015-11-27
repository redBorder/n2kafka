#include "rb_mse.c"

#include "rb_json_tests.c"
#include "rb_mse_tests.h"

#include "../src/listener/http.c"

static const char TEMP_TEMPLATE[] = "n2ktXXXXXX";

static const char CONFIG_TEST[] =
	"{"
		"\"brokers\": \"localhost\","
        "\"rb_http2k_config\": {"
            "\"uuids\" : {"
                    "\"abc\" : {"
                            "\"a\":1,"
                            "\"b\":\"c\","
                            "\"d\":true,"
                            "\"e\":null"
                    "},"
                    "\"def\" : {"
                            "\"f\":1,"
                            "\"g\":\"w\","
                            "\"h\":false,"
                            "\"i\":null"
                    "}"
            "},"
            "\"topics\" : {"
                    "\"rb_flow\": {"
                            "\"partition_key\":\"client_mac\","
                            "\"partition_algo\":\"mac\""
                    "},"
                    "\"rb_event\": {"
                    "}"
            "}"
        "}"
	"}";

static const char *VALID_URL = "/rbdata/abc/rb_flow";

static void test_validate_uri() {
	json_error_t jerr;
	json_t *config = json_loads(CONFIG_TEST, 0, &jerr);
	if(NULL == config) {
		rdlog(LOG_CRIT,"Couldn't unpack JSON config: %s",jerr.text);
		assert(0);
	}

	const json_t *decoder_config = NULL;
	const int unpack_rc = json_unpack_ex(config, &jerr, 0, "{s:o}", 
		"rb_http2k_config",&decoder_config);
	if(0 != unpack_rc) {
		rdlog(LOG_CRIT,"Can't unpack config: %s",jerr.text);
		assert(0);
	}

	struct rb_database rb_db;
	init_rb_database(&rb_db);
	parse_rb_config(&rb_db,decoder_config);
	
	int allok = 1;
	char *topic=NULL,*uuid=NULL;
	int validation_rc = rb_http2k_validation(NULL /* @TODO this should change */,VALID_URL,
							&rb_db, &allok,&topic,&uuid,"test_ip");

	assert(MHD_YES == validation_rc);
	assert(0==strcmp(topic,"rb_flow"));
	assert(0==strcmp(uuid,"abc"));

	free(topic);
	free(uuid);

	free_valid_rb_database(&rb_db);
	json_decref(config);
}

static void test_rb_decoder() {

}

int main() {
	/// @TODO Need to have rdkafka inited. Maybe this plugin should have it owns rdkafka handler.
	init_global_config();
	char temp_filename[sizeof(TEMP_TEMPLATE)];
	strcpy(temp_filename,TEMP_TEMPLATE);
	int temp_fd = mkstemp(temp_filename);
	assert(temp_fd >= 0);
	write(temp_fd, CONFIG_TEST, strlen(CONFIG_TEST));

	parse_config(temp_filename);
	unlink(temp_filename);
	test_validate_uri();

	free_global_config();

	close(temp_fd);
	
	return 0;
}
