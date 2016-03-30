#include "rb_json_tests.c"
#include "rb_http2k_tests.c"

#include <setjmp.h>
#include <cmocka.h>
#include <assert.h>

static const char TEMP_TEMPLATE[] = "n2ktXXXXXX";

static const char CONFIG_TEST[] =
    "{"
        "\"brokers\": \"localhost\","
        "\"rb_http2k_config\": {"
            "\"sensors_uuids\" : {"
                    "\"abc\" : {"
                        "\"enrichment\": {"
                            "\"sensor_uuid\":\"abc\","
                            "\"a\":1,"
                            "\"b\":\"c\","
                            "\"d\":true,"
                            "\"e\":null"
                        "}"
                    "},"
                    "\"def\" : {"
                        "\"enrichment\": {"
                            "\"sensor_uuid\":\"def\","
                            "\"f\":1,"
                            "\"g\":\"w\","
                            "\"h\":false,"
                            "\"i\":null,"
                            "\"j\":2.5"
                        "}"
                    "},"
                    "\"ghi\" : {"
                        "\"enrichment\": {"
                            "\"o\": {"
                                "\"a\":90"
                            "}"
                        "}"
                    "},"
                    "\"jkl\" : {"
                        "\"enrichment\": {"
                            "\"v\":[1,2,3,4,5]"
                        "}"
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

static void validate_test(void (*f)(struct rb_config *rb)) {
	init_global_config();
	char temp_filename[sizeof(TEMP_TEMPLATE)];
	strcpy(temp_filename,TEMP_TEMPLATE);
	int temp_fd = mkstemp(temp_filename);
	assert(temp_fd >= 0);
	write(temp_fd, CONFIG_TEST, strlen(CONFIG_TEST));

	parse_config(temp_filename);
	unlink(temp_filename);

	f(&global_config.rb);

	free_global_config();

	close(temp_fd);
}

static void validate_topic_test0(struct rb_config *rb) {
	size_t i;
	struct {
		const char *topic;
		int expected;
	} validations[] = {
		{.topic = "rb_flow", .expected = 1},
		{.topic = "rb_event", .expected = 1},
		{.topic = "rb_unknown", .expected = 0},
		{.topic = "rb_anotherone", .expected = 0},
	};

	for (i=0; i<sizeof(validations)/sizeof(validations[0]); ++i) {
		const int result = rb_http2k_validate_topic(
			&rb->database,validations[i].topic);
		assert(validations[i].expected == result);
	}
}

static void validate_topic_test() {
	validate_test(validate_topic_test0);
}

static void validate_uuid_test0(struct rb_config *rb) {
	size_t i;
	struct {
		const char *uuid;
		int expected;
	} validations[] = {
		{.uuid = "abc", .expected = 1},
		{.uuid = "def", .expected = 1},
		{.uuid = "ghi", .expected = 1},
		{.uuid = "jkl", .expected = 1},
		{.uuid = "mno", .expected = 0},
	};

	for (i=0; i<sizeof(validations)/sizeof(validations[0]); ++i) {
		const int result = rb_http2k_validate_uuid(
			&rb->database,validations[i].uuid);
		assert(validations[i].expected == result);
	}
}

static void validate_uuid_test() {
	validate_test(validate_uuid_test0);
}

#if 0
int rb_http2k_validate_topic(struct rb_database *db, const char *topic) {
	pthread_rwlock_rdlock(&db->rwlock);
	const int ret = topics_db_topic_exists(db->topics_db,topic);
	pthread_rwlock_unlock(&db->rwlock);
}
#endif


int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(validate_uuid_test),
		cmocka_unit_test(validate_topic_test),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);


	return 0;
}
