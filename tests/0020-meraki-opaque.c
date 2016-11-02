#include "engine/global_config.h"

#include "rb_meraki_tests.h"
#include "rb_mem_tests.h"

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

static void opaque_check_reload() {
	void *o_meraki;
	const char *value_a = NULL;
	json_error_t jerr;
	int ret_creator = 0;
	int ret_reload = 0;
	char err[512];

	init_global_config();

	const char MERAKI_OPAQUE[] = \
            "{\"enrichment\":{\"a\":\"b\"}}";

	global_config.topic = strdup("test");

	if (NULL == global_config.rk) {
		if (!(global_config.rk = rd_kafka_new(RD_KAFKA_CONSUMER, global_config.kafka_conf,
								err, sizeof(err)))) {
			fprintf(stderr,
					"%% Failed to create new consumer: %s\n",
					err);
			exit(1);
			}
	}

	json_t *opaque_config = json_loads(MERAKI_OPAQUE, 0, &jerr);

	ret_creator = meraki_opaque_creator(opaque_config, &o_meraki);

	/* without kafka config */
	assert_int_equal(0, ret_creator);

	assert(opaque_config);
	const int unpack_rc = json_unpack_ex(opaque_config, &jerr, 0,
	                                     "{s:{s:s}}",
	                                     "enrichment",
	                                     "a", &value_a);

	assert(unpack_rc == 0);
	assert(0 == strcmp(value_a, "b"));

	meraki_opaque_cast(opaque_config);

	ret_reload = meraki_opaque_reload(opaque_config, o_meraki);

	if (ret_reload == 0)
		meraki_opaque_destructor(o_meraki);

	free_valid_mse_database(&global_config.mse.database);
	in_addr_list_done(global_config.blacklist);
	rd_kafka_topic_conf_destroy(global_config.kafka_topic_conf);

	json_decref(opaque_config);

	free(global_config.topic);
	rd_kafka_destroy(global_config.rk);
}

static void opaque_config_empty() {
	void *o_meraki;
	const char *value_a = NULL;
	json_error_t jerr;
	int ret_creator = 0;
	int ret_reload = 0;

	const char MERAKI_OPAQUE[] = \
            "{\"enrichmentTEST\":\"1\"}";

    json_t *opaque_config = json_loads(MERAKI_OPAQUE, 0, &jerr);

	ret_creator = meraki_opaque_creator(opaque_config, &o_meraki);

	/* without kafka config */
	assert_int_equal(-1, ret_creator);


	assert(opaque_config);
	const int unpack_rc = json_unpack_ex(opaque_config, &jerr, 0,
	                                     "{s:s}",
	                                     "enrichmentTEST",
	                                     &value_a);

	assert(unpack_rc == 0);
	assert(0 == strcmp(value_a, "1"));

	meraki_opaque_cast(opaque_config);

	json_decref(opaque_config);

	free(global_config.topic);
}

static void opaque_check_no_kafka() {
	void *o_meraki;
	const char *value_a = NULL;
	json_error_t jerr;
	int ret_creator = 0;
	int ret_reload = 0;


	const char MERAKI_OPAQUE[] = \
            "{\"enrichment\":{\"a\":\"b\"}}";

    json_t *opaque_config = json_loads(MERAKI_OPAQUE, 0, &jerr);

	ret_creator = meraki_opaque_creator(opaque_config, &o_meraki);

	/* without kafka config */
	assert_int_equal(-1, ret_creator);


	assert(opaque_config);
	const int unpack_rc = json_unpack_ex(opaque_config, &jerr, 0,
	                                     "{s:{s:s}}",
	                                     "enrichment",
	                                     "a", &value_a);

	assert(unpack_rc == 0);
	assert(0 == strcmp(value_a, "b"));

	meraki_opaque_cast(opaque_config);

	json_decref(opaque_config);

	free(global_config.topic);
}

static void opaque_check_kafka() {
	void *o_meraki;
	const char *value_a = NULL;
	json_error_t jerr;
	int ret_creator = 0;
	int ret_reload = 0;
	char err[512];

	init_global_config();

	const char MERAKI_OPAQUE[] = \
            "{\"enrichment\":{\"a\":\"b\"}}";

	global_config.topic = strdup("test");

	if (NULL == global_config.rk) {
		if (!(global_config.rk = rd_kafka_new(RD_KAFKA_CONSUMER, global_config.kafka_conf,
								err, sizeof(err)))) {
			fprintf(stderr,
					"%% Failed to create new consumer: %s\n",
					err);
			exit(1);
			}
	}

	json_t *opaque_config = json_loads(MERAKI_OPAQUE, 0, &jerr);

	ret_creator = meraki_opaque_creator(opaque_config, &o_meraki);

	/* without kafka config */
	assert_int_equal(0, ret_creator);

	assert(opaque_config);
	const int unpack_rc = json_unpack_ex(opaque_config, &jerr, 0,
	                                     "{s:{s:s}}",
	                                     "enrichment",
	                                     "a", &value_a);

	assert(unpack_rc == 0);
	assert(0 == strcmp(value_a, "b"));

	meraki_opaque_cast(opaque_config);

	if (ret_reload == 0)
		meraki_opaque_destructor(o_meraki);

	json_decref(opaque_config);

	free(global_config.topic);
	rd_kafka_destroy(global_config.rk);
}



static void mem_test(void (*cb)()) {
	size_t i = 1;
	do {
		mem_wrap_fail_in = i++;
		cb();
	} while (0 == mem_wrap_fail_in);
	mem_wrap_fail_in = 0;
}

static void opaque_opaque_conf() {
	void *o_meraki;
	const char *value_a = NULL;
	json_error_t jerr;
	int ret_creator = 0;
	int ret_reload = 0;

	const char MERAKI_OPAQUE_CONF[] = \
            "{\"enrichment\":{\"a\":\"b\"}, \"topic\":\"topic_test\"}";

	json_t *opaque_config = json_loads(MERAKI_OPAQUE_CONF, 0, &jerr);

	ret_creator = meraki_opaque_creator(opaque_config, &o_meraki);

	/* without kafka config */
	assert_int_equal(-1, ret_creator);


	assert(opaque_config);
	const int unpack_rc = json_unpack_ex(opaque_config, &jerr, 0,
	                                     "{s:{s:s}}",
	                                     "enrichment",
	                                     "a", &value_a);

	assert(unpack_rc == 0);
	assert(0 == strcmp(value_a, "b"));

	meraki_opaque_cast(opaque_config);

	json_decref(opaque_config);

	free(global_config.topic);
}

static void test_opaque1(){
	opaque_check_no_kafka();
}

static void test_check_kafka(){
	opaque_check_kafka();
}

static void test_opaque_mem_test(){
	mem_test(opaque_check_no_kafka);
}

static void test_opaque_conf(){
	opaque_opaque_conf();
}

static void test_opaque_config_empty(){
	opaque_config_empty();
}


static void test_opaque_reload(){
	opaque_check_reload();
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_opaque1),
		cmocka_unit_test(test_opaque_mem_test),
		cmocka_unit_test(test_opaque_conf),
		cmocka_unit_test(test_opaque_config_empty),
		cmocka_unit_test(test_opaque_reload),
		cmocka_unit_test(test_check_kafka)
	};

	return cmocka_run_group_tests(tests, NULL, NULL);

	return 0;
}
