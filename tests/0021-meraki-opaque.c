#include "rb_meraki_tests.h"

#include <setjmp.h>
#include <cmocka.h>

#include "rb_mem_tests.h"

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

	json_decref(opaque_config);
}

static void test_opaque_no_kafka(){
	opaque_check_no_kafka();
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_opaque_no_kafka)
	};

	return cmocka_run_group_tests(tests, NULL, NULL);

	return 0;
}
