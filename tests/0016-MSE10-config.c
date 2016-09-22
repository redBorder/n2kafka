
#include "rb_mse_tests.h"

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

static void testMSE8ConfigInitGlobal() {
	init_global_config();
}



int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(testMSE8ConfigInitGlobal),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
