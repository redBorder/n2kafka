#include "rb_json_tests.c"
#include "rb_http2k_tests.c"
#include "engine/global_config.h"

#include <math.h>
#include <librd/rdfloat.h>
#include <setjmp.h>
#include <cmocka.h>
#include <assert.h>

static const char TEMP_TEMPLATE[] = "n2ktXXXXXX";

#define CONFIG_TEST_BASIC(ABC_ENRICHMENT, ABC_LIMITS, DEF_ENRICHMENT, \
							DEF_LIMITS) \
    "{" \
        "\"brokers\": \"localhost\"," \
        "\"rb_http2k_config\": {" \
            "\"sensors_uuids\" : {" \
                "\"abc\" : {" \
                    "\"organization_uuid\":\"abc_org\"" \
                "}," \
                "\"def\" : {" \
                    "\"organization_uuid\":\"def_org\"" \
                "}" \
            "}," \
            "\"organizations_uuids\":{" \
                "\"abc_org\":{" \
                    "\"enrichment\": {" \
                        ABC_ENRICHMENT \
                    "}" \
                    ABC_LIMITS \
                "}," \
                "\"def_org\":{" \
                    "\"enrichment\": {" \
                        DEF_ENRICHMENT \
                    "}" \
                    DEF_LIMITS \
                "}" \
            "}," \
            "\"topics\" : {" \
                    "\"rb_flow\": {" \
                            "\"partition_key\":\"client_mac\"," \
                            "\"partition_algo\":\"mac\"" \
                    "}," \
                    "\"rb_event\": {" \
                    "}" \
            "}" \
        "}" \
    "}"

/// Basic config with no def organization, no enrichment and no limits
static const char CONFIG_TEST_BASIC_NO_DEF[] = "{"
        "\"brokers\": \"localhost\","
        "\"rb_http2k_config\": {"
            "\"sensors_uuids\" : {"
                "\"abc\" : {"
                    "\"organization_uuid\":\"abc_org\""
                "},"
                "\"def\" : {"
                    "\"organization_uuid\":\"def_org\""
                "}"
            "},"
            "\"organizations_uuids\":{"
                "\"abc_org\":{}"
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


#define ABC_BASIC_ENRICHMENT "\"sensor_uuid\":\"abc\"," \
                            "\"a\":1," \
                            "\"b\":\"c\"," \
                            "\"d\":true," \
                            "\"e\":null"

#define DEF_BASIC_ENRICHMENT "\"sensor_uuid\":\"def\"," \
                            "\"f\":1," \
                            "\"g\":\"w\"," \
                            "\"h\":false," \
                            "\"i\":null," \
                            "\"j\":2.5"

#define CONFIG_LIMIT(limit) ",\"limits\": {\"bytes\":"#limit"}"

static const struct timespec zero_timespec_var = {
	.tv_sec = 0,
	.tv_nsec = 0,
};

static void validate_abc_basic_enrichment(const json_t *enrichment,
								void *ctx) {
	json_error_t jerr;
	const char *sensor_uuid;
	int a;
	const char *b;
	int d;

	(void)ctx;
	json_t *my_enrichment = json_deep_copy(enrichment);
	const int rc = json_unpack_ex(my_enrichment, &jerr, JSON_STRICT,
		"{s:s,s:i,s:s,s:b,s:n}",
		"sensor_uuid",&sensor_uuid,
		"a",&a,"b",&b,"d",&d,"e");

	if(0 != rc) {
		assert(!jerr.text);
	}

	assert(0==strcmp("abc",sensor_uuid));
	assert(1==a);
	assert(0==strcmp(b,"c"));
	assert(1==d);

	json_decref(my_enrichment);
}

static void validate_def_basic_enrichment(const json_t *enrichment,
								void *ctx) {
	json_error_t jerr;
	const char *sensor_uuid;
	int f;
	const char *g;
	int h;
	double j;

	(void)ctx;
	json_t *my_enrichment = json_deep_copy(enrichment);

	const int rc = json_unpack_ex(my_enrichment, &jerr, JSON_STRICT,
		"{s:s,s:i,s:s,s:b,s:n,s:f}",
		"sensor_uuid",&sensor_uuid,
		"f",&f,"g",&g,"h",&h,"i","j",&j);

	if(0 != rc) {
		assert(!jerr.text);
	}

	assert(0==strcmp("def",sensor_uuid));
	assert(1==f);
	assert(0==strcmp(g,"w"));
	assert(0==h);
	assert(rd_deq(j,2.5));

	json_decref(my_enrichment);
}

/** Every test that will be run. Steps are apply cfg -> validation callback */
struct test {
	/// Reload/apply this configuration, if any
	const char *config;
	/// Simulate activity, like HTTP sessions creation
	void (*activity_cb)(struct rb_config *cfg, void *ctx);
	/// Context to send to activity cb
	void *activity_cb_ctx;
	/// Callback to verify configuration
	void (*validation_cb)(struct rb_config *cfg, void *ctx);
	/// Context to send to validation callback
	void *validation_cb_ctx;
};

/// Convenience macro
#define TEST_INITIALIZER(cfg,mactivity_cb,mactivity_cb_ctx,\
					mvalidation_cb,mvalidation_cb_ctx) \
	{.config=cfg, \
	.activity_cb=mactivity_cb,.activity_cb_ctx=mactivity_cb_ctx, \
	.validation_cb=mvalidation_cb,.validation_cb_ctx=mvalidation_cb_ctx}

static void validate_organization(struct rb_config *cfg,
		const char *organization_uuid, size_t expected_max_bytes,
		void (*check_enrichment)(const json_t *,void *),
		void *check_enrichment_opaque) {
	organizations_db_t *organizations_db = &cfg->database.organizations_db;
	organization_db_entry_t *org = organizations_db_get(
				organizations_db,organization_uuid);
	const uint64_t org_max_bytes = organization_get_max_bytes(org);
	json_t *org_enrichment = organization_get_enrichment(org);

	assert(expected_max_bytes == org_max_bytes);
	if (check_enrichment) {
		check_enrichment(org_enrichment,check_enrichment_opaque);
	}

	organizations_db_entry_decref(org);
}

static void validation_reload_pre(struct rb_config *cfg, void *ctx) {
	(void)ctx;

	validate_organization(cfg,"abc_org",0,validate_abc_basic_enrichment,
									NULL);
	validate_organization(cfg,"def_org",0,validate_def_basic_enrichment,
									NULL);

}

static void validation_reload_post(struct rb_config *cfg, void *ctx) {
	(void)ctx;

	validate_organization(cfg,"abc_org",100,validate_def_basic_enrichment,
									NULL);
	validate_organization(cfg,"def_org",  0,validate_abc_basic_enrichment,
									NULL);
}

static void validate_single_test(const struct test *test, int first) {
	const char *cfg = test->config;
	if (cfg) {
		if (first) {
			/* First config, need to load it all */
			char temp_filename[sizeof(TEMP_TEMPLATE)];
			strcpy(temp_filename,TEMP_TEMPLATE);
			int temp_fd = mkstemp(temp_filename);
			assert(temp_fd >= 0);
			/* Need to parse all config */
			write(temp_fd, cfg, strlen(cfg));
			close(temp_fd);

			parse_config(temp_filename);
			unlink(temp_filename);
		} else {
			json_error_t jerr;
			json_t *jcfg = json_loads(cfg, 0, &jerr);
			assert(jcfg);
			json_t *rb_http2k_cfg = json_object_get(jcfg,
						"rb_http2k_config");
			/* Just reload */
			rb_decoder_reload(&global_config.rb, rb_http2k_cfg);
			json_decref(jcfg);
		}
	}

	if (test->activity_cb) {
		test->activity_cb(&global_config.rb,test->activity_cb_ctx);
	}

	if (NULL != test->validation_cb) {
		test->validation_cb(&global_config.rb,test->validation_cb_ctx);
	}
}

#define VALIDATE_TEST_F__INIT 0x01
#define VALIDATE_TEST_F__DONE 0x02

static void validate_test(const struct test tests[], size_t tests_size) {
	size_t i;
	init_global_config();

	for (i=0; i<tests_size; ++i) {
		validate_single_test(&tests[i], 0 == i);
	}

	free_global_config();
}

/** Some reloads tests */
static void validate_reloads() {
	// First basic configuration, and final configuration too
	static const char *no_limit_config =
		CONFIG_TEST_BASIC(ABC_BASIC_ENRICHMENT, "",
			DEF_BASIC_ENRICHMENT, "");

	// Second configuration, swapping enrichment and setting a limit
	// to client abc
	static const char *abc_limit_swap_enrichment_limit_config =
		CONFIG_TEST_BASIC(DEF_BASIC_ENRICHMENT, CONFIG_LIMIT(100),
			ABC_BASIC_ENRICHMENT, "");

	struct test validations[] = {
		TEST_INITIALIZER(no_limit_config,
			validation_reload_pre, NULL, NULL, NULL),
		TEST_INITIALIZER(abc_limit_swap_enrichment_limit_config,
			validation_reload_post, NULL, NULL, NULL),
		// another reload to previous configuration
		TEST_INITIALIZER(no_limit_config, validation_reload_pre, NULL,
								NULL, NULL),
	};

	validate_test(validations, RD_ARRAY_SIZE(validations));
}

/*
 * TEST 2: Delete an organization from config
 */

static void validation_reload_delete_pre(struct rb_config *cfg, void *ctx) {
	(void)ctx;

	validate_organization(cfg,"abc_org",100,validate_abc_basic_enrichment,
									NULL);
	validate_organization(cfg,"def_org",200,validate_def_basic_enrichment,
									NULL);

}

static void assert_org_not_exist(struct rb_config *cfg, const char *uuid) {
	const int uuid_exists = organizations_db_exists(
		&cfg->database.organizations_db, uuid);
	assert(!uuid_exists);
}

static void validation_reload_delete_post(struct rb_config *cfg, void *ctx) {
	(void)ctx;
	void *reload_enrichment = NULL, *reload_enrichment_ctx = NULL;

	validate_organization(cfg,"abc_org",0,reload_enrichment,
							reload_enrichment_ctx);
	assert_org_not_exist(cfg,"def_org");
}

/** Delete organizations */
static void validate_delete_organization() {
	// First basic configuration, with two organizations
	static const char *two_organizations_config =
		CONFIG_TEST_BASIC(ABC_BASIC_ENRICHMENT, CONFIG_LIMIT(100),
			DEF_BASIC_ENRICHMENT, CONFIG_LIMIT(200));

	// Second configuration, with no def client
	static const char *one_organizations_config = CONFIG_TEST_BASIC_NO_DEF;

	struct test validations[] = {
		TEST_INITIALIZER(two_organizations_config,
			validation_reload_delete_pre, NULL, NULL, NULL),
		TEST_INITIALIZER(one_organizations_config,
			validation_reload_delete_post, NULL, NULL, NULL),
	};

	validate_test(validations, RD_ARRAY_SIZE(validations));
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(validate_reloads),
		cmocka_unit_test(validate_delete_organization),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);


	return 0;
}
