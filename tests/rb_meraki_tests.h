#include "../src/decoder/meraki/rb_meraki.c"
#include "rb_json_tests.c"

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#define RB_UNUSED __attribute__((unused))

static void MerakiDecoder_test_base(const char *config_str, const char *secrets,
		const char *msg, const struct checkdata_array *checkdata) RB_UNUSED;
static void MerakiDecoder_test_base(const char *config_str, const char *secrets,
		const char *msg, const struct checkdata_array *checkdata) {
	size_t i;
	const char *topic_name = NULL;
	json_error_t jerr;
	struct meraki_config meraki_config;
	struct meraki_decoder_info decoder_info;
	json_t *config = NULL;

	memset(&meraki_config, 0, sizeof(meraki_config));
	init_meraki_database(&meraki_config.database);

	meraki_decoder_info_create(&decoder_info);

	if (config_str) {
		config = json_loads(config_str, 0, NULL);
		assert_true(config);
		parse_meraki_decoder_info(&decoder_info, &topic_name, config);
		assert_true(decoder_info.per_listener_enrichment);
	}

	// Workaround
	decoder_info.meraki_config = &meraki_config;

	json_t *meraki_secrets_array = json_loadb(secrets, strlen(secrets), 0,
									&jerr);
	assert_true(meraki_secrets_array);

	const int parse_rc = parse_meraki_secrets(&meraki_config.database,
	                     meraki_secrets_array);

	assert_true(parse_rc == 0);
	json_decref(meraki_secrets_array);

	char *aux = strdup(msg);
	struct kafka_message_array *notifications_array = process_meraki_buffer(
		aux, strlen(msg), "127.0.0.1", &decoder_info);
	free(aux);

	if (NULL != notifications_array) {
		if (checkdata) {
			rb_assert_json_array(notifications_array->msgs,
			                     notifications_array->count, checkdata);
				for (i = 0; i < notifications_array->count; ++i)
					free(notifications_array->msgs[i].payload);
				free(notifications_array);
			} else {
				assert_true(0==notifications_array);
		}
	}

	meraki_decoder_info_destructor(&decoder_info);
	if (config) {
		json_decref(config);
	}
	meraki_database_done(&meraki_config.database);
}
