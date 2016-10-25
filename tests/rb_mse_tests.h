#pragma once

#include "../src/decoder/mse/rb_mse.c"

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

/// @TODO keep in sync with testMSE10Decoder

static void testMSE10Decoder(const char *mse_array_str,
                             const char *_listener_config,
                             const char *mse_input,
                             const time_t now,
                             void (*check_result)(struct mse_array *))
__attribute__((unused));
static void testMSE10Decoder(const char *mse_array_str,
                             const char *_listener_config,
                             const char *mse_input,
                             const time_t now,
                             void (*check_result)(struct mse_array *)) {
	json_error_t jerr;
	size_t i;
	const char *topic_name;
	struct mse_config mse_config;
	struct mse_decoder_info decoder_info;

	memset(&mse_config, 0, sizeof(mse_config));
	mse_decoder_info_create(&decoder_info);

	json_t *listener_config = json_loads(_listener_config, 0, &jerr);
	const int opaque_creator_rc = parse_decoder_info(&decoder_info,
		listener_config,&topic_name);

	assert_true(0 == opaque_creator_rc);
	json_decref(listener_config);

	/* Currently, uses global_config */
	decoder_info.mse_config = &mse_config;

	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	assert_true(mse_array);
	const int parse_rc = parse_mse_array(&decoder_info.mse_config->database,
		mse_array);
	assert_true(parse_rc == 0);

	struct mse_array *notifications_array = process_mse_buffer(
	        mse_input,
	        strlen(mse_input),
	        "127.0.0.1",
	        &decoder_info,
	        now);

	check_result(notifications_array);

	for (i = 0; notifications_array && i < notifications_array->size; ++i)
		free(notifications_array->data[i].string);

	free(notifications_array);
	mse_decoder_info_destroy(&decoder_info);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

#define testMSE8Decoder testMSE10Decoder // same function
