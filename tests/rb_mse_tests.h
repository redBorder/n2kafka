#pragma once

#include "rb_mse_tests.h"

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

	struct mse_config mse_config;
	memset(&mse_config, 0, sizeof(mse_config));

	// init_mse_database(&mse_config.database);

	struct mse_opaque *opaque = NULL;
	json_t *listener_config = json_loads(_listener_config, 0, &jerr);
	const int opaque_creator_rc = mse_opaque_creator(listener_config,
	                              (void **)&opaque);
	assert(0 == opaque_creator_rc);
	json_decref(listener_config);

	json_t *mse_array = json_loads(mse_array_str, 0, &jerr);
	assert(mse_array);
	const int parse_rc = parse_mse_array(&opaque->mse_config->database, mse_array);
	assert(parse_rc == 0);

	char *aux = strdup(mse_input);
	struct mse_array *notifications_array = process_mse_buffer(
	        aux,
	        strlen(mse_input),
	        "127.0.0.1",
	        opaque,
	        now);

	check_result(notifications_array);

	free(aux);
	for (i = 0; notifications_array && i < notifications_array->size; ++i)
		free(notifications_array->data[i].string);

	free(notifications_array);
	mse_opaque_done(opaque);
	free_valid_mse_database(&mse_config.database);
	json_decref(mse_array);
}

#define testMSE8Decoder testMSE10Decoder // same function