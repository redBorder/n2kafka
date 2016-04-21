#include "decoder/rb_http2k/rb_http2k_organizations_database.h"
#include "engine/global_config.h"
#include "util/kafka.h"

#include <math.h>
#include <librd/rdfloat.h>
#include <librd/rdlog.h>
#include <setjmp.h>
#include <cmocka.h>
#include <assert.h>

static const char TEMP_TEMPLATE[] = "n2ktXXXXXX";
static const char TEST_N2KAFKA_ID[] = "0013-test";
static const time_t test_timestamp = 1462430283;
static const time_t test_report_interval = 25;
static const time_t test_clean_interval = 300;
static const time_t expected_key_timestamp_report = 175;


/// Callback that modify organization database
typedef void (*organization_database_modify_cb)(organizations_db_t *db,
								void *ctx);

/// Callback that validates a report
typedef void (*validate_report_cb)(struct kafka_message_array *mq,void *ctx);

struct report_validation {
	organization_database_modify_cb org_db_cb;
	void *org_db_cb_ctx;

	validate_report_cb validate_cb;
	void *validate_cb_ctx;

	int clean;
};

static void validate_report0(json_t *config,
		const struct report_validation *validators, size_t vsize) {
	size_t i;
	organizations_db_t db;
	struct itimerspec timer_interval = {
		.it_interval = {
			.tv_sec = test_report_interval,
			.tv_nsec = 0,
		},
		.it_value = {
			.tv_sec = 0,
			.tv_nsec = 0,
		},
	}, clean_interval = {
		.it_interval = {
			.tv_sec = test_clean_interval,
			.tv_nsec = 0,
		},
		.it_value = {
			.tv_sec = 0,
			.tv_nsec = 0,
		},
	};

	assert(config);
	assert(validators);

	organizations_db_init(&db);
	organizations_db_reload(&db, config);
	json_decref(config);

	for (i=0; i<vsize; ++i) {
		if (validators[i].org_db_cb) {
			validators[i].org_db_cb(&db,
						validators[i].org_db_cb_ctx);
		}

		if (validators[i].validate_cb) {
			struct kafka_message_array *ma = validators[i].clean ?
				organization_db_clean_consumed(&db,
					test_timestamp, &timer_interval,
					&clean_interval, TEST_N2KAFKA_ID)
				: organization_db_interval_consumed(&db,
					test_timestamp, &timer_interval,
					&clean_interval, TEST_N2KAFKA_ID);
			validators[i].validate_cb(ma,
						validators[i].validate_cb_ctx);

			free(ma);
		}
	}

	organizations_db_done(&db);
}

/*
 * TEST 1: Check that if we have 0 clients, the report contains nothing
 */

static void validate_zero_clients_reports0(struct kafka_message_array *ma,
								void *ctx) {
	(void)ctx;
	assert(ma);
	assert(0 == ma->count);
}

static void validate_zero_clients_reports() {
	json_t *zero_uuids = json_object();
	static const struct report_validation validator = {
		.validate_cb = validate_zero_clients_reports0,
		.validate_cb_ctx = NULL,
	};

	validate_report0(zero_uuids, &validator, 1);
}

/*
 * TEST 2: If we do a report inmediatly after create database with some IDs,
 * all of them have to have 0 bytes, so we should receive no report.
 */

/** Validate basic fields of the message that always need to be */
static void validate_msg(json_t *json) {
	json_error_t jerr;
	const char *monitor, *type, *unit;

	/* Validate some fields that needs to be with unknown value */
	const int json_validate_rc = json_unpack_ex(json, &jerr,
		JSON_VALIDATE_ONLY, "{s:s,s:I,s:I}","value",
			"client_limit_bytes","client_total_bytes_consumed");

	if (0 != json_validate_rc) {
		rdlog(LOG_ERR, "Couldn't validate JSON: %s", jerr.text);
		assert(0);
	}

	/* Validate fixes fields that needs to be in every message */
	const int unpack_ex = json_unpack_ex(json, &jerr, 0, "{s:s,s:s,s:s}",
		"monitor", &monitor, "type", &type, "unit", &unit);

	if (0 != unpack_ex) {
		rdlog(LOG_ERR, "Couldn't validate JSON: %s", jerr.text);
		assert(0);
	}

	assert(0 == strcmp(monitor, "organization_received_bytes"));
	assert(0 == strcmp(type, "data"));
	assert(0 == strcmp(unit, "bytes"));
}

/** Validate all messages in a queue calling cb for each message
  @param mq Message queue to validate
  @param cb callback to call for each message
  @param ctx Context to pass to callback
  */
static void validate_msgs(struct kafka_message_array *ma,
		void (*key_cb)(const char *key, int keylen, void *cb_ctx),
		void (*cb)(json_t *msg, void *cb_ctx), void *cb_ctx) {
	json_error_t jerr;
	size_t i;

	assert(ma);

	for (i=0; i<ma->count; ++i) {
		json_t *msg = json_loadb(ma->msgs[i].payload, ma->msgs[i].len,
						0, &jerr);
		if (NULL == msg) {
			rdlog(LOG_ERR, "Couldn't load msg %zu: %s", i,
								jerr.text);
			assert(0);
		}

		validate_msg(msg);
		key_cb(ma->msgs[i].key,ma->msgs[i].key_len,cb_ctx);
		cb(msg,cb_ctx);
		json_decref(msg);
		free(ma->msgs[i].payload);
	}
}

static json_t *new_config_uuid(json_t *enrichment, int byte_limit) {
	json_error_t jerr;

	json_t *ret = json_pack_ex(&jerr, 0, "{s:o,s:{s:i}}",
		"enrichment", enrichment, "limits", "bytes", byte_limit);

	if (NULL == ret) {
		rdlog(LOG_ERR, "Error packing: %s", jerr.text);
		assert(0);
	}

	return ret;
}

static void validate_zero_bytes_reports() {
	size_t n_uuids = 10;
	size_t i;
	json_t *uuids = json_object();

	const struct report_validation validator = {
		.validate_cb = validate_zero_clients_reports0,
		.validate_cb_ctx = &n_uuids,
	};

	for (i=0; i<n_uuids; ++i) {
		char uuid[BUFSIZ];
		snprintf(uuid, sizeof(uuid), "%zu", i);
		json_object_set_new(uuids, uuid,
			new_config_uuid(json_object(), 0));
	}

	validate_report0(uuids, &validator, 1);
}

/*
 * TEST 3: Check that limits are reported properly
 */

static json_t *sensor_enrichment(const char *sensor_uuid) {
	json_t *ret = json_object();
	assert(ret);
	json_object_set_new(ret,"sensor_uuid",json_string(sensor_uuid));
	return ret;
}

static void validate_reports_limits() {
	/// Not valid
}

/*
 * TEST 4: Check that consumed bytes are properly writed/readed
 */

static void client_consume_bytes(organizations_db_t *db, void *ctx) {
	char buf[BUFSIZ];
	size_t i;
	const size_t n_uuids = *(size_t *)ctx;

	for (i=0; i<n_uuids; ++i) {
		/* Let's consume bytes for each client */
		snprintf(buf, sizeof(buf), "%zu", i);
		organization_db_entry_t *organization
						= organizations_db_get(db,buf);
		assert(organization);
		organization_add_consumed_bytes(organization,i);
		organizations_db_entry_decref(organization);
	}
}

static void validate_bytes_consumed() {
	/// Not valid
}

/*
 * TEST 5: Check two client's report, and to reset client's consumed bytes
 */

struct test5_ctx {
#define TEST_5_CTX_MAGIC 0x0355CA1C0355CA1CL
	uint64_t magic;
	const char *n2kafka_id;
	size_t expected_msgs;
	int interval_bytes_multiplier;
	int total_bytes_multiplier;
	int limit_bytes_multiplier;
};
#define TEST5_INITIALIZER0(n2k_id,Em,Im,Tm,Lm) { .magic = TEST_5_CTX_MAGIC,    \
	.n2kafka_id = n2k_id,.expected_msgs = (Em),                           \
	.interval_bytes_multiplier = (Im), .total_bytes_multiplier = (Tm),    \
	.limit_bytes_multiplier = (Lm)}

#define TEST5_INITIALIZER(Em,Im,Tm,Lm) \
	TEST5_INITIALIZER0(TEST_N2KAFKA_ID,Em,Im,Tm,Lm)

static void validate_key(const char *key, int keylen, void *vctx) {
	struct test5_ctx *ctx = vctx;
	time_t key_timestamp = 0;
	const char *cursor = key;

	assert(key);
	assert(ctx);
	assert(TEST_5_CTX_MAGIC == ctx->magic);

	/* 1st part should be n2kafka_id */
	assert(0 == strcmp(cursor,ctx->n2kafka_id));

	/* 2nd part should be organization uuid */
	cursor += strlen(cursor) + 1;
	assert (cursor < key + keylen);
	const unsigned long organization_uuid = strtoul(cursor, NULL, 10);
	assert(organization_uuid > 0); /* Organization 0 nevers send bytes */
	assert(organization_uuid <= ctx->expected_msgs); /* Max Organization */

	/* 3st part should be timestamp */
	cursor += strlen(cursor) + 1;
	assert(cursor < key + keylen);
	sscanf(cursor, "%tu", &key_timestamp);
	assert(key_timestamp == expected_key_timestamp_report);
}

/// @TODO use as others tests base.
static void validate_report(json_t *msg, void *vctx) {
	json_error_t jerr;
	const char *sensor_uuid = NULL,*interval_bytes_consumed = NULL,
		   *n2kafka_id = NULL;
	json_int_t total_bytes_consumed,client_limit_bytes;
	struct test5_ctx *ctx = vctx;

	assert(msg);
	assert(ctx);
	assert(TEST_5_CTX_MAGIC == ctx->magic);

	const int rc = json_unpack_ex(msg, &jerr, 0, "{s:s,s:I,s:I,s:s,s:s}",
		"n2kafka_id",&n2kafka_id,
		"client_total_bytes_consumed",&total_bytes_consumed,
		"client_limit_bytes",&client_limit_bytes,
		"value",&interval_bytes_consumed,
		"sensor_uuid", &sensor_uuid);
	if (0 != rc) {
		rdlog(LOG_ERR, "Couldn't unpack msg %s: %s", json_dumps(msg,0),
			jerr.text);
		assert(0);
	}

	const int dsensor_uuid = atoi(sensor_uuid);

	assert(0 == strcmp(n2kafka_id, ctx->n2kafka_id));
	assert(total_bytes_consumed ==
				dsensor_uuid*ctx->total_bytes_multiplier);
	assert(atoi(interval_bytes_consumed) ==
		dsensor_uuid*ctx->interval_bytes_multiplier);
	assert(client_limit_bytes == dsensor_uuid*ctx->limit_bytes_multiplier);
}

static void validate_test5_reports(struct kafka_message_array *ma,
								void *vctx) {
	struct test5_ctx *ctx = vctx;

	assert(ma);
	assert(vctx);
	assert(TEST_5_CTX_MAGIC == ctx->magic);

	/// This will assert that we have received limit
	assert(ctx->expected_msgs == ma->count);
	validate_msgs(ma,validate_key,validate_report,ctx);
}

static void validate_reset_bytes_consumed() {
	size_t n_uuids = 10, i;
	json_t *uuids = json_object();
	/// Expected 9 messages because uuid 0
	struct test5_ctx test5_ctx1 = TEST5_INITIALIZER(9,1,1,10),
	                 test5_ctx2 = TEST5_INITIALIZER(9,1,2,10),
	                 test5_ctx3 = TEST5_INITIALIZER(9,1,3,10),
	                 test5_ctx4 = TEST5_INITIALIZER(9,1,1,10),
	                 test5_ctx5 = TEST5_INITIALIZER(0,0,0,10);

	const struct report_validation validators[] = {
		/* 1st step: Every client consume N bytes. we should check
		that bytes has been consumed and total_consumed_bytes has
		been updated accordly */
		{
			.org_db_cb = client_consume_bytes,
			.org_db_cb_ctx = &n_uuids,

			.validate_cb = validate_test5_reports,
			.validate_cb_ctx = &test5_ctx1,
		},
		/* 2nd step: Every client consume N bytes. We should check
		that bytes has been consumed and total_consumed_bytes has
		been updated accordly */
		{
			.org_db_cb = client_consume_bytes,
			.org_db_cb_ctx = &n_uuids,

			.validate_cb = validate_test5_reports,
			.validate_cb_ctx = &test5_ctx2,
		},
		/* 3rd step: Clean phase. Every client should have 0 bytes
		consumed in this interval, and 2*uuid total_consumed_bytes */
		{
			.org_db_cb = client_consume_bytes,
			.org_db_cb_ctx = &n_uuids,

			.clean = 1,

			.validate_cb = validate_test5_reports,
			.validate_cb_ctx = &test5_ctx3,
		},
		/* 4th step: Every client consume N bytes. We should check
		that bytes has been consumed, and total_consumed_bytes has
		been updated accordly */
		{
			.org_db_cb = client_consume_bytes,
			.org_db_cb_ctx = &n_uuids,

			.clean = 1,

			.validate_cb = validate_test5_reports,
			.validate_cb_ctx = &test5_ctx4,
		},
		/* 5th step: Any client consume, and we call for report. We
		should check that all clients has 0 interval bytes and 0
		total_bytes */
		{
			.org_db_cb = NULL,
			.org_db_cb_ctx = NULL,

			.validate_cb = validate_test5_reports,
			.validate_cb_ctx = &test5_ctx5,
		},
	};

	for (i=0; i<n_uuids; ++i) {
		char uuid[BUFSIZ];
		snprintf(uuid, sizeof(uuid), "%zu", i);
		json_object_set_new(uuids, uuid,
			new_config_uuid(sensor_enrichment(uuid), i*10));
	}

	validate_report0(uuids, validators, RD_ARRAYSIZE(validators));
}

/*
 * main function
 */

int main() {
	/// @TODO Need to have rdkafka inited. Maybe this plugin should have
	/// it owns rdkafka handler.

	const struct CMUnitTest tests[] = {
		cmocka_unit_test(validate_zero_clients_reports),
		cmocka_unit_test(validate_zero_bytes_reports),
		cmocka_unit_test(validate_reports_limits),
		cmocka_unit_test(validate_bytes_consumed),
		cmocka_unit_test(validate_reset_bytes_consumed),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);


	return 0;
}
