#include "decoder/rb_http2k/rb_http2k_organizations_database.h"
#include "decoder/rb_http2k/rb_http2k_sync_thread.c"
#include "engine/global_config.h"
#include "util/kafka.h"

#include <math.h>
#include <librdkafka/rdkafka.h>
#include <librd/rdfloat.h>
#include <librd/rdlog.h>
#include <setjmp.h>
#include <cmocka.h>
#include <assert.h>

static const char TEMP_TEMPLATE[] = "n2ktXXXXXX";
#define TEST_N2KAFKA_ID "0013-test"
#define TEST_OTHER_N2KAFKA_ID "other-n2kafka"
static const time_t test_timestamp = 1462430283;
static const time_t test_report_interval = 25;
static const time_t test_clean_interval = 300;
static const time_t expected_key_timestamp_report = 175;


/// Callback that modify organization database
typedef void (*organization_database_modify_cb)(organizations_db_t *db,
								void *ctx);

/// Callback that validates organization database
typedef void (*sync_receive_msg)(organizations_db_t *db,
	struct kafka_message_array *msgs);

/// Callback that validates a report
typedef void (*validate_report_cb)(struct kafka_message_array *mq,void *ctx);

struct report_validation {
	organization_database_modify_cb org_db_cb;
	void *org_db_cb_ctx;

	struct kafka_message_array *sync_msgs;
	struct msg_consume_ctx *consume_ctx;

	organization_database_modify_cb org_db_cb_bef_report;
	void *org_db_cb_bef_report_ctx;

	validate_report_cb validate_cb;
	void *validate_cb_ctx;

	organization_database_modify_cb org_db_cb_aft_report;
	void *org_db_cb_aft_report_ctx;

	int clean;
};

static void consume_sync_msgs(organizations_db_t *db,
		struct msg_consume_ctx *consume_ctx, const char *n2kafka_id,
					const struct kafka_message_array *mq) {
	/* Need to overwrite this */
	consume_ctx->thread->org_db = db;
	consume_ctx->n2kafka_id = strdup(n2kafka_id);

	size_t i = 0;
	for (i = 0; i < mq->count; ++i) {
		sync_thread_msg_consume0(&mq->msgs[i],consume_ctx);
	}

	free(consume_ctx->n2kafka_id);
	consume_ctx->n2kafka_id = NULL;
}

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

		if (validators[i].sync_msgs) {
			consume_sync_msgs(&db, validators[i].consume_ctx,
				TEST_N2KAFKA_ID, validators[i].sync_msgs);
		}

		if (validators[i].org_db_cb_bef_report) {
			validators[i].org_db_cb_bef_report(&db,
					validators[i].org_db_cb_bef_report_ctx);
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

		if (validators[i].org_db_cb_aft_report) {
			validators[i].org_db_cb_aft_report(&db,
					validators[i].org_db_cb_aft_report_ctx);
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
		} else {
			validate_msg(msg);
			key_cb(ma->msgs[i].key,ma->msgs[i].key_len,cb_ctx);
			cb(msg,cb_ctx);
			json_decref(msg);
			free(ma->msgs[i].payload);
		}
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

static void client_consume_bytes0(size_t i,
			organization_db_entry_t *organization,void *ctx) {
	(void)ctx;
	organization_add_consumed_bytes(organization, i);
}

/** Do something foreach client, where client is a numeric string from 0 to
  n_uuids */
static void foreach_numeric_client(organizations_db_t *db, size_t n_uuids,
	void (*cb)(size_t i, organization_db_entry_t *organization,void *ctx),
								void *cb_ctx) {

	char buf[BUFSIZ];
	size_t i;

	for (i=0; i<n_uuids; ++i) {
		/* Let's consume bytes for each client */
		snprintf(buf, sizeof(buf), "%zu", i);
		organization_db_entry_t *organization
						= organizations_db_get(db,buf);
		assert(organization);
		cb(i, organization, cb_ctx);
		organizations_db_entry_decref(organization);
	}
}

static void client_consume_bytes(organizations_db_t *db, void *ctx) {
	const size_t n_uuids = *(size_t *)ctx;

	foreach_numeric_client(db, n_uuids, client_consume_bytes0, NULL);
}

/*
 * TEST 4: Check two client's report, and to reset client's consumed bytes
 */

struct test4_ctx {
#define TEST_4_CTX_MAGIC 0x0355CA1C0355CA1CL
	uint64_t magic;
	const char *n2kafka_id;
	size_t expected_msgs;
	int interval_bytes_multiplier;
	int total_bytes_multiplier;
	int limit_bytes_multiplier;
};
#define TEST4_INITIALIZER0(n2k_id,Em,Im,Tm,Lm) { .magic = TEST_4_CTX_MAGIC,    \
	.n2kafka_id = n2k_id,.expected_msgs = (Em),                           \
	.interval_bytes_multiplier = (Im), .total_bytes_multiplier = (Tm),    \
	.limit_bytes_multiplier = (Lm)}

#define TEST4_INITIALIZER(Em,Im,Tm,Lm) \
	TEST4_INITIALIZER0(TEST_N2KAFKA_ID,Em,Im,Tm,Lm)

static void validate_key(const char *key, int keylen, void *vctx) {
	struct test4_ctx *ctx = vctx;
	time_t key_timestamp = 0;
	const char *cursor = key;

	assert(key);
	assert(ctx);
	assert(TEST_4_CTX_MAGIC == ctx->magic);

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
	struct test4_ctx *ctx = vctx;

	assert(msg);
	assert(ctx);
	assert(TEST_4_CTX_MAGIC == ctx->magic);

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

static void validate_test4_reports(struct kafka_message_array *ma,
								void *vctx) {
	struct test4_ctx *ctx = vctx;

	assert(ma);
	assert(vctx);
	assert(TEST_4_CTX_MAGIC == ctx->magic);

	/// This will assert that we have received limit
	assert(ctx->expected_msgs == ma->count);
	validate_msgs(ma,validate_key,validate_report,ctx);
}

static void validate_reset_bytes_consumed() {
	size_t n_uuids = 10, i;
	json_t *uuids = json_object();
	/// Expected 9 messages because uuid 0
	struct test4_ctx test4_ctx1 = TEST4_INITIALIZER(9,1,1,10),
	                 test4_ctx2 = TEST4_INITIALIZER(9,1,2,10),
	                 test4_ctx3 = TEST4_INITIALIZER(9,1,3,10),
	                 test4_ctx4 = TEST4_INITIALIZER(9,1,1,10),
	                 test4_ctx5 = TEST4_INITIALIZER(0,0,0,10);

	const struct report_validation validators[] = {
		/* 1st step: Every client consume N bytes. we should check
		that bytes has been consumed and total_consumed_bytes has
		been updated accordly */
		{
			.org_db_cb = client_consume_bytes,
			.org_db_cb_ctx = &n_uuids,

			.validate_cb = validate_test4_reports,
			.validate_cb_ctx = &test4_ctx1,
		},
		/* 2nd step: Every client consume N bytes. We should check
		that bytes has been consumed and total_consumed_bytes has
		been updated accordly */
		{
			.org_db_cb = client_consume_bytes,
			.org_db_cb_ctx = &n_uuids,

			.validate_cb = validate_test4_reports,
			.validate_cb_ctx = &test4_ctx2,
		},
		/* 3rd step: Clean phase. Every client should have 0 bytes
		consumed in this interval, and 2*uuid total_consumed_bytes */
		{
			.org_db_cb = client_consume_bytes,
			.org_db_cb_ctx = &n_uuids,

			.clean = 1,

			.validate_cb = validate_test4_reports,
			.validate_cb_ctx = &test4_ctx3,
		},
		/* 4th step: Every client consume N bytes. We should check
		that bytes has been consumed, and total_consumed_bytes has
		been updated accordly */
		{
			.org_db_cb = client_consume_bytes,
			.org_db_cb_ctx = &n_uuids,

			.clean = 1,

			.validate_cb = validate_test4_reports,
			.validate_cb_ctx = &test4_ctx4,
		},
		/* 5th step: Any client consume, and we call for report. We
		should check that all clients has 0 interval bytes and 0
		total_bytes */
		{
			.org_db_cb = NULL,
			.org_db_cb_ctx = NULL,

			.validate_cb = validate_test4_reports,
			.validate_cb_ctx = &test4_ctx5,
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
 *  TEST5: Consume reports test
 */

struct client_consume_sync_bytes_template {
	json_int_t timestamp,client_total_bytes_consumed_multiplier,
		client_limit_bytes_multiplier, value_multiplier;
	const char *monitor, *n2kafka_id, *type, *unit;
	const char *key;
	int partition;
	size_t key_len;
};

/** Creates a kafka array that updates a number of uuids with a given
multiplier */
static struct kafka_message_array *gen_client_consume_sync_bytes(
		const size_t n_uuids,
		const struct client_consume_sync_bytes_template *template) {
	size_t i;

	struct kafka_message_array *ret = new_kafka_message_array(n_uuids);
	assert(ret);

	for (i=0; i<n_uuids; ++i) {
		char uuid_buf[BUFSIZ];
		const uint64_t value = i*(uint64_t)template->value_multiplier;
		char value_buf[BUFSIZ];

		/* Let's consume bytes for each client */
		snprintf(uuid_buf, sizeof(uuid_buf), "%zu", i);
		snprintf(value_buf, sizeof(value_buf), "%"PRIu64, value);

		json_t *jmsg = json_pack("{"
			"s:I," /* "timestamp" */
			"s:I," /* "client_total_bytes_consumed" */
			"s:I," /* "client_limit_bytes" */
			"s:s," /* "monitor":"organization_received_bytes" */
			"s:s," /* "value":0 */
			"s:s," /* organization_uuid":"def_org" */
			"s:s," /* n2kafka_id":"n2kafka_test" */
			"s:s," /* type":"data" */
			"s:s," /* unit":"bytes" */
			"}",
			"timestamp",template->timestamp,
			"client_total_bytes_consumed",
			    template->client_total_bytes_consumed_multiplier,
			"client_limit_bytes",
				template->client_limit_bytes_multiplier,
			"monitor",template->monitor,
			"value",value_buf,
			"organization_uuid",uuid_buf,
			"n2kafka_id",template->n2kafka_id,
			"type",template->type,
			"unit",template->unit);

		char *msg = json_dumps(jmsg, 0);

		char *dup_key = strdup(template->key);
		save_kafka_msg_key_partition_in_array(ret, dup_key,
			template->key_len, msg, strlen(msg),
			template->partition, NULL);

		json_decref(jmsg);
	}

	return ret;
}

static void free_sync_msgs(struct kafka_message_array *sync_msgs) {
	size_t i;
	for (i=0; i<sync_msgs->count; ++i) {
		free(sync_msgs->msgs[i].key);
		free(sync_msgs->msgs[i].payload);
	}
}

/** Validate to only receive other bytes. Our reports always should send 0
consumed bytes, but actual client bytes should be reports received bytes */
static void validate_consume_reports() {
	size_t n_uuids = 10, i;
	json_t *uuids = json_object();
	sync_thread_t consume_thread_mock = {
#ifdef SYNC_THREAD_MAGIC
		.magic = SYNC_THREAD_MAGIC,
#endif
		.creation_rc = 0,
		.run = 1,
		.rk = NULL,
		.rk_conf = NULL,
		.thread = pthread_self(),
		.org_db = NULL, // It will be overriden
	};

	struct msg_consume_ctx consume_ctx = {
#ifdef MSG_CONSUME_CTX_MAGIC
		.magic = MSG_CONSUME_CTX_MAGIC,
#endif
		.thread = &consume_thread_mock,
		.in_sync = {
			.mutex= PTHREAD_MUTEX_INITIALIZER,
			.in_sync = (int []){0},
		},
		.n2kafka_id = NULL, // It will be overriden
	};

#define TEST5_CONSUME_BYTES_KEY(mn2kafka_id,mtimestamp,minterval)              \
	mn2kafka_id "\0" RD_STRINGIFY(mtimestamp) "\0" RD_STRINGIFY(minterval)
#define TEST5_CONSUME_BYTES_INIT0(mtimestamp,mn2kafka_id,                      \
		total_bytes_consumed_multiplier,limit_bytes_multiplier,        \
		mvalue_multiplier,mpartition)                                  \
	{								       \
		.timestamp = mtimestamp,				       \
		.partition = mpartition,                                       \
		.client_total_bytes_consumed_multiplier			       \
			= total_bytes_consumed_multiplier,		       \
		.client_limit_bytes_multiplier = limit_bytes_multiplier,       \
		.value_multiplier = mvalue_multiplier,			       \
		.monitor = "organization_received_bytes",		       \
		.n2kafka_id = mn2kafka_id,				       \
		.type = "data",						       \
		.unit = "bytes",					       \
		.key  = TEST5_CONSUME_BYTES_KEY(mn2kafka_id,mtimestamp,200),   \
		.key_len = sizeof(TEST5_CONSUME_BYTES_KEY(mn2kafka_id,         \
			mtimestamp,200))                                       \
	}
#define TEST5_CONSUME_BYTES_INITP(mn2kafka_id, total_bytes_consumed_multiplier,\
				limit_bytes_multiplier, mvalue_multiplier,     \
				mpartition)                                    \
	TEST5_CONSUME_BYTES_INIT0(1462282696, mn2kafka_id,                     \
		total_bytes_consumed_multiplier, limit_bytes_multiplier,       \
		mvalue_multiplier,mpartition)
#define TEST5_CONSUME_BYTES_INIT(mn2kafka_id, total_bytes_consumed_multiplier, \
				limit_bytes_multiplier, mvalue_multiplier)     \
	TEST5_CONSUME_BYTES_INITP(mn2kafka_id, total_bytes_consumed_multiplier,\
				limit_bytes_multiplier, mvalue_multiplier, 0)

	static const struct client_consume_sync_bytes_template sync_bytes =
		TEST5_CONSUME_BYTES_INIT(TEST_OTHER_N2KAFKA_ID, 1, 10, 1);
	static const struct client_consume_sync_bytes_template my_sync_bytes =
		TEST5_CONSUME_BYTES_INIT(TEST_N2KAFKA_ID, 1, 10, 1);
	struct test4_ctx test5_nomsgs = TEST4_INITIALIZER(0,0,0,0),
	                 test5_ctx2 = TEST4_INITIALIZER(9,1,2,10);

	struct kafka_message_array *sync_msgs = gen_client_consume_sync_bytes(
		n_uuids, &sync_bytes);
	struct kafka_message_array *my_sync_msgs =
		gen_client_consume_sync_bytes(n_uuids, &my_sync_bytes);

	const struct report_validation validators[] = {
		/* 1st step: Every client consume N bytes on others n2kafka.
		we should check that no report are given, since we don't have
		consumed anything */
		{
			.sync_msgs = sync_msgs,
			.consume_ctx = &consume_ctx,

			.validate_cb = validate_test4_reports,
			.validate_cb_ctx = &test5_nomsgs,
		},
		/* 2nd step: Every client consume N bytes in this n2kafka.
		We should check that bytes has been consumed and
		interval_bytes and total_consumed_bytes has been updated
		accordly */
		{
			.org_db_cb = client_consume_bytes,
			.org_db_cb_ctx = &n_uuids,

			.validate_cb = validate_test4_reports,
			.validate_cb_ctx = &test5_ctx2,
		},
		/* 3rd step: Clean phase. We only receive our first consumed
		bytes, that we should ignore, so we have received nothing.
		*/
		{
			.sync_msgs = my_sync_msgs,
			.consume_ctx = &consume_ctx,

			.clean = 1,

			.validate_cb = validate_test4_reports,
			.validate_cb_ctx = &test5_nomsgs,
		},
	};

	for (i=0; i<n_uuids; ++i) {
		char uuid[BUFSIZ];
		snprintf(uuid, sizeof(uuid), "%zu", i);
		json_object_set_new(uuids, uuid,
			new_config_uuid(sensor_enrichment(uuid), i*10));
	}

	validate_report0(uuids, validators, RD_ARRAYSIZE(validators));

	free_sync_msgs(sync_msgs); free(sync_msgs);
	free_sync_msgs(my_sync_msgs); free(my_sync_msgs);
}

/*
 * TEST 6: Testing time slices
 */

static void validate_time_slices() {
	size_t i;
	struct {
		time_t ts1,ts2,slice_width,slice_off;
		int64_t expected_slice_ts1,expected_slice_ts2;
		int cmp_time_slice;
	} tests[] = {
		{
			.ts1=0, .ts2=0, .slice_width=30, .slice_off=0,
			.expected_slice_ts1=0, .expected_slice_ts2=0,
			.cmp_time_slice = 0,
		},{
			.ts1=15, .ts2=45, .slice_width=60, .slice_off=0,
			.expected_slice_ts1=1, .expected_slice_ts2=1,
			.cmp_time_slice = 0,
		},{
			.ts1=15, .ts2=45, .slice_width=60, .slice_off=30,
			.expected_slice_ts1=0, .expected_slice_ts2=1,
			.cmp_time_slice = 1,
		}
	};

	for (i=0; i<RD_ARRAYSIZE(tests); ++i) {
		const int64_t time_slice_1 = timestamp_interval_slice(
				tests[i].ts1, tests[i].slice_width,
				tests[i].slice_off);
		const int64_t time_slice_2 = timestamp_interval_slice(
				tests[i].ts2, tests[i].slice_width,
				tests[i].slice_off);
		assert(time_slice_1 == tests[i].expected_slice_ts1);
		assert(time_slice_2 == tests[i].expected_slice_ts2);
		const int cmp = !!timestamp_interval_slice_cmp(
			tests[i].ts1, tests[i].ts2,tests[i].slice_width,
			tests[i].slice_off);
		assert(tests[i].cmp_time_slice == cmp);
	}
}

/*
 * TEST 7: Consume malformed messages
 */

static void *memdup(const void *orig, size_t siz) {
	void *ret = calloc(1,siz);
	assert(ret);
	memcpy(ret, orig, siz);
	return ret;
}

static void validate_invalid_reports() {
	static const int test_partition = 0;
	static const char test_payload[] = "test_payload";
	int msg_offset = 0;
	json_t *uuids = json_object();
	sync_thread_t consume_thread_mock = {
#ifdef SYNC_THREAD_MAGIC
		.magic = SYNC_THREAD_MAGIC,
#endif
		.creation_rc = 0,
		.run = 1,
		.rk = NULL,
		.rk_conf = NULL,
		.thread = pthread_self(),
		.org_db = NULL, // It will be overriden
	};

	struct msg_consume_ctx consume_ctx = {
#ifdef MSG_CONSUME_CTX_MAGIC
		.magic = MSG_CONSUME_CTX_MAGIC,
#endif
		.thread = &consume_thread_mock,
		.in_sync = {
			.mutex= PTHREAD_MUTEX_INITIALIZER,
			.in_sync = (int []){0},
		},
		.n2kafka_id = NULL, // It will be overriden
	};

#define TEST6_KAFKA_MSG_INITIALIZER0(m_err, m_partition, m_payload, m_len,     \
							m_key, m_key_len)      \
	{ .err = m_err, .rkt = NULL, .partition = m_partition,                 \
		.payload = m_payload ? memdup(m_payload, m_len) : NULL,        \
		.len = m_len,                                                  \
		.key = m_key ? memdup(m_key, m_key_len) : NULL,                \
		.key_len = m_key_len,                                          \
		.offset = msg_offset++, ._private = NULL}

#define TEST6_KAFKA_MSG_INITIALIZER(m_err, m_partition, m_payload, m_key) \
	TEST6_KAFKA_MSG_INITIALIZER0(m_err, m_partition, m_payload,       \
		sizeof(m_payload)-1, m_key, sizeof(m_key)-1)

	rd_kafka_message_t msgs[] = {
		/* NULL key */
		TEST6_KAFKA_MSG_INITIALIZER0(RD_KAFKA_RESP_ERR_NO_ERROR,
						test_partition, test_payload,
						sizeof(test_payload), NULL, 0),

		/* key only have 2 fields */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
				test_partition, test_payload, "hello\0world"),

		/* key have invalid timestamp: letters on it */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition, test_payload, "hello\0world\00010t"),

		/* key have invalid timestamp: only letters */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
						test_partition, test_payload,
						"hello\0world\0timestamp!"),

		/* Invalid message: Object instead of string in monitor value */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition,"{\"monitor\":{},\"value\":100,"
			"\"organization_uuid\":\"abc_org\","
			"\"n2kafka_id\":\""TEST_N2KAFKA_ID"\","
			"\"timestamp\":1462282696}",
			TEST_N2KAFKA_ID"\000abc_org\000200"),

		/* Invalid message: Invalid monitor */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition,
			"{\"monitor\":\"organization_received_bytes2\","
			"\"value\":100,"
			"\"organization_uuid\":\"abc_org\","
			"\"n2kafka_id\":\""TEST_N2KAFKA_ID"\","
			"\"timestamp\":1462282696}",
			TEST_N2KAFKA_ID"\000abc_org\000200"),

		/* Invalid message: no monitor in json */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition,"{\"value\":100,"
			"\"organization_uuid\":\"abc_org\","
			"\"n2kafka_id\":\""TEST_N2KAFKA_ID"\","
			"\"timestamp\":1462282696}",
			TEST_N2KAFKA_ID"\000abc_org\000200"),

		/* Invalid message: Object instead of string in
						organization_uuid value */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition,
			"{\"monitor\":\"organization_received_bytes\","
				"\"value\":100,,"
				"\"organization_uuid\":{},"
				"\"n2kafka_id\":\""TEST_N2KAFKA_ID"\","
				"\"timestamp\":1462282696}",
			TEST_N2KAFKA_ID"\000abc_org\000200"),

		/* Invalid message: no organization_uuid in json */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition,
			"{\"monitor\":\"organization_received_bytes\","
				"\"value\":100,"
				"\"n2kafka_id\":\""TEST_N2KAFKA_ID"\","
				"\"timestamp\":1462282696}",
			TEST_N2KAFKA_ID"\000abc_org\000200"),

		/* Invalid message: Object instead of string in
							n2kafka_id value */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition,
			"{\"monitor\":\"organization_received_bytes\","
				"\"value\":100,"
				"\"organization_uuid\":\"abc_org\","
				"\"n2kafka_id\":{},"
				"\"timestamp\":1462282696}",
			TEST_N2KAFKA_ID"\000abc_org\000200"),

		/* Invalid message: no n2kafka_id value */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition,
			"{\"monitor\":\"organization_received_bytes\","
				"\"value\":100,"
				"\"organization_uuid\":\"abc_org\","
				"\"timestamp\":1462282696}",
			TEST_N2KAFKA_ID"\000abc_org\000200"),

		/* Invalid message: Object instead of integer in timestamp
									value */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition,
			"{\"monitor\":\"organization_received_bytes\","
				"\"value\":100,"
				"\"organization_uuid\":\"abc_org\","
				"\"n2kafka_id\":\""TEST_N2KAFKA_ID"\","
				"\"timestamp\":{}}",
			TEST_N2KAFKA_ID"\000abc_org\000200"),

		/* Invalid message: no timestamp value */
		TEST6_KAFKA_MSG_INITIALIZER(RD_KAFKA_RESP_ERR_NO_ERROR,
			test_partition,
			"{\"monitor\":\"organization_received_bytes\","
				"\"value\":100,"
				"\"organization_uuid\":\"abc_org\","
				"\"n2kafka_id\":\""TEST_N2KAFKA_ID"\"}",
			TEST_N2KAFKA_ID"\000abc_org\000200"),
	};

	struct kafka_message_array sync_msgs = {
		.count = RD_ARRAYSIZE(msgs),
		.size = RD_ARRAYSIZE(msgs),
		.msgs = msgs,
	};

	const struct report_validation validators[] = {
		/* 1st step: Every client consume N bytes on others n2kafka.
		we should check that no report are given, since we don't have
		consumed anything */
		{
			.sync_msgs = &sync_msgs,
			.consume_ctx = &consume_ctx,
		},
	};

	validate_report0(uuids, validators, RD_ARRAYSIZE(validators));

	free_sync_msgs(&sync_msgs);
}

/*
 * TEST8: Validate sync
 */

struct check_consumed_cb_ctx {
#define CHECK_CONSUMED_CB_MAGIC 0x03053CBC03053CBCL
	uint64_t magic;
	size_t n_uuids;
	int consumed_bytes_multiplier;
	int reported_bytes_multiplier;
};

#define CHECK_CONSUMED_CB_CTX_INIT(mn_uuids, mconsumed, mreported)             \
	{.magic=CHECK_CONSUMED_CB_MAGIC,.n_uuids=mn_uuids,                     \
	.consumed_bytes_multiplier=mconsumed,                                  \
	.reported_bytes_multiplier=mreported}

static void client_check_consumed(size_t i,
			organization_db_entry_t *organization, void *ctx) {
	struct check_consumed_cb_ctx *consumed_ctx = ctx;
	assert_true(CHECK_CONSUMED_CB_MAGIC == consumed_ctx->magic);
	const uint64_t expected_consumed_bytes =
			i*(uint64_t)consumed_ctx->consumed_bytes_multiplier;
	const uint64_t expected_reported_bytes =
			i*(uint64_t)consumed_ctx->reported_bytes_multiplier;

	assert_true(expected_consumed_bytes ==
				organization_consumed_bytes(organization));
	assert_true(expected_reported_bytes ==
		organization->bytes_limit.reported);
}



static struct kafka_message_array *prepare_test8_msgs(size_t n_uuids) {
	size_t i;
#define total_bytes_consumed_multiplier 1
#define limit_bytes_multiplier 10000000

	static const struct client_consume_sync_bytes_template template[] = {

		TEST5_CONSUME_BYTES_INITP(TEST_N2KAFKA_ID,
			total_bytes_consumed_multiplier, limit_bytes_multiplier,
			0x1,0),
		TEST5_CONSUME_BYTES_INITP(TEST_OTHER_N2KAFKA_ID,
			total_bytes_consumed_multiplier, limit_bytes_multiplier,
			0x100,0),
		TEST5_CONSUME_BYTES_INITP(TEST_N2KAFKA_ID,
			total_bytes_consumed_multiplier, limit_bytes_multiplier,
			0x10000,1),
		TEST5_CONSUME_BYTES_INITP(TEST_OTHER_N2KAFKA_ID,
			total_bytes_consumed_multiplier, limit_bytes_multiplier,
			0x1000000,1),
	};
	const size_t ret_size = n_uuids * RD_ARRAYSIZE(template);
	struct kafka_message_array *ret = new_kafka_message_array(ret_size);

	assert(ret);

#undef total_bytes_consumed_multiplier
#undef limit_bytes_multiplier

	for (i=0; i<RD_ARRAYSIZE(template); ++i) {
		struct kafka_message_array *tmp = gen_client_consume_sync_bytes(
							n_uuids, &template[i]);
		assert(ret);
		assert(ret->count + tmp->count <= ret->size);

		memcpy(&ret->msgs[ret->count], tmp->msgs,
					sizeof(tmp->msgs[0])*tmp->count);
		ret->count += tmp->count;
		/* Do not free allocated messages */
		memset(tmp->msgs, 0, sizeof(tmp->msgs[0])*tmp->count);
		free_sync_msgs(tmp); free(tmp);
	}

	return ret;
}

static void check_consumed_cb(organizations_db_t *db, void *ctx) {
	struct check_consumed_cb_ctx *consumed_ctx = ctx;

	assert_true(CHECK_CONSUMED_CB_MAGIC == consumed_ctx->magic);
	const size_t n_uuids = consumed_ctx->n_uuids;

	foreach_numeric_client(db, n_uuids, client_check_consumed, ctx);
}

/** Message array that only contain EOF partition signal */
#define PARTITION_EOF_MSG_ARRAY(partition_id) {                                \
	.size=1, .count=1, .msgs = (rd_kafka_message_t[]){{                    \
		.err = RD_KAFKA_RESP_ERR__PARTITION_EOF,                       \
		.partition=partition_id                                        \
	}}}

/** Validate different sync partitions behavior */
static void validate_thread_sync_partitions() {
	size_t n_uuids = 10, i;
	json_t *uuids = json_object();
	sync_thread_t consume_thread_mock = {
#ifdef SYNC_THREAD_MAGIC
		.magic = SYNC_THREAD_MAGIC,
#endif
		.creation_rc = 0,
		.run = 1,
		.rk = NULL,
		.rk_conf = NULL,
		.thread = pthread_self(),
		.org_db = NULL, // It will be overriden
	};

	struct msg_consume_ctx consume_ctx = {
#ifdef MSG_CONSUME_CTX_MAGIC
		.magic = MSG_CONSUME_CTX_MAGIC,
#endif
		.thread = &consume_thread_mock,
		.in_sync = {
			.mutex= PTHREAD_MUTEX_INITIALIZER,
			.num_partitions = 2,
			.in_sync = (int []){0,0},
		},
		.n2kafka_id = NULL, // It will be overriden
	};


	struct kafka_message_array *sync_msgs = prepare_test8_msgs(n_uuids);
	struct kafka_message_array partition_eof_msg[] = {
		PARTITION_EOF_MSG_ARRAY(0),
		PARTITION_EOF_MSG_ARRAY(1),
	};

	struct check_consumed_cb_ctx check_ctx[] = {
		/* All messages arrives: +0x100 per position */
		CHECK_CONSUMED_CB_CTX_INIT(10, 0x01010101, 0x01010101),
		/* Partition 0 is in sync, so we only receive others 3 msgs */
		CHECK_CONSUMED_CB_CTX_INIT(10, 0x02020201, 0x02020201),
		/* Partition 1 is in sync, so we only receive others 2 msgs */
		CHECK_CONSUMED_CB_CTX_INIT(10, 0x03020301, 0x03020301),
	};

	const struct report_validation validators[] = {
		/* 1st step: No partition is in sync, we need to hear our
		own messages and any other messages */
		{
			.sync_msgs = sync_msgs, .consume_ctx = &consume_ctx,

			.org_db_cb_bef_report = check_consumed_cb,
			.org_db_cb_bef_report_ctx = &check_ctx[0],
		},
		/* 2nd step: Partition 0 sync. We need to stop listening our own
		messages in that partition, but keep listening our others
		partition messages */
		{
			.sync_msgs = &partition_eof_msg[0],
			.consume_ctx = &consume_ctx,
		},
		{
			.sync_msgs = sync_msgs, .consume_ctx = &consume_ctx,

			.org_db_cb_bef_report = check_consumed_cb,
			.org_db_cb_bef_report_ctx = &check_ctx[1],
		},
		{
			/* We receive EOF_PARTITION again, since we have
			consumed all pending messages */
			.sync_msgs = &partition_eof_msg[0],
			.consume_ctx = &consume_ctx,
		},
		/* 3rd step: Partition 1 sync. We need to stop listening our own
		messages in both partitions, and keep listening others */
		{
			.sync_msgs = &partition_eof_msg[1],
			.consume_ctx = &consume_ctx,
		},
		{
			.sync_msgs = sync_msgs, .consume_ctx = &consume_ctx,

			.org_db_cb_bef_report = check_consumed_cb,
			.org_db_cb_bef_report_ctx = &check_ctx[2],
		},
	};

	for (i=0; i<n_uuids; ++i) {
		char uuid[BUFSIZ];
		snprintf(uuid, sizeof(uuid), "%zu", i);
		json_object_set_new(uuids, uuid,
			new_config_uuid(sensor_enrichment(uuid), i*10));
	}

	validate_report0(uuids, validators, RD_ARRAYSIZE(validators));

	free_sync_msgs(sync_msgs); free(sync_msgs);
}

/*
 * main function
 */

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(validate_time_slices),
		cmocka_unit_test(validate_zero_clients_reports),
		cmocka_unit_test(validate_zero_bytes_reports),
		cmocka_unit_test(validate_reports_limits),
		cmocka_unit_test(validate_reset_bytes_consumed),
		cmocka_unit_test(validate_consume_reports),
		cmocka_unit_test(validate_invalid_reports),
		cmocka_unit_test(validate_thread_sync_partitions),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
