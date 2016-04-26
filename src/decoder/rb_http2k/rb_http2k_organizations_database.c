/*
**
** Copyright (c) 2014, Eneo Tecnologia
** Author: Eugenio Perez <eupm90@gmail.com>
** All rights reserved.
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU Affero General Public License as
** published by the Free Software Foundation, either version 3 of the
** License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU Affero General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "config.h"

#include "rb_http2k_organizations_database.h"
#include "rb_http2k_sync_common.h"
#include "rb_http2k_parser.h"
#include "util/util.h"

#include <librd/rdlog.h>
#include <librd/rdmem.h>
#include <yajl/yajl_gen.h>

#include <limits.h>

/// This macro does trick stringfication!
#define X(M,A) M(A)
#define ULONG_MAX_STR X(RD_STRINGIFY,ULONG_MAX)
/** Max unsigned long string size
  @note Not exactly right, the result is a bit bigger BC preprocessor
  does not do arithmetic
  */
#define ULONG_MAX_STR_SIZE sizeof(ULONG_MAX_STR)

/** Assert that argument is a valid organization */
static void organizations_db_entry_assert(
			const organization_db_entry_t *organization_entry) {
#ifdef ORGANIZATION_DB_ENTRY_MAGIC
	assert(ORGANIZATION_DB_ENTRY_MAGIC == organization_entry->magic);
#endif
}

void organizations_db_entry_decref(organization_db_entry_t *entry) {
	if (0 == ATOMIC_OP(sub,fetch,&entry->refcnt,1)) {
		if (entry->enrichment) {
			json_decref(entry->enrichment);
		}

		free(entry);
	}
}

/** Update database entry with new information (if needed)
  @param entry Entry to be updated
  @param new_config New config
  @return 0 if success, !0 in other case (reason printed)
  */
static int update_organization(organization_db_entry_t *entry,
				json_t *new_config) {
	int rc = 0;
	json_error_t jerr;
	json_int_t bytes_limit = 0;
	json_t *aux_enrichment = NULL;

	const int unpack_rc = json_unpack_ex(new_config, &jerr,
		JSON_STRICT,
		"{s?O,s?{s?I}}",
		"enrichment",&aux_enrichment,
		"limits","bytes",&bytes_limit);

	if (0 != unpack_rc) {
		const char *organization_uuid =
			organization_db_entry_get_uuid(entry);
		rdlog(LOG_ERR,"Couldn't unpack organization %s limits: %s",
						organization_uuid, jerr.text);
		rc = -1;
		goto unpack_err;
	}

	swap_ptrs(entry->enrichment, aux_enrichment);
	entry->bytes_limit.max = (uint64_t)bytes_limit;

unpack_err:
	if (aux_enrichment) {
		json_decref(aux_enrichment);
	}

	return rc;
}

/** Extracts client information in a uuid_entry
  @param sensor_uuid client UUID
  @param sensor_config Client config
  @return Generated uuid entry
  */
static organization_db_entry_t *create_organization_db_entry(
		const char *organization_uuid, json_t *organization_config) {
	assert(organization_uuid);
	assert(organization_config);

	organization_db_entry_t *entry = NULL;

	rd_calloc_struct(&entry, sizeof(*entry),
		-1, organization_uuid, &entry->uuid_entry.uuid,
		RD_MEM_END_TOKEN);

	if (NULL == entry) {
		rdlog(LOG_ERR,
			"Couldn't create uuid %s entry (out of memory?).",
			organization_uuid);
		goto err;
	}

#ifdef ORGANIZATION_DB_ENTRY_MAGIC
	entry->magic = ORGANIZATION_DB_ENTRY_MAGIC;
#endif
	uuid_entry_init(&entry->uuid_entry);

	entry->refcnt = 1;
	entry->uuid_entry.data = entry;

	const int rc = update_organization(entry,organization_config);
	if (rc != 0) {
		goto err_update;
	}

	return entry;

err_update:
	organizations_db_entry_decref(entry);
	entry = NULL;

err:
	return entry;
}

/** Clean consumed bytes
  @param org Organization
  @return Consumed bytes previous to clean
  */
#define organization_clean_consumed_bytes(org) \
	ATOMIC_OP(fetch,and,&(org)->bytes_limit.consumed,0)

void organization_add_other_consumed_bytes(organization_db_entry_t *org,
				const char *n2kafka_id, uint64_t bytes) {
	(void)n2kafka_id;
	pthread_mutex_t *reports_mutex = &org->db->reports_mutex;
	pthread_mutex_lock(reports_mutex);
	// If we increase both, next report will only contain OURs seen bytes
	organization_add_consumed_bytes(org, bytes);
	org->bytes_limit.reported += bytes;
	pthread_mutex_unlock(reports_mutex);
}

int organizations_db_init(organizations_db_t *organizations_db) {
	memset(organizations_db, 0, sizeof(*organizations_db));
	pthread_mutex_init(&organizations_db->reports_mutex, NULL);
	uuid_db_init(&organizations_db->uuid_db);
	return 0;
}

/** Transforms uuid entry to organization entry
  @param entry uuid entry
  @return result db_entry
  */
static organization_db_entry_t *uuid_db_entry2organization(
						uuid_entry_t *entry) {
	assert(entry);
	organization_db_entry_t *ret = entry->data;
	assert(ret);
	organizations_db_entry_assert(ret);
	return ret;
}

/** Obtains an entry from database, but does not increments reference counting
  @param db Database
  @param uuid UUID
  @return sensor from db
  */
static organization_db_entry_t *organizations_db_get0(organizations_db_t *db,
							const char *uuid) {
	uuid_entry_t *ret_entry = uuid_db_search(&db->uuid_db, uuid);
	return ret_entry ? uuid_db_entry2organization(ret_entry) : NULL;
}

/** Purge an organization that does not exists in the new configuration.
  @param new_orgs New organizations.
  @param entry Entry to check
  */
static void check_and_purge_organization(const json_t *new_orgs,
					organization_db_entry_t *entry) {
	const char *uuid = organization_db_entry_get_uuid(entry);
	if (NULL == json_object_get(new_orgs,uuid)) {
		/* This entry is not in the new db, we need to delete it */
		uuid_db_remove(&entry->db->uuid_db, &entry->uuid_entry);
		organizations_db_entry_decref(entry);
	}
}

/** Convenience function.
  @see check_and_purge_organization
  */
static void void_check_and_purge_organization(void *vnew_orgs, void *ventry) {
	const json_t *new_orgs = vnew_orgs;
	uuid_entry_t *uuid_entry = ventry;
	organization_db_entry_t *org = uuid_entry->data;
	organizations_db_entry_assert(org);
	check_and_purge_organization(new_orgs, org);
}

/** Clean all functions that are not in the new configuration
  @param db Database
  @param organizations New organizations config
  */
static void purge_old_organizations(organizations_db_t *db,
						json_t *organizations) {
	tommy_hashdyn_foreach_arg(&db->uuid_db,
		void_check_and_purge_organization, organizations);
}

void organizations_db_reload(organizations_db_t *db, json_t *organizations) {
	const char *organization_uuid;
	json_t *organization;

	/* 1st step: Delete all organziations that are not in the new config */
	purge_old_organizations(db,organizations);

	/* 2nd step: Update/create new ones */
	json_object_foreach(organizations, organization_uuid, organization) {
		organization_db_entry_t *entry = organizations_db_get0(db,
							organization_uuid);
		if(NULL == entry) {
			entry = create_organization_db_entry(organization_uuid,
								organization);
			if (NULL != entry) {
				entry->db = db;
				uuid_db_insert(&db->uuid_db,
							&entry->uuid_entry);
			}
		} else {
			update_organization(entry,organization);
		}

	}
}

organization_db_entry_t *organizations_db_get(organizations_db_t *db, const char *uuid) {
	organization_db_entry_t *ret = organizations_db_get0(db,uuid);
	if (ret) {
		ATOMIC_OP(add,fetch,&ret->refcnt,1);
	}
	return ret;
}

int organizations_db_exists(organizations_db_t *db, const char *uuid) {
	return NULL != organizations_db_get0(db,uuid);
}

/** Frees an element that is suppose to be an uuid_entry
  @param uuid_entry UUID entry
  */
static void void_organizations_db_entry_decref(void *vuuid_entry) {
	uuid_entry_t *uuid_entry = vuuid_entry;
	organization_db_entry_t *entry = uuid_entry->data;
	organizations_db_entry_assert(entry);
	organizations_db_entry_decref(entry);
}

void organizations_db_done(organizations_db_t *db) {
	uuid_db_foreach(&db->uuid_db,void_organizations_db_entry_decref);
	uuid_db_done(&db->uuid_db);
	pthread_mutex_destroy(&db->reports_mutex);
}

/*
 * REPORTS
 */

struct reports_ctx {
#ifndef NDEBUG
#define REPORTS_CTX_MAGIC 0x305ca1c305ca1cL
	uint64_t magic;
#endif

	enum {
		REPORTS_CTX_F__CLEAN=0x01, /// Clean consumed bytes
	} flags;

	struct {
		const char *buf;
		size_t size;
		time_t value;
		const struct itimerspec *reports_interval;
		const struct itimerspec *clean_interval;
	} timestamp;
	const char *n2kafka_id;

	struct kafka_message_array *ret;
	yajl_gen gen;
};

static void assert_reports_ctx(const struct reports_ctx *reports_ctx) {
	assert(REPORTS_CTX_MAGIC == reports_ctx->magic);
}

/** Special snprintf for a json key. In case of failure (not enough buffer or
  another error), it will print an error message and will raise return code.
  @param key JSON key to write it in error message
  @param buf Buffer to print in
  @param bufsize Size of buf
  @param fmt Format as snprint
  @return Same error code as internal snprintf
  */
static int verbose_snprintf(const char *key, char *buf, size_t bufsize,
							const char *fmt, ...) {
	char errbuf[BUFSIZ];
	va_list args;
	va_start(args, fmt);

	const int rc = vsnprintf(buf,bufsize,fmt,args);
	if (rc < 0) {
		rdlog(LOG_ERR, "Couldn't write %s report JSON: %s",
			key, mystrerror(errno, errbuf, sizeof(errbuf)));
	} else if (rc > (int)bufsize) {
		rdlog(LOG_ERR,
			"Couldn't write %s report JSON: %d bytes needed, "
					"%zu provided", key, rc, bufsize);
	}

	va_end(args);

	return rc;
}

/// message key information
struct key_info{
	/// Organization uuid
	const char *organization_uuid;
	/// N2kafka id
	const char *n2kafka_id;

	/// Time in what the message was sent
	time_t now;
	/// Configured report interval
	const struct itimerspec *report_interval;
	/// Configured clean interval
	const struct itimerspec *clean_interval;
};

/** Prints key in a dedicated buffer. Key will be obtained from:
  * n2kafka id
  * organization uuid
  * time interval (I.e., first second of report interval)

  @param key_info Information to create key
  @param buf Buffer to store key
  @param buf_size Size of buf
  @todo Prepare timestamp!
  @return key len if success, negative number if fail
  */
static int organizations_db_entry_report_key(const struct key_info *key_info,
						char *buf, size_t buf_size) {
	/// Time elapsed from last clean
	const time_t last_clean_elapsed_time =
		key_info->now % key_info->clean_interval->it_interval.tv_sec;
	/// Floor it to report_timestamp, so log compaction can work
	const time_t key_timestamp = last_clean_elapsed_time -
		key_info->now % key_info->report_interval->it_interval.tv_sec;
	return snprintf(buf, buf_size, "%s%c%s%c%tu",
		key_info->n2kafka_id, '\0', key_info->organization_uuid, '\0',
			key_timestamp);
}

/** Create organization consumed bytes report.
  @param org Organization to report
  @param reports_ctx Reports context
  @return 0 if success, 1 ioc
  @TODO error treatment
  */
static int organizations_db_entry_report(organization_db_entry_t *org,
		const struct reports_ctx *ctx) {
	char err[BUFSIZ];
	size_t i=0,len=0;
	unsigned char value_buf[ULONG_MAX_STR_SIZE],
		limit_buf[ULONG_MAX_STR_SIZE],total_buf[ULONG_MAX_STR_SIZE];
	int value_buf_rc,limit_buf_rc,total_buf_rc;
	json_t *enrichment = NULL;
	const unsigned char *gen_payload = NULL;
	char *payload = NULL;
	const uint64_t bytes_consumed =
				ctx->flags & REPORTS_CTX_F__CLEAN ?
				organization_clean_consumed_bytes(org)
				: organization_consumed_bytes(org);
	const uint64_t interval_bytes_consumed
			= bytes_consumed - org->bytes_limit.reported;
	struct key_info key_info = {
		.organization_uuid = organization_db_entry_get_uuid(org),
		.n2kafka_id        = ctx->n2kafka_id,
		.now               = ctx->timestamp.value,
		.report_interval   = ctx->timestamp.reports_interval,
		.clean_interval    = ctx->timestamp.clean_interval,
	};
	const int key_len = organizations_db_entry_report_key(&key_info, NULL,
									0);
	if (key_len < 0) {
		rdlog(LOG_ERR, "Couldn't get kafka message key: %s",
			mystrerror(errno, err, sizeof(err)));
	}

	if (0 == interval_bytes_consumed) {
		/* It does not have sense to send this! */
		return 0;
	}

// Convenience macro
#define safe_verbose_snprintf(rc,key,buf,fmt,...) do {                        \
	rc = verbose_snprintf(key,(char *)buf,sizeof(buf),fmt,__VA_ARGS__);   \
	if (rc < 0 || rc > (int)sizeof(buf)) return -1;                       \
	} while(0)
#define MY_KEY(k) .key=((const unsigned char *)k), .k_size=(strlen(k))
#define MY_VAL0(v,s) .val=((const unsigned char *)v), .v_size=((size_t)s)
#define MY_VAL(v) MY_VAL0(v,strlen(v))
#define MY_NTYPE .type=JSON_INTEGER
#define MY_STYPE .type=JSON_STRING

	safe_verbose_snprintf(value_buf_rc, "value", value_buf,
		"%"PRIu64, interval_bytes_consumed);
	safe_verbose_snprintf(limit_buf_rc, "client_limit_bytes", limit_buf,
		"%"PRIu64, organization_get_max_bytes(org));
	safe_verbose_snprintf(total_buf_rc, "client_total_bytes_consumed",
		total_buf, "%"PRIu64, bytes_consumed);

	struct {
		const unsigned char *key,*val;
		size_t k_size,v_size;
		json_type type;
	} json[] = {
		{MY_KEY(MONITOR_MSG_TIMESTAMP_KEY), MY_VAL0(ctx->timestamp.buf,
				ctx->timestamp.size), MY_NTYPE},
		{MY_KEY("client_total_bytes_consumed"),
				MY_VAL0(total_buf, total_buf_rc), MY_NTYPE},
		{MY_KEY("client_limit_bytes"), MY_VAL0(limit_buf,limit_buf_rc),
								MY_NTYPE},
		{MY_KEY(MONITOR_MSG_MONITOR_KEY),
			MY_VAL("organization_received_bytes"), MY_STYPE},
		{MY_KEY(MONITOR_MSG_VALUE_KEY),
				MY_VAL0(value_buf,value_buf_rc), MY_STYPE},
		{MY_KEY(MONITOR_MSG_ORGANIZATION_UUID_KEY),
				MY_VAL(key_info.organization_uuid), MY_STYPE},
		{MY_KEY(MONITOR_MSG_N2KAFKA_ID_KEY), MY_VAL(ctx->n2kafka_id),
								MY_STYPE},
		{MY_KEY("type"), MY_VAL("data"), MY_STYPE},
		{MY_KEY("unit"), MY_VAL("bytes"), MY_STYPE},
	};

#undef safe_verbose_snprintf
#undef MY_KEY
#undef MY_VAL
#undef MY_VAL0
#undef MY_NTYPE
#undef MY_STYPE
	yajl_gen_reset(ctx->gen, NULL);
	yajl_gen_clear(ctx->gen);

	yajl_gen_map_open(ctx->gen);

	for (i=0; i<RD_ARRAYSIZE(json); ++i) {
		yajl_gen_string(ctx->gen, json[i].key, json[i].k_size);
		if (json[i].type == JSON_INTEGER) {
			yajl_gen_number(ctx->gen,
				(const char *)json[i].val, json[i].v_size);
		} else {
			yajl_gen_string(ctx->gen, json[i].val, json[i].v_size);
		}
	}

	enrichment = organization_get_enrichment(org);
	if (enrichment) {
		gen_jansson_object(ctx->gen, enrichment);
	}

	yajl_gen_map_close(ctx->gen);
	yajl_gen_get_buf(ctx->gen,&gen_payload,&len);

	payload = calloc(1,len + (size_t)key_len + 1);
	if (NULL == payload) {
		rdlog(LOG_ERR, "Couldn't calloc buffer");
		yajl_gen_free(ctx->gen);
		return -1;
	}
	memcpy(payload, gen_payload, len);
	organizations_db_entry_report_key(&key_info, &payload[len],
							(size_t)key_len + 1);

	save_kafka_msg_key_in_array(ctx->ret, &payload[len], (size_t)key_len,
		payload, len, NULL);

	/* Let's assume the report will reach the destination */
	org->bytes_limit.reported =
		(ctx->flags & REPORTS_CTX_F__CLEAN) ? 0 : bytes_consumed;

	return 0;
}

/** Convenience function */
static void void_organizations_db_entry_report(void *arg, void *obj) {
	struct reports_ctx *ctx = arg;
	uuid_entry_t *entry = obj;
	organization_db_entry_t *org = uuid_db_entry2organization(entry);
	assert_reports_ctx(ctx);
	organizations_db_entry_assert(org);
	organizations_db_entry_report(org,ctx);
}

/// Aux function to print timestamp
static int print_timestamp(time_t timestamp, char *buf, size_t buf_size) {
	char err[BUFSIZ];
	const int snprintf_rc = snprintf(buf, buf_size, "%tu", timestamp);
	if (snprintf_rc > (int)buf_size) {
		rdlog(LOG_ERR,
			"Couldn't print organization report timestamp: needed"
			"%d, provided %zu", snprintf_rc, buf_size);
		return -1;
	} else if (snprintf_rc < 0) {
		rdlog(LOG_ERR,
			"Couldn't print organization report timestamp: %s",
			mystrerror(errno, err, sizeof(err)));
		return -1;
	}

	return snprintf_rc;
}

/** Get a bytes consumed report for each organization
  @param db Database
  @return Reports
  */
struct kafka_message_array *organization_db_interval_consumed0(
				organizations_db_t *db, time_t now,
				const struct itimerspec *interval,
				const struct itimerspec *clean_interval,
				const char *n2kafka_id, int clean) {
	static const char n2kafka_safe_id[] = "unknown_n2kafka";
	char timestamp_buf[ULONG_MAX_STR_SIZE];

	struct reports_ctx ctx = {
#ifdef REPORTS_CTX_MAGIC
		.magic = REPORTS_CTX_MAGIC,
#endif
		.ret = NULL,
		.gen = yajl_gen_alloc(NULL),
		.n2kafka_id = n2kafka_id ? n2kafka_id : n2kafka_safe_id,
		.flags = (clean ? REPORTS_CTX_F__CLEAN : 0),
		.timestamp = {
			.value = now,
			.reports_interval = interval,
			.clean_interval   = clean_interval,
		}
	};

	const int print_rc = print_timestamp(now, timestamp_buf,
							sizeof(timestamp_buf));

	if (print_rc > 0) {
		ctx.timestamp.size = (size_t)print_rc;
		ctx.timestamp.buf  = timestamp_buf;
	} else {
		goto timestamp_err;
	}

	if (NULL == ctx.gen) {
		rdlog(LOG_ERR, "Couldn't allocate yajl gen (out of memory?)");
		goto yajl_gen_err;
	}

	pthread_mutex_lock(&db->reports_mutex);
	const size_t entries = uuid_db_count(&db->uuid_db);
	ctx.ret = new_kafka_message_array(entries);

	if (NULL == ctx.ret) {
		rdlog(LOG_ERR, "Couldn't allocate kafka message array"
			"(out of memory?)");
		goto ret_err;
	}

	rdlog(LOG_DEBUG, "Reporting %sabout %zu entries",
		clean ? "and cleaning " : "", entries);

	uuid_db_foreach_arg(&db->uuid_db, void_organizations_db_entry_report,
									&ctx);

ret_err:
	pthread_mutex_unlock(&db->reports_mutex);

	yajl_gen_free(ctx.gen);

timestamp_err:
yajl_gen_err:
	return ctx.ret;
}
