
#include "config.h"
#include "topic_database.h"

#include <librd/rdavl.h>
#include <librd/rdmem.h>

typedef TAILQ_HEAD(,topic_s) topics_list;

#ifndef NDEBUG
#define TOPIC_S_MAGIC 0x01CA1C01CA1C01CL
#endif

#define topic_list_init(l) TAILQ_INIT(l)
#define topic_list_push(l,e) TAILQ_INSERT_TAIL(l,e,list_node);

struct topic_s {
#ifdef TOPIC_S_MAGIC
       uint64_t magic;
#endif
       rd_kafka_topic_t *rkt;
       const char *topic_name;
       uint64_t refcnt;

       rd_avl_node_t avl_node;
       TAILQ_ENTRY(topic_s) list_node;

       const char *partition_key;
};

rd_kafka_topic_t *topics_db_get_rdkafka_topic(struct topic_s *topic) {
	return topic->rkt;
}

void topic_decref(struct topic_s *topic) {
	if(0==ATOMIC_OP(sub,fetch,&topic->refcnt,1)) {
		rd_kafka_topic_destroy(topic->rkt);
		free(topic);
	}
}

static int topics_cmp(const struct topic_s *t1, const struct topic_s *t2) {
	assert(t1);
	assert(t1->topic_name);
	assert(t2);
	assert(t2->topic_name);

	return strcmp(t1->topic_name,t2->topic_name);
}

static int void_topics_cmp(const void *_t1, const void *_t2) {
	assert(_t1);
	assert(_t2);
	const struct topic_s *t1 = _t1;
	const struct topic_s *t2 = _t2;

#ifdef TOPIC_S_MAGIC
	assert(TOPIC_S_MAGIC == t1->magic);
	assert(TOPIC_S_MAGIC == t2->magic);
#endif

	return topics_cmp(t1,t2);
}

struct topics_db {
	rd_avl_t topics;
	/// @TODO change by a memctx
	topics_list list;
};

const char *topics_db_partition_key(struct topic_s *topic) {
	return topic->partition_key;
}

struct topics_db *topics_db_new() {
	struct topics_db *ret = calloc(1,sizeof(*ret));

	if(ret) {
		rd_avl_init(&ret->topics, void_topics_cmp, 0);
		topic_list_init(&ret->list);
	}

	return ret;
}

static void free_topics(topics_list *list) {
	struct topic_s *elm = NULL, *tmpelm = NULL;
	TAILQ_FOREACH_SAFE(elm, tmpelm, list, list_node) {
#ifdef TOPIC_S_MAGIC
		assert(TOPIC_S_MAGIC == elm->magic);
#endif
		topic_decref(elm);
	}
}

static struct topic_s *get_topic_borrow(struct topics_db *db, const char *topic) {
	char buf[strlen(topic) + 1];
	strcpy(buf, topic);

	struct topic_s dumb_topic = {
#ifdef TOPIC_S_MAGIC
		.magic = TOPIC_S_MAGIC,
#endif
		.topic_name = buf
	};

	return RD_AVL_FIND_NODE_NL(&db->topics, &dumb_topic);
}

struct topic_s *topics_db_get_topic(struct topics_db *db, const char *topic) {
	struct topic_s *ret = get_topic_borrow(db,topic);
	if(ret) {
		ATOMIC_OP(add,fetch,&ret->refcnt,1);
	}
	return ret;
}

int topics_db_topic_exists(struct topics_db *db, const char *topic) {
	return NULL != get_topic_borrow(db, topic);
}

int topics_db_add(struct topics_db *db,rd_kafka_topic_t *rkt,
        const char *partition_key, size_t partition_key_len) {
	struct topic_s *topic_s = NULL;

	const char *topic_name = rd_kafka_topic_name(rkt);

	rd_calloc_struct(&topic_s,sizeof(*topic_s),
			-1,topic_name,&topic_s->topic_name,
			partition_key_len,partition_key,&topic_s->partition_key,
			RD_MEM_END_TOKEN);

	if(topic_s) {
#ifdef TOPIC_S_MAGIC
		topic_s->magic = TOPIC_S_MAGIC;
#endif

		if (0 == partition_key_len) {
			topic_s->partition_key = NULL;
		}

		topic_s->rkt = rkt;
		topic_s->refcnt = 1;

		RD_AVL_INSERT(&db->topics,topic_s,avl_node);
		topic_list_push(&db->list,topic_s);
	}

	return topic_s != NULL;
}

void topics_db_done(struct topics_db *db) {
   free_topics(&db->list);
   free(db);
}