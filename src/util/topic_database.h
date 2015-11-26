#pragma once

#include <librdkafka/rdkafka.h>

struct topic_s;

void topic_decref(struct topic_s *topic);

/** Topics database.
	@warning The only function thread safe is topics_db_get_topic */
struct topics_db;

/** It creates a new database */
struct topics_db *topics_db_new();

/** Add a topic into a database 
	@param topics_db Topics database
	@param topic Topic to produce
	@param partition_key Field to extract partition key
	@param partition_key_len Length of partition key
	@return 1 if OK, 0 ioc.
	*/
int topics_db_add(struct topics_db *topics_db,rd_kafka_topic_t *rkt,
	const char *partition_key,size_t partition_key_len);

/** Returns JSON key for what message has to be partitioned
	@param topic Topic.
	@return JSON key */
const char *topics_db_partition_key(struct topic_s *topic);

/** Get a topic from database.
	@param db Database
	@param topic Topic name to search
	@return Associated topic. Need to decref returned value when finish. */
struct topic_s *topics_db_get_topic(struct topics_db *db, const char *topic);

/** Check if a topic exists in database
	@param db Database to search in.
	@param topic Topic to search for.
	@return 1 if exists, 0 if not */
int topics_db_topic_exists(struct topics_db *db, const char *topic);

/** Extract rdkafka topic from topic handler
	@param topic topic handler
	@return rdkafka usable topic */
rd_kafka_topic_t *topics_db_get_rdkafka_topic(struct topic_s *topic);

/** Get topic handler's topic name
	@param topic Topic handler
	@return Name of topic
	*/
const char *topics_db_get_topic_name(const struct topic_s *topic);

/** Destroy a topics db.
	You can keep using topics extracted from it, but you can't search for more topics
	*/
void topics_db_done(struct topics_db *topics_db);
