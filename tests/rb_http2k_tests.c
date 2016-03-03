#include "../src/decoder/rb_http2k_decoder.c"

struct message_in {
	const char *msg;
	size_t size;
};

typedef void (*check_callback_fn)(struct rb_session **,void *opaque);

/** Template for rb_decoder test
	@param args Arguments like client_ip, topic, etc
	@param msgs Input messages
	@param msgs_len Length of msgs
	@param check_callback Array of functions that will be called with each
	session status. It is suppose to be the same length as msgs array.
	@param check_callback_opaque Opaque used in the second parameter of
	check_callback[iteration] call
	*/
static void test_rb_decoder0(const char *config_str, keyval_list_t *args,
		struct message_in *msgs, check_callback_fn *check_callback,
		size_t msgs_len, void *check_callback_opaque) {
	json_error_t jerr;
	size_t i;
	json_t *config = json_loads(config_str, 0, &jerr);
	if(NULL == config) {
		rdlog(LOG_CRIT,"Couldn't unpack JSON config: %s",jerr.text);
		assert(0);
	}

	const json_t *decoder_config = NULL;
	const int unpack_rc = json_unpack_ex(config, &jerr, 0, "{s:o}",
		"rb_http2k_config",&decoder_config);
	if(0 != unpack_rc) {
		rdlog(LOG_CRIT,"Can't unpack config: %s",jerr.text);
		assert(0);
	}

	struct rb_database rb_db;
	init_rb_database(&rb_db);
	parse_rb_config(&rb_db,decoder_config);

	struct rb_opaque rb_opaque = {
#ifdef RB_OPAQUE_MAGIC
		.magic = RB_OPAQUE_MAGIC,
#endif
		.rb_config = &global_config.rb,
	};

	struct rb_session *my_session = NULL;

	for(i=0;i<msgs_len;++i) {
		process_rb_buffer(msgs[i].msg, msgs[i].msg ? msgs[i].size : 0, args,
			&rb_opaque, &my_session);
		check_callback[i](&my_session,check_callback_opaque);
	}

	free_valid_rb_database(&rb_db);
	json_decref(config);
}

/** Function that check that session has no messages
	@param sess Session pointer
	@param unused context information
*/
static void check_zero_messages(struct rb_session **sess,
            void *unused __attribute__((unused))) __attribute__((unused));
static void check_zero_messages(struct rb_session **sess,
                    void *unused __attribute__((unused))) {

	assert(NULL != sess);
	assert(NULL != *sess);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));
}
