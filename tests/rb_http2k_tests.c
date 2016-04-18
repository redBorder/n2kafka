#include "decoder/rb_http2k/rb_http2k_decoder.c"
#include "util/kafka.h"

struct message_in {
	const char *msg;
	size_t size;
};

typedef void (*check_callback_fn)(struct rb_session **,void *opaque);

static void test_rb_decoder_setup(const char *config_txt) {
	static const char TEMP_TEMPLATE[] = "n2ktXXXXXX";
	init_global_config();
	char temp_filename[sizeof(TEMP_TEMPLATE)];
	strcpy(temp_filename,TEMP_TEMPLATE);
	int temp_fd = mkstemp(temp_filename);
	assert(temp_fd >= 0);
	write(temp_fd, config_txt, strlen(config_txt));
	parse_config(temp_filename);
	unlink(temp_filename);
	close(temp_fd);
}

static void test_rb_decoder_teardown() {
	free_global_config();
}

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
	size_t i;

	test_rb_decoder_setup(config_str);

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

	test_rb_decoder_teardown();
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

/** This function just checks that session is NULL */
static void check_null_session(struct rb_session **sess,
		void *unused __attribute__((unused)))
		__attribute__((unused));
static void check_null_session(struct rb_session **sess,
                    void *unused __attribute__((unused))) {

	assert(NULL != sess);
	assert(NULL == *sess);
}
