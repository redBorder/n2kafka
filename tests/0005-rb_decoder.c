#include "rb_json_tests.c"
#include "rb_http2k_tests.c"

#include "../src/listener/http.c"

#include <setjmp.h>
#include <cmocka.h>
#include <assert.h>

static const char CONFIG_TEST[] =
    "{"
        "\"brokers\": \"localhost\","
        "\"rb_http2k_config\": {"
            "\"uuids\" : {"
                    "\"abc\" : {"
                    		"\"sensor_uuid\":\"abc\","
                            "\"a\":1,"
                            "\"b\":\"c\","
                            "\"d\":true,"
                            "\"e\":null"
                    "},"
                    "\"def\" : {"
                    		"\"sensor_uuid\":\"def\","
                            "\"f\":1,"
                            "\"g\":\"w\","
                            "\"h\":false,"
                            "\"i\":null,"
                            "\"j\":2.5"
                    "},"
                    "\"ghi\" : {"
                        "\"o\": {"
                            "\"a\":90"
                        "}"
                    "},"
                    "\"jkl\" : {"
                        "\"v\":[1,2,3,4,5]"
                    "},"
                    "\"v0\" : {"
                        "\"v\":[]"
                    "},"
                    "\"v1\" : {"
                        "\"v\":[0]"
                    "},"
                    "\"v2\" : {"
                        "\"v\":[0,1]"
                    "},"
                    "\"v3\" : {"
                        "\"v\":[0,1,2]"
                    "}"
            "},"
            "\"topics\" : {"
                    "\"rb_flow\": {"
                            "\"partition_key\":\"client_mac\","
                            "\"partition_algo\":\"mac\""
                    "},"
                    "\"rb_event\": {"
                    "}"
            "}"
        "}"
    "}";

static const char *VALID_URL = "/rbdata/abc/rb_flow";

static void test_validate_uri() {
	test_rb_decoder_setup(CONFIG_TEST);

	int allok = 1;
	char *topic=NULL,*uuid=NULL;
	int validation_rc = rb_http2k_validation(
		NULL /* @TODO this should change */,VALID_URL,
		&global_config.rb.database, &allok,&topic,&uuid,"test_ip");

	assert(MHD_YES == validation_rc);
	assert(0==strcmp(topic,"rb_flow"));
	assert(0==strcmp(uuid,"abc"));

	free(topic);
	free(uuid);

	test_rb_decoder_teardown();
}

static void prepare_args(
        const char *topic,const char *sensor_uuid,const char *client_ip,
        struct pair *mem,size_t memsiz,keyval_list_t *list) {
	assert(3==memsiz);
	memset(mem,0,sizeof(*mem)*3);

	mem[0].key   = "topic";
	mem[0].value = topic;
	mem[1].key   = "sensor_uuid";
	mem[1].value = sensor_uuid;
	mem[2].key   = "client_ip";
	mem[2].value = client_ip;

	add_key_value_pair(list,&mem[0]);
	add_key_value_pair(list,&mem[1]);
	add_key_value_pair(list,&mem[2]);
}

static void check_rb_decoder_double0(struct rb_session **sess,
                void *unused __attribute__((unused)),size_t expected_size) {
	size_t i=0;
	rd_kafka_message_t rkm[2];
	json_error_t jerr;
	const char *client_mac,*application_name,*sensor_uuid,*b;
	json_int_t a;
	int d;

	assert(expected_size==rd_kafka_msg_q_size(&(*sess)->msg_queue));
	rd_kafka_msg_q_dump(&(*sess)->msg_queue,rkm);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));

	for(i=0;i<expected_size;++i) {
		json_t *root = json_loadb(rkm[i].payload, rkm[i].len, 0, &jerr);
		if(NULL == root) {
			rdlog(LOG_ERR,"Couldn load file: %s",jerr.text);
			assert(0);
		}

		const int rc = json_unpack_ex(root, &jerr, 0,
			"{s:s,s:s,s:s,s:I,s:s,s:n,s:b}",
			"client_mac",&client_mac,"application_name",&application_name,
			"sensor_uuid",&sensor_uuid,"a",&a,"b",&b,"e","d",&d);

		if(rc != 0) {
			rdlog(LOG_ERR,"Couldn't unpack values: %s",jerr.text);
			assert(0);
		}

		if(i==0) {
			assert(0==strcmp(client_mac,"54:26:96:db:88:01"));
		} else {
			assert(0==strcmp(client_mac,"54:26:96:db:88:02"));
		}
		assert(0==strcmp(application_name,"wwww"));
		assert(0==strcmp(sensor_uuid,"abc"));
		assert(a == 1); /* Enrichment! original message had 5 here */
		assert(0==strcmp(b,"c"));
		assert(d == 1);

		json_decref(root);
		free(rkm[i].payload);
	}
}

static void check_rb_decoder_simple(struct rb_session **sess,void *opaque) {
	check_rb_decoder_double0(sess,opaque,1);
}

static void check_rb_decoder_double(struct rb_session **sess,void *opaque) {
	check_rb_decoder_double0(sess,opaque,2);
}

static void check_rb_decoder_simple_def(struct rb_session **sess,
                void *unused __attribute__((unused))) {
	rd_kafka_message_t rkm[2];
	json_error_t jerr;
	const char *client_mac,*application_name,*sensor_uuid,*g;
	json_int_t f;
	int h,u;

	assert(1==rd_kafka_msg_q_size(&(*sess)->msg_queue));
	rd_kafka_msg_q_dump(&(*sess)->msg_queue,rkm);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));

	json_t *root = json_loadb(rkm[0].payload, rkm[0].len, 0, &jerr);
	if(NULL == root) {
		rdlog(LOG_ERR,"Couldn load file: %s",jerr.text);
		assert(0);
	}

	const int rc = json_unpack_ex(root, &jerr, 0,
		"{s:s,s:s,s:s,s:I,s:s,s:b,s:n,s:b}",
		"client_mac",&client_mac,"application_name",&application_name,
		"sensor_uuid",&sensor_uuid,"f",&f,"g",&g,"h",&h,"i","u",&u);

	if(rc != 0) {
		rdlog(LOG_ERR,"Couldn't unpack values: %s",jerr.text);
		assert(0);
	}

	assert(0==strcmp(client_mac,"54:26:96:db:88:02"));
	assert(0==strcmp(application_name,"wwww"));
	assert(0==strcmp(sensor_uuid, "def"));
	assert(1==f);
	assert(0==strcmp(g, "w"));
	assert(0==h);
	assert(0!=u);

	json_decref(root);
	free(rkm[0].payload);
}

static void check_rb_decoder_object(struct rb_session **sess,
                void *unused __attribute__((unused))) {
	rd_kafka_message_t rkm;
	json_error_t jerr;
	const char *client_mac,*application_name,*sensor_uuid,*b;
	json_int_t a,t1;
	int d;

	assert(1==rd_kafka_msg_q_size(&(*sess)->msg_queue));
	rd_kafka_msg_q_dump(&(*sess)->msg_queue,&rkm);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));

	json_t *root = json_loadb(rkm.payload, rkm.len, 0, &jerr);
	if(NULL == root) {
		rdlog(LOG_ERR,"Couldn load file: %s",jerr.text);
		assert(0);
	}

	const int rc = json_unpack_ex(root, &jerr, 0,
		"{s:s,s:s,s:s,s:I,s:s,s:n,s:b,s:{s:I}}",
		"client_mac",&client_mac,"application_name",&application_name,
		"sensor_uuid",&sensor_uuid,"a",&a,"b",&b,"e","d",&d,
		"object","t1",&t1
		);

	if(rc != 0) {
		rdlog(LOG_ERR,"Couldn't unpack values: %s",jerr.text);
		assert(0);
	}

	assert(0==strcmp(client_mac,"54:26:96:db:88:01"));
	assert(0==strcmp(application_name,"wwww"));
	assert(0==strcmp(sensor_uuid,"abc"));
	assert(a == 1); /* Enrichment! original message had 5 here */
	assert(0==strcmp(b,"c"));
	assert(d == 1);
	assert(t1== 1);

	json_decref(root);
	free(rkm.payload);
}

static void check_rb_decoder_object_enrich(struct rb_session **sess,
                void *unused __attribute__((unused))) {
	rd_kafka_message_t rkm;
	json_error_t jerr;
	const char *client_mac,*application_name,*sensor_uuid;
	json_int_t a,a2,u_a;

	assert(1==rd_kafka_msg_q_size(&(*sess)->msg_queue));
	rd_kafka_msg_q_dump(&(*sess)->msg_queue,&rkm);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));

	json_t *root = json_loadb(rkm.payload, rkm.len, 0, &jerr);
	if(NULL == root) {
		rdlog(LOG_ERR,"Couldn load file: %s",jerr.text);
		assert(0);
	}

	const int rc = json_unpack_ex(root, &jerr, 0,
		"{s:s,s:s,s:s,s:I,s:{s:I},s:{s:I}}",
		"client_mac",&client_mac,"application_name",&application_name,
		"sensor_uuid",&sensor_uuid,"a",&a,
		"o","a",&a2,
		"u","a",&u_a
		);

	if(rc != 0) {
		rdlog(LOG_ERR,"Couldn't unpack values: %s",jerr.text);
		assert(0);
	}

	assert(0==strcmp(client_mac,"54:26:96:db:88:01"));
	assert(0==strcmp(application_name,"wwww"));
	assert(0==strcmp(sensor_uuid,"ghi"));
	assert(a == 5);
	/* Enrichment */
	assert(a2 == 90);
	assert(u_a == 1);

	json_decref(root);
	free(rkm.payload);
}

static void check_rb_decoder_array_enrich_v00(struct rb_session **sess,
                size_t v_size) {
	size_t i;
	char buf[BUFSIZ];
	rd_kafka_message_t rkm;
	json_error_t jerr;
	const char *client_mac,*application_name,*sensor_uuid;
	json_t *v=NULL;

	assert(1==rd_kafka_msg_q_size(&(*sess)->msg_queue));
	rd_kafka_msg_q_dump(&(*sess)->msg_queue,&rkm);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));

	json_t *root = json_loadb(rkm.payload, rkm.len, 0, &jerr);
	if(NULL == root) {
		rdlog(LOG_ERR,"Couldn load file: %s",jerr.text);
		assert(0);
	}

	const int rc = json_unpack_ex(root, &jerr, 0,
		"{s:s,s:s,s:s,s:o}",
		"client_mac",&client_mac,"application_name",&application_name,
		"sensor_uuid",&sensor_uuid,"v",&v);

	if(rc != 0) {
		rdlog(LOG_ERR,"Couldn't unpack values: %s",jerr.text);
		assert(0);
	}

	snprintf(buf,sizeof(buf),"v%zu",v_size);

	assert(0==strcmp(sensor_uuid,buf));
	assert(0==strcmp(client_mac,"54:26:96:db:88:01"));
	assert(0==strcmp(application_name,"wwww"));

	/* Enrichment vector */
	assert(json_is_array(v));
	assert(v_size==json_array_size(v));
	for (i=0; i<v_size; ++i) {
		json_t *v_i = json_array_get(v,i);
		assert(json_is_integer(v_i));
		assert((int)i == json_integer_value(v_i));
	}

	json_decref(root);
	free(rkm.payload);
}

#define CHECK_RB_DECODER_ARRAY_FN(N) \
static void check_rb_decoder_array_enrich_v##N(struct rb_session **sess, \
		void *unused __attribute__((unused))) { \
	check_rb_decoder_array_enrich_v00(sess,N);  \
}

CHECK_RB_DECODER_ARRAY_FN(0)
CHECK_RB_DECODER_ARRAY_FN(1)
CHECK_RB_DECODER_ARRAY_FN(2)
CHECK_RB_DECODER_ARRAY_FN(3)

static void check_rb_decoder_array_enrich(struct rb_session **sess,
                void *unused __attribute__((unused))) {
	rd_kafka_message_t rkm;
	json_error_t jerr;
	const char *client_mac,*application_name,*sensor_uuid;
	json_t *g=NULL,*v=NULL;
	json_t *g0=NULL,*g1=NULL,*g2=NULL;
	json_t *v0=NULL,*v1=NULL,*v2=NULL,*v3=NULL,*v4=NULL;

	assert(1==rd_kafka_msg_q_size(&(*sess)->msg_queue));
	rd_kafka_msg_q_dump(&(*sess)->msg_queue,&rkm);
	assert(0==rd_kafka_msg_q_size(&(*sess)->msg_queue));

	json_t *root = json_loadb(rkm.payload, rkm.len, 0, &jerr);
	if(NULL == root) {
		rdlog(LOG_ERR,"Couldn load file: %s",jerr.text);
		assert(0);
	}

	const int rc = json_unpack_ex(root, &jerr, 0,
		"{s:s,s:s,s:s,s:o,s:o}",
		"client_mac",&client_mac,"application_name",&application_name,
		"sensor_uuid",&sensor_uuid,"g",&g,"v",&v);

	if(rc != 0) {
		rdlog(LOG_ERR,"Couldn't unpack values: %s",jerr.text);
		assert(0);
	}

	assert(0==strcmp(client_mac,"54:26:96:db:88:01"));
	assert(0==strcmp(application_name,"wwww"));
	assert(0==strcmp(sensor_uuid,"jkl"));

	/* Original vector */
	assert(json_is_array(g));
	assert(3==json_array_size(g));
	g0 = json_array_get(g,0);
	g1 = json_array_get(g,1);
	g2 = json_array_get(g,2);
	assert(json_is_string(g0));
	assert(0==strcmp("a",json_string_value(g0)));
	assert(json_is_integer(g1));
	assert(5==json_integer_value(g1));
	assert(json_is_null(g2));

	/* Enrichment vector */
	assert(json_is_array(v));
	assert(5==json_array_size(v));
	v0 = json_array_get(v,0);
	v1 = json_array_get(v,1);
	v2 = json_array_get(v,2);
	v3 = json_array_get(v,3);
	v4 = json_array_get(v,4);
	assert(json_is_integer(v0));
	assert(json_is_integer(v1));
	assert(json_is_integer(v2));
	assert(json_is_integer(v3));
	assert(json_is_integer(v4));
	assert(1 == json_integer_value(v0));
	assert(2 == json_integer_value(v1));
	assert(3 == json_integer_value(v2));
	assert(4 == json_integer_value(v3));
	assert(5 == json_integer_value(v4));

	json_decref(root);
	free(rkm.payload);
}

static void test_rb_decoder_simple() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"abc\", \"a\":5}",  \
		check_rb_decoder_simple)                                              \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

/// Simple decoding with another enrichment
static void test_rb_decoder_simple_def() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","def","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_mac\": \"54:26:96:db:88:02\", "                          \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"def\", "   \
		"\"a\":5, \"u\":true}",                                       \
		check_rb_decoder_simple_def)                                  \
	/* Free & Check that session has been freed */                        \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

/** Two messages in the same input string */
static void test_rb_decoder_double() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"abc\", \"a\":5}"   \
	  "{\"client_mac\": \"54:26:96:db:88:02\", "                              \
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"abc\", \"a\":5}",  \
		check_rb_decoder_double)                                              \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

static void test_rb_decoder_half() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_mac\": \"54:26:96:db:88:01\", ",check_zero_messages)         \
	X("\"application_name\": \"wwww\", \"sensor_uuid\":\"abc\", \"a\":5}",    \
		check_rb_decoder_simple)                                              \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

/** Checks that the decoder can handle to receive the half of a string */
static void test_rb_decoder_half_string() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_mac\": \"54:26:96:",check_zero_messages)                     \
	X("db:88:01\", \"application_name\": \"wwww\", "                          \
		"\"sensor_uuid\":\"abc\", \"a\":5}",                                  \
		check_rb_decoder_simple)                                              \
	X("{\"client_mac\": \"",check_zero_messages)                              \
	X("54:26:96:db:88:01\", \"application_name\": \"wwww\", "                 \
		"\"sensor_uuid\":\"abc\", \"a\":5}",                                  \
		check_rb_decoder_simple)                                              \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

/** Checks that the decoder can handle to receive the half of a key */
static void test_rb_decoder_half_key() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_",check_zero_messages)                                       \
	X("mac\": \"54:26:96:db:88:01\", \"application_name\": \"wwww\", "        \
		"\"sensor_uuid\":\"abc\", \"a\":5}",                                  \
		check_rb_decoder_simple)                                              \
	X("{\"client_mac",check_zero_messages)                                    \
	X("\": \"54:26:96:db:88:01\", \"application_name\": \"wwww\", "           \
		"\"sensor_uuid\":\"abc\", \"a\":5}",                                  \
		check_rb_decoder_simple)                                              \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

/** Test object that don't need to enrich */
static void test_rb_decoder_objects() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","abc","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_",check_zero_messages)                                       \
	X("mac\": \"54:26:96:db:88:01\", \"application_name\": \"wwww\", "        \
		"\"sensor_uuid\":\"abc\", \"object\":{\"t1\":1}, \"a\":5}",           \
		check_rb_decoder_object)                                              \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

/** Test if we can enrich by an object*/
static void test_rb_object_enrich() {
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","ghi","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_",check_zero_messages)                                       \
	X("mac\": \"54:26:96:db:88:01\", \"application_name\": \"wwww\", "        \
		"\"sensor_uuid\":\"ghi\", \"a\":5, \"u\":{\"a\":1}}",                 \
		check_rb_decoder_object_enrich)                                       \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", "                                    \
		"\"sensor_uuid\":\"ghi\", \"a\":5, \"o\":5, \"u\":{\"a\":1}}",        \
		check_rb_decoder_object_enrich)                                       \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", "                                    \
		"\"sensor_uuid\":\"ghi\", \"a\":5, \"o\":{\"a\":5}, \"u\":{\"a\":1}}",\
		check_rb_decoder_object_enrich)                                       \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", "                                    \
		"\"sensor_uuid\":\"ghi\", \"o\":{\"a\":5}, \"u\":{\"a\":1}, \"a\":5}",\
		check_rb_decoder_object_enrich)                                       \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", "                                    \
		"\"sensor_uuid\":\"ghi\", \"o\":null, \"u\":{\"a\":1}, \"a\":5}",     \
		check_rb_decoder_object_enrich)                                       \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", "                                    \
		"\"sensor_uuid\":\"ghi\", \"o\":true, \"u\":{\"a\":1}, \"a\":5}",     \
		check_rb_decoder_object_enrich)                                       \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"application_name\": \"wwww\", "                                    \
		"\"sensor_uuid\":\"ghi\", \"o\":false, \"u\":{\"a\":1}, \"a\":5}",    \
		check_rb_decoder_object_enrich)                                       \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"sensor_uuid\":\"ghi\", \"o\":{"                                    \
			"\"n\":null,\"t\":true,\"f\":false,\"d\":3,\"o\":{},\"a\":[],"    \
			"\"s\":\"s\"},"                                                   \
		"\"application_name\": \"wwww\", "                                    \
		"\"u\":{\"a\":1}, \"a\":5}",                                          \
		check_rb_decoder_object_enrich)                                       \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"sensor_uuid\":\"ghi\", \"o\":{"                                    \
			"\"n\":null,\"t\":true,\"f\":false,\"d\":3,\"a\":[],"             \
			"\"s\":\"s\",\"o\":{}},"                                          \
		"\"application_name\": \"wwww\", "                                    \
		"\"u\":{\"a\":1}, \"a\":5}",                                          \
		check_rb_decoder_object_enrich)                                       \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"sensor_uuid\":\"ghi\", \"o\":{"                                    \
			"\"n\":null,\"t\":true,\"f\":false,\"d\":3,\"o\":{},"             \
			"\"s\":\"s\",\"a\":[]},"                                          \
		"\"application_name\": \"wwww\", "                                    \
		"\"u\":{\"a\":1}, \"a\":5}",                                          \
		check_rb_decoder_object_enrich)                                       \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}

static void test_rb_array_enrich0(const char *sensor_uuid,
					check_callback_fn check_callback) {
	char msg_buf[BUFSIZ];
	struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow",sensor_uuid,"127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);
	const int snprintf_rc = snprintf(msg_buf,sizeof(msg_buf),
		"{\"client_mac\": \"54:26:96:db:88:01\", "
		"\"application_name\": \"wwww\", \"sensor_uuid\":\"%s\", "
		"\"v\":5, \"g\":[\"a\",5,null]}",sensor_uuid);

	assert(snprintf_rc > 0);
	assert(snprintf_rc < BUFSIZ);

	struct message_in msgs[] = {
		{msg_buf,(size_t)snprintf_rc},
		{NULL, 0}
	};

	check_callback_fn callbacks_functions[] = {
		check_callback,
		check_null_session
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);
}

/** Test if we can enrich with a 0-length array */
static void test_rb_array_enrich_v0() {
	test_rb_array_enrich0("v0",check_rb_decoder_array_enrich_v0);
}

/** Test if we can enrich with a 1-length array */
static void test_rb_array_enrich_v1() {
	test_rb_array_enrich0("v1",check_rb_decoder_array_enrich_v1);
}

/** Test if we can enrich with a 2-length array */
static void test_rb_array_enrich_v2() {
	test_rb_array_enrich0("v2",check_rb_decoder_array_enrich_v2);
}

/** Test if we can enrich with a 3-length array */
static void test_rb_array_enrich_v3() {
	test_rb_array_enrich0("v3",check_rb_decoder_array_enrich_v3);
}

/** Test if we can enrich with a 5-length array */
static void test_rb_array_enrich_v5() {
	test_rb_array_enrich0("jkl",check_rb_decoder_array_enrich);
}

/** Test if we can enrich over an array */
static void test_rb_array_enrich() {
		struct pair mem[3];
	keyval_list_t args;
	keyval_list_init(&args);
	prepare_args("rb_flow","ghi","127.0.0.1",mem,RD_ARRAYSIZE(mem),&args);

#define MESSAGES                                                              \
	X("{\"client_mac\": \"54:26:96:db:88:01\", "                              \
		"\"sensor_uuid\":\"ghi\", \"o\":[1,2,3,4,5,6],"                       \
		"\"application_name\": \"wwww\", "                                    \
		"\"u\":{\"a\":1}, \"a\":5}",                                          \
		check_rb_decoder_object_enrich)                                       \
	/* Free & Check that session has been freed */                            \
	X(NULL,check_null_session)

	struct message_in msgs[] = {
#define X(a,fn) {a,sizeof(a)-1},
		MESSAGES
#undef X
	};

	check_callback_fn callbacks_functions[] = {
#define X(a,fn) fn,
		MESSAGES
#undef X
	};

	test_rb_decoder0(CONFIG_TEST, &args, msgs, callbacks_functions,
		RD_ARRAYSIZE(msgs), NULL);

#undef MESSAGES
}


/** @TODO test matrix enrichment: test that all types can be overriden by all
    types
    */
int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test_validate_uri),
		cmocka_unit_test(test_rb_decoder_simple),
		cmocka_unit_test(test_rb_decoder_simple_def),
		cmocka_unit_test(test_rb_decoder_double),
		cmocka_unit_test(test_rb_decoder_half),
		cmocka_unit_test(test_rb_decoder_half_string),
		cmocka_unit_test(test_rb_decoder_half_key),
		cmocka_unit_test(test_rb_decoder_objects),
		cmocka_unit_test(test_rb_object_enrich),
		cmocka_unit_test(test_rb_array_enrich_v0),
		cmocka_unit_test(test_rb_array_enrich_v1),
		cmocka_unit_test(test_rb_array_enrich_v2),
		cmocka_unit_test(test_rb_array_enrich_v3),
		cmocka_unit_test(test_rb_array_enrich_v5),
		cmocka_unit_test(test_rb_array_enrich),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
