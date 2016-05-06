THIS_SRCS := \
	rb_http2k_decoder.c \
	rb_database.c \
	uuid_database.c \
	rb_http2k_parser.c \
	rb_http2k_sensors_database.c \
	rb_http2k_sync_thread.c \
	rb_http2k_curl_handler.c \
	rb_http2k_organizations_database.c \
	tommyds/tommyhash.c \
	tommyds/tommyhashdyn.c \
	tommyds/tommylist.c \

SRCS := $(SRCS) $(addprefix $(CURRENT_N2KAFKA_DIR),$(THIS_SRCS))
