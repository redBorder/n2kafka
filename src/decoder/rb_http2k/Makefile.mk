THIS_SRCS := \
	rb_http2k_decoder.c \
	rb_database.c \
	rb_http2k_parser.c \
	rb_http2k_sensors_database.c \
	tommyds/tommyhash.c \
	tommyds/tommyhashtbl.c \
	tommyds/tommylist.c \

SRCS := $(SRCS) $(addprefix $(CURRENT_N2KAFKA_DIR),$(THIS_SRCS))
