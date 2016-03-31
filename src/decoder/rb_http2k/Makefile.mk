THIS_SRCS := \
	rb_http2k_decoder.c \
	rb_database.c \
	rb_http2k_parser.c

SRCS := $(SRCS) $(addprefix $(CURRENT_N2KAFKA_DIR),$(THIS_SRCS))
