THIS_SRCS := \
	engine.c \
	global_config.c \
	n2kafka.c \
	rb_addr.c \

SRCS += $(addprefix $(CURRENT_N2KAFKA_DIR), $(THIS_SRCS))

THIS_SRCS :=
