THIS_SRCS := \
	http.c \
	socket.c \

SRCS += $(addprefix $(CURRENT_N2KAFKA_DIR), $(THIS_SRCS))

THIS_SRCS :=