BIN=	n2kafka

SRCS=	engine.c global_config.c kafka.c n2kafka.c in_addr_list.c http.c socket.c
OBJS=	$(SRCS:.c=.o)

.PHONY:

all: $(BIN)

include mklove/Makefile.base

install: bin-install

clean: bin-clean

-include $(DEPS)
