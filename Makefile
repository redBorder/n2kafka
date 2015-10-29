BIN=	n2kafka

SRCS=	src/decoder/rb_http2k_decoder.c src/decoder/rb_meraki.c \
	src/decoder/rb_mse.c src/engine/engine.c \
	src/engine/global_config.c src/engine/n2kafka.c \
	src/util/pair.c \
	src/engine/rb_addr.c src/listener/http.c \
	src/listener/socket.c src/util/in_addr_list.c \
	src/util/kafka.c src/util/rb_json.c src/util/rb_mac.c
OBJS=	$(SRCS:.c=.o)

.PHONY:

all: $(BIN)

CFLAGS+=-I. -I./src/decoder -I./src/engine -I./src/util -I./src/listener

include mklove/Makefile.base

.PHONY: version.c

version.c: 
	@rm -f $@
	@echo "const char *n2kafka_revision=\"`git describe --abbrev=6 --dirty --tags --always`\";" >> $@
	@echo 'const char *n2kafka_version="1.0.0";' >> $@

install: bin-install

clean: bin-clean

-include $(DEPS)
