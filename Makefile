BIN=	n2kafka

SRCS=	src/decoder/rb_http2k/rb_http2k_decoder.c \
	src/decoder/rb_http2k/rb_database.c \
	src/decoder/rb_http2k/rb_http2k_parser.c \
	src/decoder/meraki/rb_meraki.c \
	src/decoder/mse/rb_mse.c src/engine/engine.c \
	src/engine/global_config.c src/engine/n2kafka.c \
	src/util/pair.c \
	src/engine/rb_addr.c src/listener/http.c \
	src/listener/socket.c src/util/in_addr_list.c \
	src/util/kafka.c src/util/rb_json.c src/util/rb_mac.c \
	src/util/topic_database.c src/util/kafka_message_list.c
OBJS=	$(SRCS:.c=.o)

.PHONY:

all: $(BIN)

CFLAGS+=-I. -I./src/decoder -I./src/engine -I./src/util -I./src/listener

include mklove/Makefile.base

.PHONY: version.c tests coverage

version.c: 
	@rm -f $@
	@echo "const char *n2kafka_revision=\"`git describe --abbrev=6 --dirty --tags --always`\";" >> $@
	@echo 'const char *n2kafka_version="1.0.0";' >> $@

coverage: Makefile_config_bak := $(shell mktemp)
coverage: version.o
	# Need to disable optimizations
	@cp Makefile.config ${Makefile_config_bak}
	@sed -i 's%\-O[1-9s]%\-O0%g' Makefile.config
	-(CPPFLAGS='--coverage' LDFLAGS='--coverage' make && cd tests && make coverage)
	@cp ${Makefile_config_bak} Makefile.config
	@-rm ${Makefile_config_bak}

install: bin-install

clean: bin-clean

tests:
	cd tests; make

-include $(DEPS)
