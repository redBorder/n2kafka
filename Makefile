BIN=	n2kafka

SRCS=   version.c

OBJS=	$(SRCS:.c=.o)
all: $(BIN)

CURRENT_N2KAFKA_DIR = $(dir $(lastword $(MAKEFILE_LIST)))

include src/Makefile.mk
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
