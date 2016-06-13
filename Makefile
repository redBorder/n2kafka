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

coverage: all
	@( if [[ "x$(WITH_COVERAGE)" == "xn" ]]; \
	then echo "$(MKL_RED) You need to configure using --enable-coverage"; false; \
	else (cd tests && make coverage); fi)

install: bin-install

clean: bin-clean

tests:
	cd tests; make

-include $(DEPS)
