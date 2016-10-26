BIN=	n2kafka

SRCS=   version.c
OBJS = $(SRCS:.c=.o)
TESTS_C = $(sort $(wildcard tests/0*.c))

TESTS = $(TESTS_C:.c=.test)
TESTS_OBJS = $(TESTS:.test=.o)
TESTS_CHECKS_XML = $(TESTS_C:.c=.xml)
TESTS_MEM_XML = $(TESTS_C:.c=.mem.xml)
TESTS_HELGRIND_XML = $(TESTS_C:.c=.helgrind.xml)
TESTS_DRD_XML = $(TESTS_C:.c=.drd.xml)
TESTS_VALGRIND_XML = $(TESTS_MEM_XML) $(TESTS_HELGRIND_XML) $(TESTS_DRD_XML)
TESTS_XML = $(TESTS_CHECKS_XML) $(TESTS_VALGRIND_XML)

all: $(BIN)

CURRENT_N2KAFKA_DIR = $(dir $(lastword $(MAKEFILE_LIST)))

include src/Makefile.mk
include mklove/Makefile.base

.PHONY: version.c tests coverage

version.c:
	@rm -f $@
	@echo "const char *n2kafka_revision=\"`git describe --abbrev=6 --dirty --tags --always`\";" >> $@
	@echo 'const char *n2kafka_version="1.0.0";' >> $@

install: bin-install

clean: bin-clean
	rm -f $(TESTS) $(TESTS_OBJS) $(TESTS_XML) $(COV_FILES)

COV_FILES = $(foreach ext,gcda gcno, $(SRCS:.c=.$(ext)) $(TESTS_C:.c=.$(ext)))

VALGRIND ?= valgrind
SUPPRESSIONS_FILE ?= tests/valgrind.suppressions
ifneq ($(wildcard $(SUPPRESSIONS_FILE)),)
SUPPRESSIONS_VALGRIND_ARG = --suppressions=$(SUPPRESSIONS_FILE)
endif

.PHONY: tests checks memchecks drdchecks helchecks coverage check_coverage

run_tests = tests/run_tests.sh $(1) $(TESTS_C:.c=)
run_valgrind = $(VALGRIND) --tool=$(1) $(SUPPRESSIONS_VALGRIND_ARG) --xml=yes \
					--xml-file=$(2) $(3) >/dev/null 2>&1

setup-tests:
	docker network create --subnet=172.26.0.0/24 test
	docker run -d --net test --ip 172.26.0.2 --name zookeeper wurstmeister/zookeeper
	docker run -d --net test --ip 172.26.0.3 --name kafka -e KAFKA_ADVERTISED_HOST_NAME="172.26.0.3" -e KAFKA_ADVERTISED_PORT="9092" -e KAFKA_ZOOKEEPER_CONNECT="172.26.0.2:2181" -v /var/run/docker.sock:/var/run/docker.sock wurstmeister/kafka

teardown-tests:
	docker rm -f zookeeper kafka
	docker network rm test

tests: $(TESTS_XML)
	@$(call run_tests, -cvdh)

checks: $(TESTS_CHECKS_XML)
	@$(call run_tests,-c)

memchecks: $(TESTS_VALGRIND_XML)
	@$(call run_tests,-v)

drdchecks: $(TESTS_DRD_XML)
	@$(call run_tests,-d)

helchecks: $(TESTS_HELGRIND_XML)
	@$(call run_tests,-h)

tests/%.mem.xml: tests/%.test
	@echo -e '\033[1;34m[Checking memory ]\033[0m\t $<'
	-@$(call run_valgrind,memcheck,"$@","./$<")

tests/%.helgrind.xml: tests/%.test
	@echo -e '\033[1;34m[Checking concurrency with HELGRIND]\033[0m\t $<'
	-@$(call run_valgrind,helgrind,"$@","./$<")

tests/%.drd.xml: tests/%.test
	@echo -e '\033[1;34m[Checking concurrency with DRD]\033[0m\t $<'
	-@$(call run_valgrind,drd,"$@","./$<")

tests/%.xml: tests/%.test
	@echo -e '\033[1;34m[Testing ]\033[0m\t $<'
	@CMOCKA_XML_FILE="$@" CMOCKA_MESSAGE_OUTPUT=XML "./$<" >/dev/null 2>&1

tests/%.test: CPPFLAGS := -I. $(CPPFLAGS)
tests/%.test: tests/%.o $(filter-out src/engine/n2kafka.o,$(OBJS))
	@echo -e '\033[1;32m[Building]\033[0m\t $@'
	@$(CC) $(CPPFLAGS) $(LDFLAGS) $< $(shell cat $(@:.test=.objdeps)) -o $@ $(LIBS) -lcmocka >/dev/null 2>&1

check_coverage:
	@( if [[ "x$(WITH_COVERAGE)" == "xn" ]]; then \
	echo -e "$(MKL_RED) You need to configure using --enable-coverage"; \
	echo -n "$(MKL_CLR_RESET)"; \
	false; \
	fi)

COVERAGE_INFO ?= coverage.info
COVERAGE_OUTPUT_DIRECTORY ?= coverage.out.html
COV_VALGRIND ?= valgrind
COV_GCOV ?= gcov
COV_LCOV ?= lcov

coverage: check_coverage $(TESTS)
	( for test in $(TESTS); do ./$$test; done )
	$(COV_LCOV) --gcov-tool=$(COV_GCOV) -q \
                --rc lcov_branch_coverage=1 --capture \
                --directory ./ --output-file ${COVERAGE_INFO}
	genhtml --branch-coverage ${COVERAGE_INFO} --output-directory \
				${COVERAGE_OUTPUT_DIRECTORY} > coverage.out
	# ./display_coverage.sh

-include $(DEPS)
