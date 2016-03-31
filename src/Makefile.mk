SUBDIRS := \
	decoder \
	engine \
	listener \
	util \

include $(addprefix $(CURRENT_N2KAFKA_DIR), $(addsuffix /Makefile.mk,$(SUBDIRS)))

SUBDIRS :=