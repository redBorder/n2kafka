SUBDIRS := \
	rb_http2k \
	mse \
	meraki

include $(addprefix $(CURRENT_N2KAFKA_DIR), $(addsuffix /Makefile.mk, $(SUBDIRS)))
SUBDIRS :=