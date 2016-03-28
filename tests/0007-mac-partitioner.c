#include "rb_json_tests.c"
#include <librdkafka/rdkafka.h>

#include "../src/decoder/rb_http2k/rb_http2k_decoder.c"
#include "../src/listener/http.c"

#include <assert.h>
#include <setjmp.h>
#include <cmocka.h>

static const int32_t default_partition_cnt = 4;

struct test_mac {
	const char mac[sizeof("00:00:00:00:00:00")];
	int32_t idx;
};

static const struct test_mac test_mac[] = {
	{.mac = "d2:65:2f:83:68:cd", .idx = 1},
	{.mac = "97:0f:d4:6e:92:05", .idx = 1},
	{.mac = "f8:43:aa:3b:e0:ce", .idx = 2},
	{.mac = "de:fb:64:43:65:ae", .idx = 2},
	{.mac = "ab:e4:94:bc:61:78", .idx = 0},
	{.mac = "4a:7e:b9:9c:3b:89", .idx = 1},
	{.mac = "00:1c:bf:45:7a:68", .idx = 0},
	{.mac = "c2:cb:7c:3a:9b:a9", .idx = 1},
	{.mac = "2c:77:58:60:dd:ce", .idx = 2},
	{.mac = "05:2e:4c:41:84:05", .idx = 1},
	{.mac = "6a:76:92:dc:84:c6", .idx = 2},
	{.mac = "fe:12:91:75:19:b8", .idx = 0},
	{.mac = "05:85:19:42:15:cb", .idx = 3},
	{.mac = "67:d0:9a:59:9f:5e", .idx = 2},
	{.mac = "97:70:ba:00:23:a6", .idx = 2},
	{.mac = "5c:4d:ee:b5:23:6e", .idx = 2},
	{.mac = "46:c1:04:97:f5:ba", .idx = 2},
	{.mac = "f7:1f:cd:1b:a9:82", .idx = 2},
	{.mac = "cd:0b:b6:b8:92:a0", .idx = 0},
	{.mac = "48:a3:c3:25:da:aa", .idx = 2},
	{.mac = "90:a3:b5:ef:e9:d1", .idx = 1},
	{.mac = "d9:43:06:a8:f2:43", .idx = 3},
	{.mac = "2c:3f:e6:35:bf:b0", .idx = 0},
	{.mac = "32:dc:d4:4d:65:da", .idx = 2},
	{.mac = "3a:15:85:35:4c:78", .idx = 0},
	{.mac = "49:80:d6:34:e5:da", .idx = 2},
	{.mac = "89:0b:35:43:a2:53", .idx = 3},
	{.mac = "78:ec:60:96:d4:e2", .idx = 2},
	{.mac = "c3:ce:a8:7d:89:3c", .idx = 0},
	{.mac = "83:e1:bc:5d:cd:73", .idx = 3},
};

static int32_t mac_partitioner (const rd_kafka_topic_t *rkt,
                                const void *keydata, size_t keylen,
                                int32_t partition_cnt,
                                void *rkt_opaque, void *msg_opaque);

/// @note HOOK to know that mac_partitioner could not find a valid MAC
int32_t rd_kafka_msg_partitioner_random(const rd_kafka_topic_t *rkt,
                                const void *keydata, size_t keylen,
                                int32_t partition_cnt,
                                void *rkt_opaque, void *msg_opaque) {
	(void)rkt;
	(void)keydata;
	(void)keylen;
	(void)partition_cnt;
	(void)rkt_opaque;
	(void)msg_opaque;
	return 100;
}

static void samples_test() {
	const rd_kafka_topic_t *rkt = NULL;
	void *rkt_opaque = NULL, *msg_opaque = NULL;

	size_t i;
	for(i=0; i<sizeof(test_mac)/sizeof(test_mac[0]); ++i) {
		int32_t result = mac_partitioner(rkt,
		                test_mac[i].mac, strlen(test_mac[i].mac),
		                default_partition_cnt,
		                rkt_opaque, msg_opaque);

		assert(result == test_mac[i].idx);
	}
}

/// Tests all mac lengths between 0 and mac_length+1
static void invalid_mac_len_test() {
	size_t i;
	const rd_kafka_topic_t *rkt = NULL;
	void *rkt_opaque = NULL, *msg_opaque = NULL;
	char aux_mac[sizeof(test_mac[0].mac) + 1];
	const size_t mac_len = strlen(test_mac[0].mac);

	/// Test all sizes under right size
	for (i = 0; i <= mac_len + 1; ++i) {
		if (i == mac_len) {
			continue;
		}

		aux_mac[0] = '\0';
		strncat(aux_mac,test_mac[0].mac,i);

		if (i > mac_len) {
			aux_mac[mac_len] = '0';
			aux_mac[i] = '\0';
		}

		const int32_t result = mac_partitioner(rkt,
		                aux_mac, strlen(aux_mac),
		                default_partition_cnt,
		                rkt_opaque, msg_opaque);

		assert(100 == result);
	}
}

/// Test introducing weird characters in all position of the mac
static void invalid_mac_character() {
	size_t i,c;
	const rd_kafka_topic_t *rkt = NULL;
	void *rkt_opaque = NULL, *msg_opaque = NULL;
	char aux_mac[sizeof(test_mac[0].mac)];
	aux_mac[0] = '\0';
	strncat(aux_mac,test_mac[0].mac,sizeof(aux_mac));

	const char buggy_chars[] = {'g', '?', '\0', ':'};

	for (c = 0; c < sizeof(buggy_chars)/sizeof(buggy_chars[0]); ++c) {
		for (i=0; i<strlen(aux_mac); ++i) {
			if (aux_mac[i] == buggy_chars[c]) {
				/* Already the same, this will not raise error*/
				continue;
			}
			char aux = aux_mac[i];
			aux_mac[i] = buggy_chars[c];

			const int32_t result = mac_partitioner(rkt,
			                aux_mac, strlen(aux_mac),
			                default_partition_cnt,
			                rkt_opaque, msg_opaque);

			aux_mac[i] = aux;

			assert(100 == result);
		}
	}
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(samples_test),
		cmocka_unit_test(invalid_mac_len_test),
		cmocka_unit_test(invalid_mac_character),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
