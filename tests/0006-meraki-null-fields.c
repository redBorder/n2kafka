#include "rb_meraki_tests.h"

#include <setjmp.h>
#include <cmocka.h>

// these message have some fields null, meraki decoder should throw them
// away
static const char MERAKI_MSG[] =
	"{"
	    "\"secret\": \"redborder\","
	    "\"version\": \"2.0\","
	    "\"type\": \"DevicesSeen\","
	    "\"data\": {"
	        "\"apMac\": \"00:a0:f9:37:a0:d1\","
	        "\"apFloors\": [],"
	        "\"observations\": [{"
	            "\"clientMac\": \"00:00:5f:f1:cd:8d\","
	            "\"ipv4\": null,"
	            "\"ipv6\": null,"
	            "\"seenTime\": \"2016-03-01T14:57:49Z\","
	            "\"seenEpoch\": 1456844269,"
	            "\"ssid\": null,"
	            "\"rssi\": 34,"
	            "\"manufacturer\": \"SamsungE\","
	            "\"os\": null,"
	            "\"location\": null"
	        "}, {"
	            "\"clientMac\": \"00:00:c5:46:53:10\","
	            "\"ipv4\": null,"
	            "\"ipv6\": null,"
	            "\"seenTime\": \"2016-03-01T14:57:51Z\","
	            "\"seenEpoch\": 1456844271,"
	            "\"ssid\": null,"
	            "\"rssi\": 14,"
	            "\"manufacturer\": \"HonHaiPr\","
	            "\"os\": null,"
	            "\"location\": null"
	        "}, {"
	            "\"clientMac\": \"00:00:51:1f:53:b0\","
	            "\"ipv4\": null,"
	            "\"ipv6\": null,"
	            "\"seenTime\": \"2016-03-01T14:57:52Z\","
	            "\"seenEpoch\": 1456844272,"
	            "\"ssid\": null,"
	            "\"rssi\": 14,"
	            "\"manufacturer\": \"LiteonTe\","
	            "\"os\": null,"
	            "\"location\": null"
	        "}]"
	    "}"
	"}";
  // *INDENT-ON*

/** Location is not NULL or valid object, so output message should not contains
	location
	*/
static const char MERAKI_MSG_BAD_FORMAT_LOCATION[] =
	"{"
	    "\"secret\": \"redborder\","
	    "\"version\": \"2.0\","
	    "\"type\": \"DevicesSeen\","
	    "\"data\": {"
	        "\"apMac\": \"00:a0:f9:37:a0:d1\","
	        "\"apFloors\": [],"
	        "\"observations\": [{"
	            "\"clientMac\": \"00:00:5f:f1:cd:8d\","
	            "\"ipv4\": null,"
	            "\"ipv6\": null,"
	            "\"seenTime\": \"2016-03-01T14:57:49Z\","
	            "\"seenEpoch\": 1456844269,"
	            "\"ssid\": null,"
	            "\"rssi\": 34,"
	            "\"manufacturer\": \"SamsungE\","
	            "\"os\": null,"
	            "\"location\": \"abc\""
	        "}, {"
	            "\"clientMac\": \"00:00:c5:46:53:10\","
	            "\"ipv4\": null,"
	            "\"ipv6\": null,"
	            "\"seenTime\": \"2016-03-01T14:57:51Z\","
	            "\"seenEpoch\": 1456844271,"
	            "\"ssid\": null,"
	            "\"rssi\": 14,"
	            "\"manufacturer\": \"HonHaiPr\","
	            "\"os\": null,"
	            "\"location\": 6"
	        "}, {"
	            "\"clientMac\": \"00:00:51:1f:53:b0\","
	            "\"ipv4\": null,"
	            "\"ipv6\": null,"
	            "\"seenTime\": \"2016-03-01T14:57:52Z\","
	            "\"seenEpoch\": 1456844272,"
	            "\"ssid\": null,"
	            "\"rssi\": 14,"
	            "\"manufacturer\": \"LiteonTe\","
	            "\"os\": null,"
	            "\"location\": true"
	        "}]"
	    "}"
	"}";
  // *INDENT-ON*

static const char MERAKI_SECRETS[] = \
  // *INDENT-OFF*
	"{"
		/* "\"meraki-secrets\": {" */
	        "\"redborder\": { "
	          "\"sensor_name\": \"meraki1\" "
	          ", \"sensor_id\": 2"
	        "},"
	        "\"redborder2\": { "
	          "\"sensor_name\": \"meraki2\" "
	          ", \"sensor_id\": 3"
	        "}"
	    /* "}" */
	"}";
  // *INDENT-ON*

CHECKDATA(check1,
    {.key = "type", .value = "meraki"},
    {.key = "client_mac", .value="00:00:5f:f1:cd:8d"},
    {.key = "client_mac_vendor", .value = "SamsungE"},
    {.key = "src", .value = NULL},
    {.key = "wireless_id", .value = NULL},
    {.key = "client_rssi_num", .value ="-61"},
    {.key = "os", .value = NULL},
    {.key = "location", .value = NULL},
);

CHECKDATA(check2,
    {.key = "type", .value = "meraki"},
    {.key = "client_mac", .value="00:00:c5:46:53:10"},
    {.key = "client_mac_vendor", .value = "HonHaiPr"},
    {.key = "src", .value = NULL},
    {.key = "wireless_id", .value = NULL},
    {.key = "client_rssi_num", .value ="-81"},
    {.key = "os", .value = NULL},
    {.key = "location", .value = NULL},
);

CHECKDATA(check3,
    {.key = "type", .value = "meraki"},
    {.key = "client_mac", .value="00:00:51:1f:53:b0"},
    {.key = "client_mac_vendor", .value = "LiteonTe"},
    {.key = "src", .value = NULL},
    {.key = "wireless_id", .value = NULL},
    {.key = "client_rssi_num", .value ="-81"},
    {.key = "os", .value = NULL},
    {.key = "location", .value = NULL},
);

static void MerakiDecoder_nulls() {
	static const struct checkdata *checkdata_array[] = {
		&check1, &check2, &check3
	};

	static const struct checkdata_array checkdata = {
		.checks = checkdata_array,
		.size = sizeof(checkdata_array) / sizeof(checkdata_array[0]),
	};

	MerakiDecoder_test_base(NULL, MERAKI_SECRETS,
		MERAKI_MSG, &checkdata);
}

static void MerakiDecoder_bad_location() {
	static const struct checkdata *checkdata_array[] = {
		&check1, &check2, &check3
	};

	static const struct checkdata_array checkdata = {
		.checks = checkdata_array,
		.size = sizeof(checkdata_array) / sizeof(checkdata_array[0]),
	};

	MerakiDecoder_test_base(NULL, MERAKI_SECRETS,
		MERAKI_MSG_BAD_FORMAT_LOCATION, &checkdata);
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(MerakiDecoder_nulls),
		cmocka_unit_test(MerakiDecoder_bad_location),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);

	return 0;
}
