#include "rb_mse_tests.h"

#include "rb_mem_tests.h"

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

static const time_t NOW = 1446650950;

static const char MSE10_ASSOC[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	            "\"notificationType\":\"association\","
	            "\"subscriptionName\":\"rb-assoc\","
	            "\"entity\":\"WIRELESS_CLIENTS\","
	            "\"deviceId\":\"00:ca:00:05:06:52\","
	            "\"lastSeen\":\"2015-02-24T08:41:50.026+0000\","
	            "\"ssid\":\"SHAW_SABER\","
	            "\"band\":\"IEEE_802_11_B\","
	            "\"apMacAddress\":\"00:ba:20:10:4f:00\","
	            "\"association\":true,"
	            "\"ipAddress\":["
	                "\"25.145.34.131\""
	            "],"
	            "\"status\":3,"
	            "\"username\":\"\","
	            "\"timestamp\":1446650950000"
	        "}"
	    "]"
	"}";
		// *INDENT-ON*

static const char MSE8_PROBING[] =
    // *INDENT-OFF*
    "{\"StreamingNotification\":{"
        "\"subscriptionName\":\"MSE_SanCarlos\","
        "\"entity\":\"WIRELESS_CLIENTS\","
        "\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
        "\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
        "\"floorRefId\":0,"
        "\"timestampMillis\":1446650950000,"
        "\"location\":{"
            "\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
            "\"currentlyTracked\":true,"
            "\"confidenceFactor\":88.0,"
            "\"userName\":\"\","
            "\"ssId\":\"\","
            "\"band\":\"UNKNOWN\","
            "\"apMacAddress\":\"\","
            "\"dot11Status\":\"PROBING\","
            "\"guestUser\":false,"
            "\"MapInfo\":{\"mapHierarchyString\":\"SantaClara Convention>Hyatt>HSC-FL8\","
            "\"floorRefId\":4698041283715793160,"
            "\"Dimension\":{\"length\":136.0,"
            "\"width\":408.0,"
            "\"height\":10.0,"
            "\"offsetX\":0.0,"
            "\"offsetY\":0.0,"
            "\"unit\":\"FEET\"},"
            "\"Image\":{\"imageName\":\"domain_0_1365721057045.png\"}"
        "},"
        "\"MapCoordinate\":{"
            "\"x\":116.18053,"
            "\"y\":85.699814,"
            "\"unit\":\"FEET\""
        "},"
        "\"Statistics\":{"
            "\"currentServerTime\":\"2015-03-16T01:57:50.000-0700\","
            "\"firstLocatedTime\":\"2015-03-16T01:57:49.991-0700\","
            "\"lastLocatedTime\":\"2015-03-16T01:57:49.991-0700\"}},"
            "\"timestamp\":\"2015-03-16T01:57:50.000-0700\""
        "}"
    "}";
    // *INDENT-ON*

static const char MSE8_DEVICE_ID_AS_INT[] =
    // *INDENT-OFF*
	"{\"StreamingNotification\":{"
		"\"subscriptionName\":\"MSE_SanCarlos\","
		"\"entity\":\"WIRELESS_CLIENTS\","
//		"\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
		"\"deviceId\":1111111111,"
		"\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
		"\"floorRefId\":0,"
		"\"timestampMillis\":1426496270000,"
		"\"location\":{"
			"\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
			"\"currentlyTracked\":true,"
			"\"confidenceFactor\":88.0,"
			"\"userName\":\"\","
			"\"ssId\":\"\","
			"\"band\":\"UNKNOWN\","
			"\"apMacAddress\":\"\","
			"\"dot11Status\":\"PROBING\","
			"\"guestUser\":false,"
			"\"MapInfo\":{\"mapHierarchyString\":\"SantaClara Convention>Hyatt>HSC-FL8\","
			"\"floorRefId\":4698041283715793160,"
			"\"Dimension\":{\"length\":136.0,"
			"\"width\":408.0,"
			"\"height\":10.0,"
			"\"offsetX\":0.0,"
			"\"offsetY\":0.0,"
			"\"unit\":\"FEET\"},"
			"\"Image\":{\"imageName\":\"domain_0_1365721057045.png\"}"
		"},"
		"\"MapCoordinate\":{"
			"\"x\":116.18053,"
			"\"y\":85.699814,"
			"\"unit\":\"FEET\""
		"},"
		"\"Statistics\":{"
			"\"currentServerTime\":\"2015-03-16T01:57:50.000-0700\","
			"\"firstLocatedTime\":\"2015-03-16T01:57:49.991-0700\","
			"\"lastLocatedTime\":\"2015-03-16T01:57:49.991-0700\"}},"
			"\"timestamp\":\"2015-03-16T01:57:50.000-0700\""
		"}"
	"}";

static const char MSE8_DEVICE_SHORT_AS_INT[] =
    // *INDENT-OFF*
	"{\"StreamingNotification\":{"
		"\"subscriptionName\":\"MSE_SanCarlos\","
		"\"entity\":\"WIRELESS_CLIENTS\","
		"\"deviceId\":\"cc:aa:aa:aa:aa:ab\","
		"\"mseUdi\":\"AIR-MSE-VA-K9:V01:MSE-VA-77_32af66dc-bb7b-11e3-9121-005056bd06d8\","
		"\"floorRefId\":0,"
		"\"timestampMillis\":1426496270000,"
		"\"location\":{"
			"\"macAddress\":\"cc:aa:aa:aa:aa\","
//			"\"macAddress\":\"cc:aa:aa:aa:aa:ab\","
			"\"currentlyTracked\":true,"
			"\"confidenceFactor\":88.0,"
			"\"userName\":\"\","
			"\"ssId\":\"\","
			"\"band\":\"UNKNOWN\","
			"\"apMacAddress\":\"\","
			"\"dot11Status\":\"PROBING\","
			"\"guestUser\":false,"
			"\"MapInfo\":{\"mapHierarchyString\":\"SantaClara Convention>Hyatt>HSC-FL8\","
			"\"floorRefId\":4698041283715793160,"
			"\"Dimension\":{\"length\":136.0,"
			"\"width\":408.0,"
			"\"height\":10.0,"
			"\"offsetX\":0.0,"
			"\"offsetY\":0.0,"
			"\"unit\":\"FEET\"},"
			"\"Image\":{\"imageName\":\"domain_0_1365721057045.png\"}"
		"},"
		"\"MapCoordinate\":{"
			"\"x\":116.18053,"
			"\"y\":85.699814,"
			"\"unit\":\"FEET\""
		"},"
		"\"Statistics\":{"
			"\"currentServerTime\":\"2015-03-16T01:57:50.000-0700\","
			"\"firstLocatedTime\":\"2015-03-16T01:57:49.991-0700\","
			"\"lastLocatedTime\":\"2015-03-16T01:57:49.991-0700\"}},"
			"\"timestamp\":\"2015-03-16T01:57:50.000-0700\""
		"}"
	"}";

static const char MSE10_2[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	        "}"
	    "]"
	"}";
static const char LISTENER_CONFIG[] = \
            "{\"enrichment\":{\"a\":\"b\"}}";


static const char LISTENER_NULL_CONFIG[] = "{}";
static const char MSE_NOT_ARRAY_IN[] = "{}";


static const char MSE_ARRAY_IN[] = \
	// *INDENT-OFF*
	"[\n" \
	"]";
	// *INDENT-ON*

static const char MSE_EMPTY[] = "";

static void mem_test(void (*cb)(const char *_listener_config),
						const char *_listener_config) {
	size_t i = 1;
	do {
		mem_wrap_fail_in = i++;
		cb(_listener_config);
	} while (0 == mem_wrap_fail_in);
	mem_wrap_fail_in = 0;
}

static const char ERROR_MAC_MSE10_ASSOC[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	            "\"notificationType\":\"association\","
	            "\"subscriptionName\":\"rb-assoc\","
	            "\"entity\":\"WIRELESS_CLIENTS\","
	            "\"deviceId\":\"00:ca:00:05:06\","
//	            "\"deviceId\":\"00:ca:00:05:06:52\","
	            "\"lastSeen\":\"2015-02-24T08:41:50.026+0000\","
	            "\"ssid\":\"SHAW_SABER\","
	            "\"band\":\"IEEE_802_11_B\","
	            "\"apMacAddress\":\"00:ba:20:10:4f:00\","
	            "\"association\":true,"
	            "\"ipAddress\":["
	                "\"25.145.34.131\""
	            "],"
	            "\"status\":3,"
	            "\"username\":\"\","
	            "\"timestamp\":1446650950000"
	        "}"
	    "]"
	"}";

static const char NOT_MSE10_ASSOC[] =
	// *INDENT-OFF*
	"{"
	    "\"notifications\":["
	        "{"
	            "\"notificationType\":\"association\","
	            "\"subscriptionName\":\"rb-assoc\","
	        "}"
	    "]"
	"}";


static const char MSE10_ASSOC_NO_JSON[] = "";

static const char MSE10_ASSOC_EMPTY_JSON[] = "{}";

static void mytest_mse10_not_json2(const char *buffer) {

	json_error_t err;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_non_null(json);

	struct mse_array *notifications = NULL;
	notifications = extract_mse_data(buffer, json);
	assert_null(notifications);

	free(notifications);
	json_decref(json);
}

static void mytest4(const char *buffer) {

	json_error_t err;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_null(json);

	struct mse_array *notifications = NULL;
	notifications = extract_mse_data(buffer, json);
	assert_null(notifications);

	free(notifications);
	json_decref(json);
}

static void mytest4_2(const char *buffer) {
	json_error_t err;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_non_null(json);

	struct mse_array *notifications = NULL;
	notifications = extract_mse_data(buffer, json);

	if (NULL != notifications)
		free(notifications);

	json_decref(json);

}

static void mytest8(const char *buffer) {
	json_error_t err;
	int i;
	json_t *json = json_loadb(buffer, strlen(buffer), 0, &err);
	assert_non_null(json);
	struct mse_array *mse_ar = NULL;
	mse_ar = extract_mse8_rich_data(json, &i);

	json_decref(json);
	if (NULL != mse_ar)
		free(mse_ar);
}

static void test8() {
	mem_test(mytest8, MSE8_DEVICE_ID_AS_INT);
}

static void test5() {
	mem_test(mytest4_2, ERROR_MAC_MSE10_ASSOC);
}

static void test4_empty_json() {
	mem_test(mytest_mse10_not_json2,MSE10_ASSOC_EMPTY_JSON);
}

static void test4_no_json() {
	mem_test(mytest4,MSE10_ASSOC_NO_JSON);
}

static void test4() {
	mem_test(mytest4,NOT_MSE10_ASSOC);
}

int main() {
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(test4),
		cmocka_unit_test(test5),
		cmocka_unit_test(test8),
		cmocka_unit_test(test4_no_json),
		cmocka_unit_test(test4_empty_json),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}
