#include "assertion_handler.c"

#include <curl/curl.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

#include <cmocka.h>

#include <librdkafka/rdkafka.h>

#define EXPECTED_MESSAGE                                                       \
  "{\"message\":\"Hello "                                                      \
  "world\",\"a\":1,\"b\":\"c\",\"d\":true,\"e\":null,\"a_org\":10}"
#define EXPECTED_MESSAGE_2                                                     \
  "{\"message\":\"Sayounara "                                                  \
  "baby\",\"a\":1,\"b\":\"c\",\"d\":true,\"e\":null,\"a_org\":10}"
#define SENT_MESSAGE "{\"message\": \"Hello world\"}"
#define SENT_MESSAGE_2 "{\"message\": \"Sayounara baby\"}"
#define N_MESSAGES_EXPECTED 2
#define VALID_UUID "/abc"
#define VALID_URL "http://localhost:2057/rbdata"
#define VALID_TOPIC "/rb_flow"

static rd_kafka_t *rk;

static void init_kafka() {
  // Kafka
  char errstr[512];
  char *brokers = "eugeniodev:9092";
  rd_kafka_topic_partition_list_t *topics;
  rd_kafka_resp_err_t err;
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();

  // Set group id
  if (rd_kafka_conf_set(conf, "group.id", "tester", errstr, sizeof(errstr)) !=
      RD_KAFKA_CONF_OK) {
    fprintf(stderr, "%% %s\n", errstr);
    exit(1);
  }

  // Version fallback. Needed for newer brokers
  // if (rd_kafka_conf_set(conf, "broker.version", "0.8.2", errstr,
  //                       sizeof(errstr)) != RD_KAFKA_CONF_OK) {
  //   fprintf(stderr, "%% %s\n", errstr);
  //   exit(1);
  // }

  // Use broker to store offset
  if (rd_kafka_topic_conf_set(topic_conf, "offset.store.method", "broker",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "%% %s\n", errstr);
    exit(1);
  }

  rd_kafka_conf_set_default_topic_conf(conf, topic_conf);

  // Create Kafka handle
  if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr)))) {
    fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
    exit(1);
  }

  // Add brokers
  if (rd_kafka_brokers_add(rk, brokers) == 0) {
    fprintf(stderr, "%% No valid brokers specified\n");
    exit(1);
  }

  // Redirect rd_kafka_poll() to consumer_poll()
  rd_kafka_poll_set_consumer(rk);

  // Topic list
  topics = rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(topics, "rb_flow", 0);

  // Assign partitions
  if ((err = rd_kafka_assign(rk, topics))) {
    fprintf(stderr, "%% Failed to assign partitions: %s\n",
            rd_kafka_err2str(err));
  }

  rd_kafka_topic_partition_list_destroy(topics);
}

/**
 * Send a message using curl and expect to receive the enriched message via
 * kafka
 */
static void test_send_message_http() {
  // Init curl
  CURL *curl;
  CURLcode res;
  long http_code = 0;
  curl = curl_easy_init();

  // Build the URL
  char *url = calloc(128, sizeof(char));
  strcat(url, VALID_URL);
  strcat(url, VALID_UUID);
  strcat(url, VALID_TOPIC);

  // Use the assertion_handler to proccess assertions asynchronously
  struct assertion_handler_s *assertion_handler = NULL;
  assertion_handler = assertion_handler_new();

  if (curl) {
    // Push an assertion to the assertion handler with the expected message
    struct assertion_e *assertion =
        (struct assertion_e *)malloc(sizeof(struct assertion_e));
    assertion->str = strdup(EXPECTED_MESSAGE);
    assertion_handler_push_assertion(assertion_handler, assertion);

    struct assertion_e *assertion2 =
        (struct assertion_e *)malloc(sizeof(struct assertion_e));
    assertion2->str = strdup(EXPECTED_MESSAGE_2);
    assertion_handler_push_assertion(assertion_handler, assertion2);

    // Send message via curl
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, SENT_MESSAGE);
    res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    // Assertions
    assert_int_equal(res, 0);
    assert_true(http_code == 200);

    // Send message 2 via curl
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, SENT_MESSAGE_2);
    res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    // Assertions
    assert_int_equal(res, 0);
    assert_true(http_code == 200);

    curl_easy_cleanup(curl);
  }

  free(url);

  // Try to read the message from kafka and push it to the assertion handler
  int n_messages = 0;
  while (n_messages < N_MESSAGES_EXPECTED) {
    rd_kafka_message_t *rkmessage;
    struct value_e *value;

    rkmessage = rd_kafka_consumer_poll(rk, 1000);
    if (rkmessage->len > 0) {
      n_messages++;
      value = (struct value_e *)malloc(sizeof(struct value_e));
      value->str = malloc((rkmessage->len + 1) * sizeof(char));
      memmove(value->str, rkmessage->payload, rkmessage->len);
      value->str[rkmessage->len] = '\0';
      value->len = rkmessage->len;
      rd_kafka_message_destroy(rkmessage);
      assertion_handler_push_value(assertion_handler, value);
    }
  }

  // Assert pending messages
  assert_int_equal(assertion_handler_assert(assertion_handler), 0);
  assertion_handler_destroy(assertion_handler);
}

int main() {
  init_kafka();
  curl_global_init(CURL_GLOBAL_ALL);

  // Fork n2kafka
  printf("Initializing n2kafka\n");
  pid_t pID = fork();

  if (pID == 0) {
    // Close stdin, stdout, stderr
    close(0);
    close(1);
    close(2);
    open("/dev/null", O_RDWR);
    (void)(dup(0) + 1);
    (void)(dup(0) + 1);

    execlp("../n2kafka", "n2kafka",
           "../configs_example/n2kafka_config_rbhttp.json", (char *)0);
    printf("Error executing n2kafka\n");
    exit(1);

  } else if (pID < 0) {
    exit(1);
  }

  // Wait for n2kafka to initialize
  sleep(1);

  const struct CMUnitTest tests[] = {cmocka_unit_test(test_send_message_http)};
  const int res = cmocka_run_group_tests(tests, NULL, NULL);

  kill(pID, SIGINT);
  sleep(1);

  rd_kafka_consumer_close(rk);
  rd_kafka_destroy(rk);
  curl_global_cleanup();
  sleep(1);

  return res;
}
