#include <curl/curl.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zlib.h>

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

#include <cmocka.h>

#define VALID_MESSAGE "{\"message\": \"Hello world\"}"
#define VALID_TOPIC "/rb_flow"
#define INVALID_TOPIC "/rb_nothing"
#define VALID_URL "http://localhost:2057/rbdata"
#define INVALID_URL "http://localhost:2057/invalid"
#define VALID_UUID "/abc"
#define INVALID_UUID "/abcdefg"

// It should returns 200 when UUID, TOPIC and MESSAGE are valids
static void test_valid_POST() {
  CURL *curl;
  CURLcode res;
  long http_code = 0;
  char *url = calloc(128, sizeof(char));

  curl = curl_easy_init();

  strcat(url, VALID_URL);
  strcat(url, VALID_UUID);
  strcat(url, VALID_TOPIC);

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, VALID_MESSAGE);

    res = curl_easy_perform(curl);

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    // Assertions
    assert_int_equal(res, 0);
    assert_true(http_code == 200);
  }

  free(url);
}

// It should returns 401 when UUID is not valid
static void test_invalid_UUID() {
  CURL *curl;
  CURLcode res;
  long http_code = 0;
  char *url = calloc(128, sizeof(char));

  curl = curl_easy_init();

  strcat(url, VALID_URL);
  strcat(url, INVALID_UUID);
  strcat(url, VALID_TOPIC);

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, VALID_MESSAGE);

    res = curl_easy_perform(curl);

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    // Assertions
    assert_int_equal(res, 0);
    assert_true(http_code == 401);
  }

  free(url);
}

// It should returns 403 when Topic is not valid
static void test_invalid_topic() {
  CURL *curl;
  CURLcode res;
  long http_code = 0;
  char *url = calloc(128, sizeof(char));

  curl = curl_easy_init();

  strcat(url, VALID_URL);
  strcat(url, VALID_UUID);
  strcat(url, INVALID_TOPIC);

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, VALID_MESSAGE);

    res = curl_easy_perform(curl);

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    // Assertions
    assert_int_equal(res, 0);
    assert_true(http_code == 403);
  }

  free(url);
}

static void test_no_topic() {
  CURL *curl;
  CURLcode res;
  long http_code = 0;
  char *url = calloc(128, sizeof(char));

  curl = curl_easy_init();

  strcat(url, VALID_URL);
  strcat(url, VALID_UUID);

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, VALID_MESSAGE);

    res = curl_easy_perform(curl);

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    // Assertions
    assert_int_equal(res, 0);
    assert_true(http_code == 400);
  }

  free(url);
}

static void test_empty_body() {
  CURL *curl;
  CURLcode res;
  long http_code = 0;
  char *url = calloc(128, sizeof(char));

  curl = curl_easy_init();

  strcat(url, VALID_URL);
  strcat(url, VALID_UUID);
  strcat(url, VALID_TOPIC);

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "");

    res = curl_easy_perform(curl);

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    // Assertions
    assert_int_equal(res, 0);
    assert_true(http_code == 200);
  }

  free(url);
}

// It should returns 403 when Topic is not valid
static void test_invalid_URL() {
  CURL *curl;
  CURLcode res;
  long http_code = 0;
  char *url = calloc(128, sizeof(char));

  curl = curl_easy_init();

  strcat(url, INVALID_URL);
  strcat(url, VALID_UUID);
  strcat(url, INVALID_TOPIC);

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, VALID_MESSAGE);

    res = curl_easy_perform(curl);

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    // Assertions
    assert_int_equal(res, 0);
    assert_true(http_code == 400);
  }

  free(url);
}

// It should returns 403 when Topic is not valid
static void test_deflate() {
  z_stream defstream;
  defstream.zalloc = Z_NULL;
  defstream.zfree = Z_NULL;
  defstream.opaque = Z_NULL;
  CURL *curl;
  CURLcode res;

  long http_code = 0;
  char *url = calloc(128, sizeof(char));

  char input[50] = VALID_MESSAGE;
  char output[50];

  defstream.avail_in =
      (uInt)strlen(input) + 1;        // size of input, string + terminator
  defstream.next_in = (Bytef *)input; // input char array
  defstream.avail_out = (uInt)sizeof(output); // size of output
  defstream.next_out = (Bytef *)output;       // output char array

  deflateInit(&defstream, Z_DEFAULT_COMPRESSION);
  deflate(&defstream, Z_FINISH);
  deflateEnd(&defstream);

  curl = curl_easy_init();

  strcat(url, VALID_URL);
  strcat(url, VALID_UUID);
  strcat(url, VALID_TOPIC);

  if (curl) {
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Content-Encoding: deflate");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, output);

    res = curl_easy_perform(curl);

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    // Assertions
    assert_int_equal(res, 0);
    assert_true(http_code == 200);
  }

  free(url);
}

int main() {
  curl_global_init(CURL_GLOBAL_ALL);

  printf("Initializing n2kafka\n");
  pid_t pID = fork();

  if (pID == 0) {
    // Close stdin, stdout, stderr
    close(0);
    close(1);
    close(2);
    open("/dev/null", O_RDWR);
    (void)(dup(0)+1);
    (void)(dup(0)+1);

    execlp("../n2kafka", "n2kafka",
           "../configs_example/n2kafka_config_rbhttp.json", (char *)0);
    printf("Error executing n2kafka\n");
    exit(1);

  } else if (pID < 0) {
    exit(1);
  }

  // Wait for n2kafka to initialize
  sleep(1);

  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_valid_POST),    cmocka_unit_test(test_invalid_UUID),
      cmocka_unit_test(test_invalid_topic), cmocka_unit_test(test_no_topic),
      cmocka_unit_test(test_invalid_URL),   cmocka_unit_test(test_deflate),
      cmocka_unit_test(test_empty_body)};
  const int res = cmocka_run_group_tests(tests, NULL, NULL);

  curl_global_cleanup();
  kill(pID, SIGINT);
  sleep(1);

  return res;
}
