#include "assertion_handler.h"

#include <assert.h>
#include <jansson.h>

struct assertion_handler_s {
  TAILQ_HEAD(, assertion_e) assertion_q;
  TAILQ_HEAD(, value_e) value_q;
  pthread_mutex_t assertion_mutex;
  pthread_mutex_t value_mutex;
};

/**
 * Creates a new assertion_handler
 * @return assertion_handler The new assertion handler
 */
struct assertion_handler_s *assertion_handler_new() {
  struct assertion_handler_s *assertion_handler =
      (struct assertion_handler_s *)malloc(sizeof(struct assertion_handler_s));
  TAILQ_INIT(&assertion_handler->assertion_q);
  TAILQ_INIT(&assertion_handler->value_q);

  if ((pthread_mutex_init(&assertion_handler->assertion_mutex, NULL) ||
       pthread_mutex_init(&assertion_handler->value_mutex, NULL)) != 0) {
    printf("\n mutex init failed\n");
  }

  return assertion_handler;
}

/**
 * Inserts an assertion at the end of the assertion queue
 * @param assertion_handler assertion_handler_s to add the assertion
 * @param assertion         New assertion to add
 */
void assertion_handler_push_assertion(
    struct assertion_handler_s *assertion_handler,
    struct assertion_e *assertion) {

  pthread_mutex_lock(&assertion_handler->assertion_mutex);
  TAILQ_INSERT_TAIL(&assertion_handler->assertion_q, assertion, tailq);
  pthread_mutex_unlock(&assertion_handler->assertion_mutex);
}

/**
 * Inserts a new value at the end of the value queue
 * @param assertion_handler assertion_handler_s to add the assertion
 * @param value             New value to add
 */
void assertion_handler_push_value(struct assertion_handler_s *assertion_handler,
                                  struct value_e *value) {

  pthread_mutex_lock(&assertion_handler->value_mutex);
  TAILQ_INSERT_TAIL(&assertion_handler->value_q, value, tailq);
  pthread_mutex_unlock(&assertion_handler->value_mutex);
}

/**
 * Iterates the assertions and check for matchs between assertions and values
 * @param assertion_handler assertion_handler_s to use
 */
int assertion_handler_assert(struct assertion_handler_s *assertion_handler) {
  uint found = 0;
  int err = 0;

  pthread_mutex_lock(&assertion_handler->value_mutex);
  pthread_mutex_lock(&assertion_handler->assertion_mutex);

  struct assertion_e *assertion;
  TAILQ_FOREACH(assertion, &assertion_handler->assertion_q, tailq) {
    json_error_t assertion_jerr;

    char *assertion_message, *assertion_b = NULL;
    json_int_t assertion_a, assertion_a_org;
    int assertion_d;

    struct value_e *value;

    json_t *assertion_json = json_loads(assertion->str, 0, &assertion_jerr);
    json_unpack_ex(assertion_json, &assertion_jerr, JSON_STRICT,
                   "{s:s,s:i,s:s,s:b,s:n,s:i}", "message", &assertion_message,
                   "a", &assertion_a, "b", &assertion_b, "d", &assertion_d, "e",
                   "a_org", &assertion_a_org);

    found = 0;

    TAILQ_FOREACH(value, &assertion_handler->value_q, tailq) {
      json_error_t value_jerr;

      char *value_message, *value_b = NULL;
      json_int_t value_a, value_a_org;
      int value_d;

      json_t *value_json = json_loads(value->str, 0, &value_jerr);
      json_unpack_ex(value_json, &value_jerr, JSON_STRICT,
                     "{s:s,s:i,s:s,s:b,s:n,s:i}", "message", &value_message,
                     "a", &value_a, "b", &value_b, "d", &value_d, "e", "a_org",
                     &value_a_org);

      // // Message
      // printf("%s:%s -> %d\n", assertion_message, value_message,
      //        strcmp(assertion_message, value_message) == 0);
      //
      // // a
      // printf("%d:%d -> %d \n", (int)assertion_a, (int)value_a,
      //        (int)assertion_a == (int)value_a);
      // // b
      // printf("%s:%s -> %d \n", assertion_b, value_b,
      //        strcmp(assertion_b, value_b) == 0);
      //
      // // d
      // printf("%d:%d -> %d\n", assertion_d, value_d, assertion_d == value_d);
      //
      // // a_org
      // printf("%d:%d -> %d\n", (int)assertion_a_org, (int)value_a_org,
      //        (int)assertion_a_org == (int)value_a_org);

      if (strcmp(assertion_message, value_message) == 0 &&
          (int)assertion_a == (int)value_a &&
          strcmp(assertion_b, value_b) == 0 &&
          (int)assertion_d == (int)value_d &&
          (int)assertion_a_org == (int)value_a_org) {
        found++;
        json_decref(value_json);
        break;
      }

      json_decref(value_json);
    }

    json_decref(assertion_json);

    if (!found) {
      err++;
      break;
    }
  }

  pthread_mutex_unlock(&assertion_handler->assertion_mutex);
  pthread_mutex_unlock(&assertion_handler->value_mutex);

  return err;
}

void assertion_handler_destroy(struct assertion_handler_s *assertion_handler) {
  pthread_mutex_lock(&assertion_handler->value_mutex);
  pthread_mutex_lock(&assertion_handler->assertion_mutex);

  struct assertion_e *assertion = NULL;
  while (!TAILQ_EMPTY(&assertion_handler->assertion_q)) {
    assertion = TAILQ_FIRST(&assertion_handler->assertion_q);
    TAILQ_REMOVE(&assertion_handler->assertion_q, assertion, tailq);
    free(assertion->str);
    free(assertion);
  }

  struct value_e *value;
  while (!TAILQ_EMPTY(&assertion_handler->value_q)) {
    value = TAILQ_FIRST(&assertion_handler->value_q);
    TAILQ_REMOVE(&assertion_handler->value_q, value, tailq);
    free(value->str);
    free(value);
  }

  pthread_mutex_unlock(&assertion_handler->assertion_mutex);
  pthread_mutex_unlock(&assertion_handler->value_mutex);

  pthread_mutex_destroy(&assertion_handler->value_mutex);
  pthread_mutex_destroy(&assertion_handler->assertion_mutex);

  free(assertion_handler);
}
