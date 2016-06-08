#include "assertion_handler.h"

/**
 * Queue for store assertions
 */
struct assertion_e {
  TAILQ_ENTRY(assertion_e) tailq;
  char *assertion;
};

/**
 * Initializes the handler
 * @param  assertion_handler Handler that will be used
 * @return                   Error code
 */
struct assertion_handler_s *assertion_handler_create() {
  struct assertion_handler_s *assertion_handler =
      (struct assertion_handler_s *)calloc(1,
                                           sizeof(struct assertion_handler_s));

  if (pthread_mutex_init(&assertion_handler->mutex, NULL) != 0) {
    return NULL;
  }

  TAILQ_INIT(&assertion_handler->assertion_q);

  return assertion_handler;
}

/**
 * Adds an assertion to a given handler
 * @param  assertion_handler Handler that will be used
 * @param  message_assertion Message to assert
 * @return                   Error code
 */
int assertion_handler_add(struct assertion_handler_s *assertion_handler,
                          char *message_assertion) {

  if (NULL == message_assertion) {
    return 1;
  }

  struct assertion_e *assertion =
      (struct assertion_e *)calloc(1, sizeof(struct assertion_e));

  assertion->assertion = strdup(message_assertion);

  if (strlen(message_assertion) > 0) {
    pthread_mutex_lock(&assertion_handler->mutex);
    TAILQ_INSERT_TAIL(&assertion_handler->assertion_q, assertion, tailq);
    pthread_mutex_unlock(&assertion_handler->mutex);
  }

  return 0;
}

/**
 * Assert every element in the queue
 * @param  assertion_handler Handler that will be used
 * @return                   Error code
 */
int assertion_handler_assert(struct assertion_handler_s *assertion_handler) {
  struct assertion_e *assertion;

  //// NOTE Debug block
  pthread_mutex_lock(&assertion_handler->mutex);
  TAILQ_FOREACH(assertion, &assertion_handler->assertion_q, tailq) {
    printf("ASSERTION: %s\n", assertion->assertion);
    pthread_mutex_unlock(&assertion_handler->mutex);
  }
  ////

  // TODO Assert here

  return 0;
}

/**
 * Removes every element in the queue and free the handler
 * @param  assertion_handler Handler that will be used
 * @return                   Error code
 */
int assertion_handler_destroy(struct assertion_handler_s *assertion_handler) {
  // TODO Free elements

  pthread_mutex_destroy(&assertion_handler->mutex);
  free(assertion_handler);
  return 0;
}
