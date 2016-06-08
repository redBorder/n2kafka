#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>

/**
 * assertion_handler_s is used to store assertions and check them later.
 */
struct assertion_handler_s {
  TAILQ_HEAD(, assertion_e) assertion_q;
  pthread_mutex_t mutex; // Mutex used to keep the handle memory safe
};

struct assertion_handler_s *assertion_handler_create();
int assertion_handler_add(struct assertion_handler_s *assertion_handler,
                          char *message_assertion);
int assertion_handler_assert(struct assertion_handler_s *assertion_handler);
int assertion_handler_destroy(struct assertion_handler_s *assertion_handler);
