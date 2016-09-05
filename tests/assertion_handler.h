#include "stdio.h"
#include "stdlib.h"

#include <string.h>
#include <pthread.h>
#include <sys/queue.h>

// Public structs
struct assertion_e {
  TAILQ_ENTRY(assertion_e) tailq;
  char *str;
};

struct value_e {
  TAILQ_ENTRY(value_e) tailq;
  char *str;
  size_t len;
};

// Private structs
struct assertion_handler_s;

// Functions
struct assertion_handler_s *assertion_handler_new();
void assertion_handler_push_assertion(
    struct assertion_handler_s *assertion_handler,
    struct assertion_e *assertion);
void assertion_handler_push_value(struct assertion_handler_s *assertion_handler,
                                  struct value_e *value);
int assertion_handler_assert(struct assertion_handler_s *assertion_handler);
void assertion_handler_destroy(struct assertion_handler_s *assertion_handler);
