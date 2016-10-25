#include "../config.h"

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
#ifdef TESTS_KAFKA_HOST
#define SKIP_IF_NOT_INTEGRATION
#else
#define SKIP_IF_NOT_INTEGRATION                                                \
  do {                                                                         \
    skip();                                                                    \
    return;                                                                    \
  } while (0)
#endif
