#pragma once

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

size_t mem_wrap_fail_in;

void mem_wraps_set_fail_in(size_t);
size_t mem_wraps_get_fail_in();

