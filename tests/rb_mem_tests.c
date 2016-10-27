/*
  Copyright (C) 2016 Eneo Tecnologia S.L.
  Author: Ana Rey <anarey@gmail.com>
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.
  You should have received a copy of the GNU Affero General Public License
  along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#include "rb_mem_tests.h"

#include <jansson.h>
#include <pthread.h>
/* malloc / calloc fails tests */

size_t mem_wrap_fail_in = 0;


void mem_wraps_set_fail_in(size_t i) {
	mem_wrap_fail_in = i;
}

size_t mem_wraps_get_fail_in() {
	return mem_wrap_fail_in;
}

#define COMMA ,

#define WRAP_MEM_FN(fun, ret_t, args, real_args)                               \
ret_t __real_##fun (args);                                              \
ret_t __wrap_##fun (args);                                              \
ret_t __wrap_##fun (args) {                                             \
  return (mem_wrap_fail_in == 0 || --mem_wrap_fail_in) ?                 \
            __real_##fun (real_args) : 0;\
}

WRAP_MEM_FN(malloc, void *, size_t m, m)
WRAP_MEM_FN(strdup, char *, const char *str, str)
WRAP_MEM_FN(json_object, json_t *,,)
WRAP_MEM_FN(calloc, void *, size_t n COMMA size_t m, n COMMA m)
WRAP_MEM_FN(realloc, void *, void *ptr COMMA size_t m, ptr COMMA m)

