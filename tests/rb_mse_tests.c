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

#include <jansson.h>

#include "rb_mse_tests.h"

/* malloc / calloc fails tests */

size_t mem_wrap_fail_in = 0;

void mem_wraps_set_fail_in(size_t i) {
	mem_wrap_fail_in = i;
}

size_t mem_wraps_get_fail_in() {
	return mem_wrap_fail_in;
}
