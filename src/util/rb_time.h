 /*
** Copyright (C) 2016 Eneo Tecnologia S.L.
** Author: Eugenio Perez <eupm90@gmail.com>
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU Affero General Public License as
** published by the Free Software Foundation, either version 3 of the
** License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU Affero General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <time.h>

/** Checks a timespec struct
  @param @tv Timespec
  @returns 0 if timespec is different than zero. !0 ioc
  */
static int zero_timespec(const struct timespec *ts) __attribute__((unused));
static int zero_timespec(const struct timespec *ts) {
	return 0 == ts->tv_sec && 0 == ts->tv_nsec;
}

/** Compare two timespecs
  @param ts1 First timespec
  @param ts2 second timespec
  @return 0 if equal, >0 if second timespec if bigger than first, and
  <0 in other case
  */
static int cmp_timespec(const struct timespec *ts1,
			const struct timespec *ts2) __attribute__((unused));
static int cmp_timespec(const struct timespec *ts1,
			const struct timespec *ts2) {
	if(ts1->tv_sec != ts2->tv_sec) {
		return ts2->tv_sec - ts1->tv_sec;
	} else {
		return ts2->tv_nsec - ts1->tv_nsec;
	}
}
