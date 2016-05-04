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

#pragma once

#include <signal.h>
#include <time.h>
#include <sys/queue.h>

/// Internal - single timer
struct rb_timer;
typedef struct rb_timer rb_timer_t;

/// List of timers
typedef struct rb_timers_list_s {
	/* Internal */
	pthread_mutex_t mutex;

	/// List of registered timers
	LIST_HEAD(, rb_timer) list;
} rb_timers_list_t;

/** Sets a new interval to a timer
  @param timer timer.
  @param tv new interval
  @param err Error string (if any)
  @param errsize Size of err
  @return 0 is success, !0 and errno if error
  */
int rb_timer_set_interval(rb_timers_list_t *list, rb_timer_t *timer,
                        const struct itimerspec *ts, char *err, size_t errsize);

/** Gets timer interval
  @param timer timer to get the interval
  @param ts Timespec to save the interval
  @return 0 if success. !0 and errno if error
  */
int rb_timer_get_interval(const rb_timer_t *timer, struct itimerspec *ts);

/** Start a timers list
  @param list of timers
  @return 0 in success, 1 ioc.
  */
void rb_timers_list_init(rb_timers_list_t *list);

/** Destroy a list of timers and all timers that it contains
  @param list list of timers
  */
void rb_timers_list_done(rb_timers_list_t *list);

/** Destroy a timer
  @param list List of timers
  @param timer Timer to destroy
  */
void rb_timer_done(rb_timers_list_t *list, struct rb_timer *timer);

/**
  Runs all pending timers
  @TODO this function is pointless if we can use SIGEV_THREAD.
  */
void rb_timers_run(rb_timers_list_t *list);

/** Create a timer and append it to a list of timers
  @param tlist timer list
  @param interval Time interval you want the timer to signal you.
  @param cb Callback to call
  @param cb_ctx Context to send to callback
  @param err Error string
  @param errsize Error string size
  @return new timer if successful
  */
rb_timer_t *rb_timer_create(rb_timers_list_t *tlist,
	const struct itimerspec *interval, void (*cb)(void *), void *cb_ctx,
	char *err, size_t errsize);

/// Need to call this function if a registered timer calls SIGALRM
void rb_timer_sigaction(int signum, siginfo_t *siginfo, void *ucontext);
