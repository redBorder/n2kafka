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

#include "rb_timer.h"
#include "rb_time.h"
#include "util/util.h"

#include <time.h>
#include <librd/rdlog.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

/// @TODO delete?
#include <sys/resource.h>

/*
 * TIMERS
 */

struct rb_timer  {
	/// Do we need to execute timer?.
	/// @TODO this field is not needed if we use SIGEV_THREAD
	volatile int execute_cb;

	/// Timer id
	timer_t timerid;

	/// Callback registered
	void (*cb)(void *);

	/// Context to call callback with
	void *cb_ctx;

	/// List entry
	LIST_ENTRY(rb_timer) entry;
};

/**
  Set timer interval
  @param timer timer to modify
  @param tv timeval
  @param err error buffer
  @param errsize error buffer size
  @return 0 if success, !0 if any
  */
int rb_timer_set_interval0(rb_timer_t *timer, int flags,
		const struct itimerspec *ts, char *err, size_t errsize) {
	struct itimerspec *old_value = NULL;

	const int settime_rc = timer_settime(timer->timerid, flags, ts,
								old_value);

	if (0 != settime_rc) {
		char buf[BUFSIZ];
		snprintf(err, errsize, "Couldn't set timer %p: %s",
			timer, mystrerror(errno, buf, sizeof(buf)));
	}

	return settime_rc;
}

int rb_timer_get_interval(const struct rb_timer *timer, struct itimerspec *ts) {
	return timer_gettime(timer->timerid, ts);
}

/** Delete a timer
  @param timer Timer to destroy
  */
static void rb_timer_done0(struct rb_timer *timer) {
	timer_delete(timer->timerid);
	free(timer);
}


/*
 * TIMERS LIST
 */

#define rb_timers_list_first(list) LIST_FIRST(list)
#define rb_timers_list_remove_elm(elm) LIST_REMOVE(elm, entry)
#define rb_timers_list_foreach(elm,list) LIST_FOREACH(elm, list, entry)
#define rb_timers_list_insert_head_nl(list, elm) \
	LIST_INSERT_HEAD(list, elm, entry)
static void rb_timers_list_insert_head(rb_timers_list_t *list,
							struct rb_timer *elm) {
	pthread_mutex_lock(&list->mutex);
	rb_timers_list_insert_head_nl(&list->list, elm);
	pthread_mutex_unlock(&list->mutex);
}

rb_timer_t *rb_timer_create(rb_timers_list_t *tlist,
		const struct itimerspec *interval,
		void (*cb)(void *), void *cb_ctx, char *err, size_t errsize) {
	struct sigevent sevp;

	struct rb_timer *new_timer = calloc(1, sizeof(*new_timer));
	if (NULL == new_timer) {
		snprintf(err, errsize,
			"Couldn't allocate timer (out of memory?)");
		return NULL;
	}
	new_timer->cb = cb;
	new_timer->cb_ctx = cb_ctx;

	memset(&sevp,0,sizeof(sevp));
#if 0
	sevp.sigev_notify = SIGEV_THREAD; /* Notification method */
	sevp.sigev_value.sival_ptr = new_timer;  /* Data passed with
	                                	    notification */
	sevp.sigev_notify_function = alarm_signal_action;
	                    /* Function used for thread
	                       notification (SIGEV_THREAD) */
	sevp.sigev_notify_attributes = NULL;
#elif 1
	sevp.sigev_notify          = SIGEV_SIGNAL; /* Notification method */
	sevp.sigev_signo           = SIGALRM;
	sevp.sigev_value.sival_ptr = new_timer;
#endif

	const int create_rc = timer_create(CLOCK_REALTIME, &sevp,
		&new_timer->timerid);
	if (0 != create_rc) {
		char buf[BUFSIZ];

		snprintf(err, errsize, "Couldn't create kernel timer: %s",
			mystrerror(errno,buf,sizeof(buf)));

		goto err;
	}

	const int settime_rc = rb_timer_set_interval(new_timer, interval,
								err, errsize);
	if (0 != settime_rc) {
		goto settime_err;
	}

	rb_timers_list_insert_head(tlist, new_timer);

	return new_timer;

settime_err:
	timer_delete(new_timer->timerid);
err:
	free(new_timer);
	return NULL;
}

void rb_timer_sigaction(int signum, siginfo_t *siginfo, void *ucontext) {
	(void)ucontext;
	(void)signum;
	struct rb_timer *timer = siginfo->si_value.sival_ptr;

	if(SIGALRM != signum){
		rdlog(LOG_DEBUG, "Unknown signal received!");
		return;
	}
	if(timer) {
		timer->execute_cb = 1;
	}
}

void rb_timers_list_init(rb_timers_list_t *list) {
	assert(list);

	pthread_mutex_init(&list->mutex, NULL);
	LIST_INIT(&list->list);
}

void rb_timers_run(rb_timers_list_t *list) {
	rb_timer_t *timer;
	/* Pending timers. This way, you don't need to hold the mutex when you
	call the timer */
	LIST_HEAD(, rb_timer) pending_timers;
	LIST_INIT(&pending_timers);

	rdlog(LOG_DEBUG, "Cheking timers");

	/*
	 * Just copy the pending timers
	 */
	pthread_mutex_lock(&list->mutex);
	rb_timers_list_foreach(timer,&list->list) {
		if (timer->execute_cb) {
			rdlog(LOG_DEBUG, "Raising & restarting timer %p",
				timer);

			rb_timer_t *pending_timer = calloc(1,
				sizeof(*pending_timer));
			if (NULL == pending_timer) {
				rdlog(LOG_ERR,
					"Couldn't duplicate list element! %p",
					timer);
			} else {
				memcpy(pending_timer, timer,
					sizeof(*pending_timer));
				LIST_INSERT_HEAD(&pending_timers,
							pending_timer, entry);
			}

			timer->execute_cb = 0;
		}
	}
	pthread_mutex_unlock(&list->mutex);

	/*
	 * RUN THEM!
	 */
	while ((timer = LIST_FIRST(&pending_timers))) {
		rb_timers_list_remove_elm(timer);
		timer->cb(timer->cb_ctx);
		free(timer);
	}
}

void rb_timer_done(rb_timers_list_t *list, struct rb_timer *timer) {
	pthread_mutex_lock(&list->mutex);
	rb_timers_list_remove_elm(timer);
	pthread_mutex_unlock(&list->mutex);

	rb_timer_done0(timer);
}

/** Remove and frees a timer, but does not lock list
  @param timer Timer to release
  */
static void rb_timer_done_NL(struct rb_timer *timer) {
	rb_timers_list_remove_elm(timer);
	rb_timer_done0(timer);
}


void rb_timers_list_done(rb_timers_list_t *list) {
	struct rb_timer *iter = NULL;
	while((iter = rb_timers_list_first(&list->list))) {
		rb_timer_done_NL(iter);
	}
	pthread_mutex_destroy(&list->mutex);
}
