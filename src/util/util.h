/*
** Copyright (C) 2015 Eneo Tecnologia S.L.
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

#include "librd/rdlog.h"

#include <string.h>

#define RB_UNUSED __attribute__((unused))

#ifdef likely
#undef likely
#endif
#define likely(x)       __builtin_expect(!!(x), 1)

#ifdef unlikely
#undef unlikely
#endif
#define unlikely(x)     __builtin_expect(!!(x), 0)

#define rblog(x...) rdlog(x)

#define fatal(msg...) do{rblog(LOG_ERR,msg);exit(1);}while(0)

static inline char *mystrerror(int _errno,char *buffer,size_t buffer_size){
	strerror_r(_errno,buffer,buffer_size);
	return buffer;
}