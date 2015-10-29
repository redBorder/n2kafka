/*
**
** Copyright (c) 2014, Eneo Tecnologia
** Author: Eugenio Perez <eupm90@gmail.com>
** All rights reserved.
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
#include <stdint.h>

/// MAX mac can return parse_mac
#define MAX_MAC 0xffffffffffffL

#define valid_mac(mac) (mac <= MAX_MAC)
#define INVALID_MAC (MAX_MAC+1)

// error if return 0xFFFFFFFFFFFFFFFFL
uint64_t parse_mac(const char *mac);
