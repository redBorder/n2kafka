/*
**
** Copyright (c) 2014, Eneo Tecnologia
** Author: Eugenio Perez <eupm90@gmail.com>
** All rights reserved.
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU General Public License Version 2 as
** published by the Free Software Foundation.  You may not use, modify or
** distribute this program under any other version of the GNU General
** Public License.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program; if not, write to the Free Software
** Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*/

#include <stdint.h>
#include <string.h>

/* All functions are thread-safe here, excepting free_valid_mse_database */

struct mse_data {
	/* is NULL in some flows, so we need to save them here */
	uint64_t client_mac;
	const char *subscriptionName;
	/* private */
	const char *_client_mac;
};

struct valid_mse_database;
struct valid_mse_database *parse_valid_mse_file(const char *path,char *err,size_t err_size);
void free_valid_mse_database(struct valid_mse_database *db);
int reload_valid_mse_database(struct valid_mse_database *db,char *err,size_t err_size);

struct enrich_with;
struct enrich_with *process_enrich_with(const char *enrich_with);
void free_enrich_with(struct enrich_with *enrich_with);

char *process_mse_buffer(char *from,size_t *bsize,struct mse_data *data,
                                  const struct enrich_with *enrich_with,
                                  struct valid_mse_database *db);
