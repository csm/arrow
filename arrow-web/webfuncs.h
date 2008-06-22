/* webfuncs.h -- 
   Copyright (C) 2008  Casey Marshall

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.  */


#ifndef __WEBFUNCS_H__
#define __WEBFUNCS_H__

#include <backup.h>
#include <sync.h>

#define WEB_STORE_PATH "/store/"

struct web_ctx_s;
typedef struct web_ctx_s web_ctx_t;

/**
 * Initialize a web context.
 * 
 * \param ctx Pointer to a pointer to store the new context.
 */
int web_init (web_ctx_t **ctx, const char *baseurl);

/**
 * Destroy a web context.
 * 
 * \param ctx The context to destroy.
 */
void web_deinit (web_ctx_t *ctx);

int web_add_ref (void *state, const arrow_id_t *id);
int web_put_block (void *state, const arrow_id_t *id, const void *buf, size_t len);
int web_store_contains (void *state, const arrow_id_t *id);
int web_emit_chunk (void *state, const file_chunk_t *chunk);

#endif /* __WEBFUNCS_H__ */
