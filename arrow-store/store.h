/* store.h -- 
   Copyright (C) 2008  Casey Marshall

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  */


#ifndef __STORE_H__
#define __STORE_H__

#include <arrow.h>
#include <stdlib.h>
#include <stdio.h>

#define STORE_ID_LEN 12 /* store a 8-byte value in base-64 */

typedef struct store_error_s
{
  size_t count; /**< Number of errors. */
  int *keys;    /**< Indexes of the keys that failed. Contains `count'  */
} store_error_t;

#define store_free_error_info(e) if ((e)->keys != NULL) free ((e)->keys)

typedef struct store_s
{
  char id[STORE_ID_LEN+1];       /**< store identifier. */
  mapped_file_t data;            /**< The memory-mapped file. */
} store_t;

struct store_state_s;
typedef struct store_state_s store_state_t;

int store_init (const char *rootdir, store_state_t **state);
void store_destroy (store_state_t *state);

int store_open (store_state_t *state, store_t *store);
int store_close (store_state_t *state, store_t *store);

/**
 * Compute the store identifier for the given ID value.
 *
 * \param state  The store state.
 * \param id     The store ID to map.
 * \param result Storage for the result; must be at least
 *               \c STORE_ID_LEN+1 bytes.
 */
void store_map_key (store_state_t *state, const arrow_id_t *id, char *result);

int store_put (store_state_t *state, const arrow_id_t *id, const void *buf, size_t len);
int store_addref (store_state_t *state, const arrow_id_t *id);
size_t store_get (store_state_t *state, const arrow_id_t *id, void *out, size_t maxlen);
size_t store_get_len (store_state_t *state, const arrow_id_t *id);
int store_contains (store_state_t *state, const arrow_id_t *id);

int store_put_into (store_t *store, const arrow_id_t *id, const void *buf, size_t len);
int store_addref_to (store_t *store, const arrow_id_t *id);
size_t store_get_from (store_t *store, const arrow_id_t *id, void *out, size_t maxlen);
size_t store_get_len_from (store_t *store, const arrow_id_t *id);

int store_verify_all (store_state_t *state);
int store_verify (store_t *store, store_error_t *errors);
int store_repair (store_t *store, store_error_t *errors);

void store_dump (FILE *out, store_state_t *state);
void store_dump_store (FILE *out, store_t *store);

int store_size (store_state_t *state, uint64_t *used, uint64_t *total);

#endif /* __STORE_H__ */
