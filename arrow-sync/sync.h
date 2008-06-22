/* sync.h -- 
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


#ifndef __SYNC_H__
#define __SYNC_H__

#include <stdio.h>
#include <fileinfo.h>
#include <store.h>

typedef struct sync_callbacks_s
{
  int (*add_ref) (void *state, const arrow_id_t *id);
  int (*put_block) (void *state, const arrow_id_t *id, const void *buf, size_t len);
  int (*store_contains) (void *state, const arrow_id_t *id);
  int (*emit_chunk) (void *state, const file_chunk_t *chunk);
  void *state;
} sync_callbacks_t;

int sync_generate (file_t *file, FILE *input, sync_callbacks_t *cb);
int sync_file (file_t *basis, file_t *newfile, FILE *data, sync_callbacks_t *cb, int *hash_match);

/* Callback functions that directly insert into a local store. */

typedef struct sync_store_state_s
{
  store_state_t *store;
  FILE *chunks_out;
} sync_store_state_t;

int sync_store_add_ref (void *state, const arrow_id_t *id);
int sync_store_put_block (void *state, const arrow_id_t *id, const void *buf, size_t len);
int sync_store_contains (void *state, const arrow_id_t *id);
int sync_store_emit_chunk (void *state, const file_chunk_t *chunk);

#endif /* __SYNC_H__ */
