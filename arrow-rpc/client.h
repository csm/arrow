/* client.h -- 
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


#ifndef __CLIENT_H__
#define __CLIENT_H__

#include <stdint.h>
#include <fileinfo.h>
#include "rpc.h"

int rpc_client_read_link (rpc_t *client, const char *path, uuid_t uuid);
int rpc_client_fetch_file (rpc_t *client, file_t *file);
int rpc_client_read_file_hash (rpc_t *client, file_t *file);
int rpc_client_create_file (rpc_t *client, file_t *file);
int rpc_client_make_link (rpc_t *client, const char *path, const uuid_t uuid);
int rpc_client_add_ref (void *client, const arrow_id_t *id);
int rpc_client_put_chunk (void *client, const arrow_id_t *id, const void *buf, size_t len);
int rpc_client_contains (void *client, const arrow_id_t *id);
int rpc_client_emit_chunk (void *client, const file_chunk_t *chunk);
int rpc_client_close_file (rpc_t *client, file_t *file, int abort);
int rpc_client_goodbye (rpc_t *client);

#endif /* __CLIENT_H__ */
