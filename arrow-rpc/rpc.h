/* rpc.h -- 
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


#ifndef __RPC_H__
#define __RPC_H__

#include <stdio.h>
#include <stdint.h>

typedef enum rpc_command_e
{
  READ_LINK_FILE      =  2,
  FETCH_VERSION_FILE  =  3,
  READ_FILE_HASH      =  4,
  CREATE_VERSION_FILE =  5,
  MAKE_FILE_LINK      =  6,
  STORE_ADD_REF       =  7,
  STORE_PUT_CHUNK     =  8,
  STORE_BLOCK_EXISTS  =  9,
  FILE_EMIT_CHUNK     = 10,
  CLOSE_VERSION_FILE  = 11,
  GOODBYE             = 12
} rpc_command_t;

typedef struct rpc_stats_s
{
  uint64_t bytes_in;
  uint64_t bytes_out;
} rpc_stats_t;

typedef struct rpc_s
{
  FILE *in;
  FILE *out;
  rpc_stats_t *stats;
} rpc_t;

int write_value (rpc_t *rpc, size_t esize, size_t count, const void *value);
int write_short (rpc_t *rpc, uint16_t value);
int write_int (rpc_t *rpc, uint32_t value);
int write_long (rpc_t *rpc, uint64_t value);
int write_string (rpc_t *rpc, const char *str);

int read_value (rpc_t *rpc, size_t esize, size_t count, void *buf);
int read_short (rpc_t *rpc, uint16_t *value);
int read_int (rpc_t *rpc, uint32_t *value);
int read_long (rpc_t *rpc, uint64_t *value);
int read_string (rpc_t *rpc, char *buf, int len);

#endif /* __RPC_H__ */
