/* arrow.h -- 
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


#ifndef __ARROW_H__
#define __ARROW_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <openssl/md5.h>

/**
 * Protocol version.
 */
#define ARROW_PROTOCOL 1

/**
 * File (superblock, block) versions.
 */
#define ARROW_FILE_VERSION 1

/**
 * Initial number of chunks allocated per block.
 */
#define ARROW_BLOCK_INITIAL_COUNT (5 * 1024)

/**
 * Approximation of the chunk size. Real chunks can be of any length,
 * but we'll try to keep chunks above 255 bytes.
 */
#define ARROW_CHUNK_SIZE 1000

#define ARROW_BLOCKS_DIR "blocks"

#define MIN_CHUNK_SIZE 700
#define MAX_CHUNK_SIZE 16000

/**
 * 
 */
typedef uint8_t arrow_key_t[MD5_DIGEST_LENGTH];
/* typedef struct arrow_key_s */
/* { */
/*   uint8_t id[16];  /\**< An MD5 hash value. *\/ */
/* } arrow_key_t; */

typedef uint32_t arrow_fast_key_t;

typedef struct arrow_id_s
{
  arrow_fast_key_t weak; /**< The weak checksum.   */
  arrow_key_t strong;    /**< The strong checksum. */
} arrow_id_t;

/* struct _arrow_state; */
/* typedef struct _arrow_state arrow_state; */

/* int arrow_init (arrow_state *state); */
/* void arrow_destroy (arrow_state *state); */

typedef struct mapped_file_s
{
  int fd;
  void *data;
  size_t length;
} mapped_file_t;

#define align_up(sz,ps) (((ps) - ((sz) % (ps))) + (sz))
#define align_down(sz,ps) (((sz) / (ps)) * (ps))

void arrow_compute_key (arrow_id_t *id, const void *data, size_t len);

/**
 * Convert the 8 bytes starting at \c buf to an unsigned 64-bit
 * integer. The value is taken as big-end-first.
 */
uint64_t arrow_bytes_to_long (const uint8_t *buf);

void arrow_long_to_bytes (uint8_t *buf, uint64_t value);

void arrow_left_fill (char *buf, size_t len, char fillchar);

void arrow_push_errno(void);
void arrow_pop_errno(void);

#define arrow_id_cmp(i1,i2) (memcmp (i1, i2, sizeof (arrow_id_t)))

#define MIN(a,b) ((a)<(b) ? (a) : (b))

int arrow_popen (const char *cmd, char * const argv[], pid_t *pid, FILE **out, FILE **in);
pid_t arrow_pclose (pid_t pid, FILE *out, FILE *in, int *status);

void *arrow_malloc (size_t s);

#endif /* __ARROW_H__ */

/* Local Variables: */
/* tab-width: 8 */
/* indent-tabs-mode: nil */
/* c-basic-offset: 2 */
/* End: */
