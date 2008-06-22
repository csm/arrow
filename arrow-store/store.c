/* store.c -- 
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


#include "store.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <openssl/md5.h>
#include <base64.h>
#include <rollsum.h>
#include <fail.h>
/* #include "../rslib/rs.h" */

/* fake definitions, in rslib's absense */
typedef int rs_handle;
#define rslib_free_rs(x)

#define STORE_SUPERBLOCK ".superblock"
#define MAX_LOAD_FACTOR 0.70

/* How large each codeword is -- we run an RS code over each subblock
   of this size. */
#define RS_CODEWORD_SIZE 253

#define RS_PARITY_SIZE 2

/*
 * The layout of the block store is a linear hash table, with a series
 * of small files storing some number of chunks. Each block file is
 * organized into four parts:
 *
 *  1. The block header. This contains meta-information about the
 *     block. This is fixed-size, and is represented by the
 *     block_header_t type.
 *
 *  2. The key list. This is a list of all chunk keys in this
 *     block. This is constant size, equivalent to the chunk_count
 *     value in the header, times the size of a block_key_t. Each
 *     block key contains the chunk identifer, the offset of the chunk
 *     data in the data region, the chunk length, and a reference
 *     count. Blank slots are represented by all zeros (the null_key
 *     contant).
 *
 *  3. The chunks, aka the data region. This region is alloc_size
 *     bytes long.
 *
 *  4. The parity data. This is computed over the entire file, with
 *     one byte per 127 bytes in the rest of the file.
 */

int STORE_DEBUG = 0;

#define STORE_PERF    1
#define STORE_TRACE  (1 << 1)
#define STORE_RSLIB  (1 << 2)
#define STORE_SPLIT  (1 << 3)

#define store_log(lvl,fmt,args...) if (STORE_DEBUG & lvl) fprintf (stderr, "%s (%s:%d): " fmt "\n", __FUNCTION__, __FILE__, __LINE__, ##args)
#define store_trace(fmt, args...) store_log(STORE_TRACE, fmt, ##args)
#define store_perror(fmt, args...) store_log(STORE_TRACE, fmt, ##args)

#define STORE_CACHE_SIZE 128

typedef struct store_cached_entry_s
{
  store_t entry;
  uint32_t refs;
} store_cached_entry_t;

struct store_state_s
{
  char *rootdir;
  mapped_file_t data; /**< The memory-mapped superblock. */
  store_cached_entry_t cache[STORE_CACHE_SIZE]; /**< Cache of stores kept open. */
  struct rs_handle *rs;
};

/**
 * The superblock contains the bookkeeping info for the whole store.
 * The store is implemented using linear hashing, and the superblock
 * contains the info for maintaining that state.
 */
typedef struct store_sb_s
{
  char header[4];
  uint8_t version;
  uint16_t i;      /**< Linear hash level. */
  uint64_t n;      /**< Linear hash pointer. */
} store_sb_t;

typedef struct block_key_s
{
  arrow_id_t id;        /**< The block identifier. */
  uint32_t offset;      /**< Offset, relative to the start of the data region. */
  uint32_t length;      /**< Length of the chunk. */
  uint16_t references;  /**< Reference count. */
} block_key_t;

/**
 * A block is a collection of chunks. Blocks begin with the chunk
 * count, followed by a bunch of keys, offsets, and sizes, followed by
 * the chunks, and ending with parity bytes over the whole block.
 */
typedef struct block_header_s
{
  char header[4];
  uint8_t version;
  uint16_t chunk_count;
  uint32_t alloc_size;
  block_key_t keys[0];
} block_header_t;

static const block_key_t null_key = { { 0, { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } }, 0, 0, 0 };

static const char superblock_header[4] = { 'A', 'R', 'W', 'S' };
static const char block_header[4] = { 'A', 'R', 'W', 'B' };

static int
store_put_into_int (store_t *store, const arrow_id_t *id, const void *buf, size_t len,
                    int gen_rs, struct rs_handle *rs);

 /* Static functions. */

static struct rs_handle *
make_rs_handle (void)
{
/*   uint8_t init_vector[RS_CODEWORD_SIZE + 1]; */
/*   int i; */
/*   for (i = 0; i < RS_CODEWORD_SIZE + 1; i++) */
/*     init_vector[i] = (uint8_t) i; /\* huh??? *\/ */
/*   return rslib_init_rs (RS_CODEWORD_SIZE, RS_PARITY_SIZE, init_vector); */
  return NULL;
}

static uint64_t
do_map_key(store_state_t *state, const arrow_id_t *id, uint64_t n)
{
  uint64_t key = 0, x = 0;
  store_sb_t *sb = (store_sb_t *) state->data.data;

  x = (((uint64_t) (id->strong[8] & 0xFFULL) << 56)
	   | ((uint64_t) (id->strong[9] & 0xFFULL) << 48)
	   | ((uint64_t) (id->strong[10] & 0xFFULL) << 40)
       | ((uint64_t) (id->strong[11] & 0xFFULL) << 32)
       | ((uint64_t) (id->strong[12] & 0xFFULL) << 24)
       | ((uint64_t) (id->strong[13] & 0xFFULL) << 16)
       | ((uint64_t) (id->strong[14] & 0xFFULL) << 8)
       | ((uint64_t) (id->strong[15] & 0xFFULL)));
  store_trace("%llu (i: %u, n: %llu)", x, sb->i, sb->n);
  key = x & ((1ULL << sb->i) - 1);
  if (key < n)
	key = x & ((1ULL << (sb->i+1)) - 1);

  return key;
}

inline static size_t
store_header_size (store_t *store)
{
  block_header_t *header = (block_header_t *) store->data.data;
  return sizeof (block_header_t) + (header->chunk_count * sizeof (block_key_t));
}

inline static void *
store_data_base (store_t *store)
{
  return store->data.data + store_header_size (store);
}

inline static size_t
store_offset_of_key (store_t *store, int key)
{
  return sizeof (block_header_t) + (key * sizeof (block_key_t));
}

inline static size_t
store_offset_of_chunk (store_t *store, size_t offset)
{
  return store_header_size (store) + offset;
}

inline static size_t
store_offset_of_chunk_number (store_t *store, int i)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  return store_header_size (store) + keys[i].offset;
}

static double
store_load_factor (store_t *store)
{
  block_header_t *header = store->data.data;
  block_key_t *keys = header->keys;
  int used = 0;
  int i;

  for (i = 0; i < header->chunk_count; i++)
	{
	  if (memcmp (&keys[i], &null_key, sizeof (block_key_t)) != 0)
		used++;
	}

  return (double) used / (double) header->chunk_count;
}

static void *
get_parity_bytes (store_t *store, int i)
{
  block_header_t *header = (block_header_t *) store->data.data;
  size_t offset = (sizeof (block_header_t) + (header->chunk_count * sizeof (block_key_t))
                   + (header->alloc_size));

  offset = align_up (offset, RS_CODEWORD_SIZE);
  return store->data.data + offset + (i * RS_PARITY_SIZE);
}

/**
 * Generate reed-solomon codes for store starting at subblock begin to
 * subblock end.
 */
static void
generate_rscode (struct rs_handle *rs, store_t *store, int begin, int end)
{
/*   block_header_t *header = (block_header_t *) store->data.data; */
/*   size_t offset = (sizeof (block_header_t) + (header->chunk_count * sizeof (block_key_t)) */
/*                    + (header->alloc_size)); */
/*   int i, n; */
/*   clock_t clk; */
/*   int alloced_rs = 0; */

/*   store_log (STORE_RSLIB, "generating reed-solomon code for subblocks [%d, %d)", */
/*              begin, end); */

/*   if (rs == NULL) */
/*     { */
/*       rs = make_rs_handle(); */
/*       alloced_rs = 1; */
/*     } */

/*   offset = align_up (offset, RS_CODEWORD_SIZE); */

/*   n = offset / RS_CODEWORD_SIZE; */
/*   if (begin < 0) */
/*     begin = 0; */
/*   if (end < 0) */
/*     end = n; */

/*   clk = clock(); */
/*   for (i = begin; i < end; i++) */
/*     rslib_encode (rs, store->data.data + (i * RS_CODEWORD_SIZE), */
/*                   store->data.data + offset + (i * RS_PARITY_SIZE)); */
/*   clk = clock() - clk; */
/*   store_log (STORE_PERF, "rslib_encode of %d bytes took %f seconds", */
/*              (end - begin) * RS_CODEWORD_SIZE, (double) clk / (double) CLOCKS_PER_SEC); */
/*   if (alloced_rs) */
/*     rslib_free_rs (rs); */
}

/*
 * Finds the indexes [begin, end) of subblocks that have changed, if
 * the length *bytes* starting at offset changed. 
 */
static inline void
find_changed_subblocks (size_t offset, size_t length, int *begin, int *end)
{
  *begin = offset / RS_CODEWORD_SIZE;
  *end = align_up(offset + length, RS_CODEWORD_SIZE) / RS_CODEWORD_SIZE;
}

static int
create_new_block (store_state_t *state, uint64_t id)
{
  store_t store;
  block_header_t header;
  off_t total_size;
  char *path;
  size_t pathlen;
  int fd;
  struct stat st;

  memcpy (header.header, block_header, 4);
  header.version = ARROW_FILE_VERSION;
  header.chunk_count = ARROW_BLOCK_INITIAL_COUNT;
  header.alloc_size = header.chunk_count * ARROW_CHUNK_SIZE;

  /* What's the total size of our file? */
  total_size = (sizeof (block_header_t) + (header.chunk_count * sizeof (block_key_t))
                + (header.alloc_size));
  total_size = align_up (total_size, RS_CODEWORD_SIZE);
  total_size += (total_size / RS_CODEWORD_SIZE) * RS_PARITY_SIZE;

  b64_encode (id, store.id);
  pathlen = strlen (state->rootdir) + strlen (ARROW_BLOCKS_DIR) + strlen(store.id) + 3;
  path = (char *) malloc (pathlen);

  if (path == NULL)
    return -1;

  snprintf (path, pathlen, "%s/%s", state->rootdir, ARROW_BLOCKS_DIR);
  if (stat (path, &st) != 0)
    {
      if (errno != ENOENT)
        {
          free (path);
          return -1;
        }
      if (mkdir (path, 0700) != 0)
        {
          free (path);
          return -1;
        }
    }
  else if ((st.st_mode & S_IFDIR) == 0)
    {
      free (path);
      errno = ENOTDIR;
      return -1;
    }

  snprintf (path, pathlen, "%s/%s/%s", state->rootdir, ARROW_BLOCKS_DIR, store.id);
  fd = open (path, O_RDWR | O_CREAT | O_EXCL, 0600);
  if (fd < 0)
    {
      free (path);
      return -1;
    }

  free (path);
  ftruncate (fd, total_size);
  write (fd, &header, sizeof (block_header_t));
  close (fd);

  return 0;
}

static void
compact_block (store_state_t *state, store_t *store)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  int i, j = -1;
  int begin, end;
  int numkeys = 0;
  uint32_t last = 0;

  for (i = 0; i < header->chunk_count; i++)
    {
      if (memcmp (&keys[i], &null_key, sizeof (block_key_t)) == 0)
        {
          /* Found a free slot. */
          if (j == -1)
            j = i;
        }
      else
        {
          numkeys++;
          if (j != -1)
            {
              uint32_t offset = 0;
              if (j > 0)
                offset = keys[j-1].offset + keys[j-1].length;
              memmove (store_data_base (store) + offset,
                       store_data_base (store) + keys[i].offset,
                       keys[i].length);
              memmove (&keys[j], &keys[i], sizeof(block_key_t));
              keys[j].offset = offset;
              memset (&keys[i], 0, sizeof (block_key_t));
              last = keys[i].offset + keys[i].length;
              /* Find the next empty slot, which might be slot i. */
              j++;
              for (; j <= i; j++)
                {
                  if (memcmp (&keys[j], &null_key, sizeof (block_key_t)) == 0)
                    break;
                }
            }
        }
    }

  store_log (STORE_SPLIT, "going to re-rscode from %d, %d; and from %d, %d",
             sizeof (block_header_t), numkeys * sizeof (block_key_t),
             store_offset_of_chunk_number (store, 0),
             store_offset_of_chunk_number (store, numkeys - 1)
             - store_offset_of_chunk_number (store, 0));

  find_changed_subblocks (sizeof (block_header_t), numkeys * sizeof (block_key_t),
                          &begin, &end);
  generate_rscode (state->rs, store, begin, end);
  find_changed_subblocks (store_offset_of_chunk_number (store, 0),
                          store_offset_of_chunk_number (store, numkeys - 1)
                          - store_offset_of_chunk_number (store, 0),
                          &begin, &end);
  generate_rscode (state->rs, store, begin, end);
}

static size_t
store_used_extent (store_t *store)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  int i;

  for (i = 0; i < header->chunk_count; i++)
    {
      if (memcmp (&keys[i], &null_key, sizeof (block_key_t)) == 0)
        return store_offset_of_chunk (store, keys[i].offset + keys[i].length);
    }
  return store_offset_of_chunk (store, (keys[header->chunk_count - 1].offset
                                        + keys[header->chunk_count - 1].length));
}

static int
split_next_store (store_state_t *state)
{
  store_sb_t *sb = state->data.data;
  uint64_t next_id;
  store_t next, curr;
  block_header_t *currhdr;
  block_key_t *currkeys;
  int i;
  int count = 0, moved = 0;
  uint64_t limit = (1ULL << sb->i) - 1;
  int begin, end;
  clock_t clk;

  store_trace ("n: %llu, limit: %llu", sb->n, limit);

  next_id = (1ULL << (sb->i)) + sb->n;
  create_new_block (state, next_id);

  store_log (STORE_SPLIT, "splitting store %llu into %llu", sb->n, next_id);

  b64_encode (sb->n, curr.id); 
  if (store_open (state, &curr) != 0)
    return -1;
  currhdr = (block_header_t *) curr.data.data;
  currkeys = currhdr->keys;

  b64_encode (next_id, next.id);
  if (store_open (state, &next) != 0)
    return -1;

  clk = clock();
  for (i = 0; i < currhdr->chunk_count; i++)
    {
      if (memcmp (&currkeys[i], &null_key, sizeof (block_key_t)) != 0)
        {
          uint64_t x = do_map_key (state, &currkeys[i].id, sb->n + 1);
          count++;
          if (x != sb->n) /* Found one to move. */
            {
              store_trace ("old: %llu, new: %llu, x: %llu key: %02x%02x%02x%02x",
                           sb->n, next_id, x, currkeys[i].id.strong[0],
                           currkeys[i].id.strong[1], currkeys[i].id.strong[2],
                           currkeys[i].id.strong[3]);
              assert (x == next_id);
              store_put_into_int (&next, &currkeys[i].id,
                                  store_data_base (&curr) + currkeys[i].offset,
                                  currkeys[i].length, 0, NULL);
              /* Erase the key from the original block. */
              memset (&currkeys[i], 0, sizeof (block_key_t));
              moved++;
            }
        }
    }
  clk = clock() - clk;
  store_log (STORE_PERF, "moving %d blocks took %f seconds",
             moved, (double) clk / (double) CLOCKS_PER_SEC);

  store_log (STORE_SPLIT, "I think I need to rs-encode keys from 0, %lu, and values from %ld, %ld",
             sizeof (block_header_t) + (moved * sizeof (block_key_t)),
             store_offset_of_chunk_number (&next, 0),
             store_offset_of_chunk_number (&next, moved - 1)
             - store_offset_of_chunk_number (&next, 0));
  store_log (STORE_SPLIT, "%d %d %d", moved, store_offset_of_chunk_number(&next, 0),
             store_offset_of_chunk_number (&next, moved));

  find_changed_subblocks (0, sizeof (block_header_t) + (moved * sizeof (block_key_t)),
                          &begin, &end);
  generate_rscode (state->rs, &next, begin, end);
  find_changed_subblocks (store_offset_of_chunk_number (&next, 0),
                          store_offset_of_chunk_number (&next, moved - 1)
                          - store_offset_of_chunk_number (&next, 0),
                          &begin, &end);
  generate_rscode (state->rs, &next, begin, end);
                          
  /* If we've split all 0..2^i-1 stores, increment i and start over. */
  if (sb->n == limit)
    {
      store_trace ("incrementing i");
      sb->i++;
      sb->n = 0;
    }
  else
    sb->n++;

  /* Compact the block that we moved entries out of. */
  compact_block (state, &curr);

  store_close (state, &curr);
  store_close (state, &next);

  store_log (STORE_SPLIT, "moved %d out of %d chunks, ratio: %f; n is %llu, i is %u",
             moved, count, (double) moved / (double) count,
             sb->n, sb->i);

  return 0;
}

int
store_init (const char *rootdir, store_state_t **state)
{
  struct stat statbuf;
  char *path;
  size_t len = strlen (rootdir) + strlen(STORE_SUPERBLOCK) + 2;
  store_state_t *st = NULL;
  int create = 0;
  size_t pagesize = getpagesize();

  *state = NULL;

  path = (char *) malloc (len);
  store_trace ("malloc %ld -> %p", len, path);
  if (path == NULL)
    return -1;
  snprintf (path, len, "%s/%s", rootdir, STORE_SUPERBLOCK);

  store_trace ("path is %s", path);

  st = (store_state_t *) malloc (sizeof (struct store_state_s));
  store_trace ("new store -> %p", st);
  if (st == NULL)
    {
      free (path);
      return -1;
    }

  st->rootdir = strdup (rootdir);
  if (st->rootdir == NULL)
    {
      store_perror ("strdup");
      free (st);
      free (path);
      return -1;
    }

  memset (st->cache, 0, sizeof (st->cache));

  st->rs = make_rs_handle();

  if (stat (path, &statbuf) != 0)
	{
	  if (errno != ENOENT)
        {
          store_perror ("stat");
          free (st);
          free (path);
          return -1;
        }
	  create = 1;
	}

  store_trace ("will create a store? %s", create ? "yes" : "no");

  st->data.fd = open (path, O_RDWR | O_CREAT, 0600);
  if (st->data.fd < 0)
    {
      store_perror ("open(%s)", path);
      free (st);
      free (path);
      return -1;
    }

  free (path);

  if (create)
	{
	  if (ftruncate (st->data.fd, (off_t) sizeof (struct store_sb_s)) != 0)
		{
		  free (st);
		  close (st->data.fd);
		  return -1;
		}
	}

  st->data.length = align_up (sizeof (struct store_sb_s), pagesize);
  st->data.data = mmap (NULL, st->data.length, PROT_READ | PROT_WRITE,
						MAP_SHARED, st->data.fd, 0);
  if (st->data.data == (void *) -1)
    {
      store_perror ("mmap(%d, %ld)", st->data.fd, st->data.length);
      close (st->data.fd);
      free (st);
      return -1;
    }

  if (create)
	{
	  store_sb_t *sb = (store_sb_t *) st->data.data;
	  memcpy (sb->header, superblock_header, 4);
	  sb->version = 1;
	  sb->i = 0;
	  sb->n = 0;

      create_new_block (st, 0);
	}

  {
    store_sb_t *sb = (store_sb_t *) st->data.data;
    store_trace ("created store i:%d n:%llu", sb->i, sb->n);
  }

  *state = st;
  return 0;
}

void
store_destroy (store_state_t *state)
{
  store_trace ("%p", state);
  if (state)
    {
      store_trace ("munmap (%p, %ld)", state->data.data, state->data.length);
      munmap (state->data.data, state->data.length);
      store_trace ("close (%d)", state->data.fd);
      close (state->data.fd);
      rslib_free_rs (state->rs);
      free (state);
    }
}

int
store_open (store_state_t *state, store_t *store)
{
  struct stat st;
  char *path;
  int len = strlen(state->rootdir) + strlen (ARROW_BLOCKS_DIR) + STORE_ID_LEN + 3;
  size_t pagesize = getpagesize();
  int i;
  int added_to_cache = 0;

  for (i = 0; i < STORE_CACHE_SIZE; i++)
    {
      if (strcmp (store->id, state->cache[i].entry.id) == 0)
        {
          state->cache[i].refs++;
          store->data.fd = state->cache[i].entry.data.fd;
          store->data.data = state->cache[i].entry.data.data;
          store->data.length = state->cache[i].entry.data.length;
          return 0;
        }
    }

  path = (char *) malloc (len);
  if (path == NULL)
    return -1;

  snprintf (path, len, "%s/%s/%s", state->rootdir, ARROW_BLOCKS_DIR, store->id);

  store->data.fd = open (path, O_RDWR);
  if (store->data.fd < 0)
    {
      free (path);
      return -1;
    }

  free (path);
  if (fstat (store->data.fd, &st) != 0)
    {
      close (store->data.fd);
      return -1;
    }

  store->data.length = align_up ((size_t) st.st_size, pagesize);
  store->data.data = mmap (NULL, store->data.length, PROT_READ | PROT_WRITE,
						   MAP_SHARED, store->data.fd, 0);
  if (store->data.data == (void *) -1)
	{
	  close(store->data.fd);
	  return -1;
	}

  for (i = 0; i < STORE_CACHE_SIZE; i++)
    {
      if (state->cache[i].refs == 0)
        {
          if (strlen (state->cache[i].entry.id) == 0)
            {
              strcpy (state->cache[i].entry.id, store->id);
              state->cache[i].entry.data.fd = store->data.fd;
              state->cache[i].entry.data.data = store->data.data;
              state->cache[i].entry.data.length = store->data.length;
              state->cache[i].refs = 1;
              return 0;
            }
        }
    }

  /* FIXME - should do LRU or something. */

  for (i = 0; i < STORE_CACHE_SIZE; i++)
    {
      if (state->cache[i].refs == 0) /* empty cache slot */
        {
          if (state->cache[i].entry.data.data != NULL) /* Valid cache entry, no refs. */
            {
              munmap (state->cache[i].entry.data.data, state->cache[i].entry.data.length);
              state->cache[i].entry.data.data = NULL;
              if (state->cache[i].entry.data.fd != -1)
                close (state->cache[i].entry.data.fd);
            }
          strcpy (state->cache[i].entry.id, store->id);
          state->cache[i].entry.data.fd = store->data.fd;
          state->cache[i].entry.data.data = store->data.data;
          state->cache[i].entry.data.length = store->data.length;
          state->cache[i].refs = 1;
          close (state->cache[i].entry.data.fd);
          state->cache[i].entry.data.fd = -1;
          return 0;
        }
    }

  return 0;
}

int
store_close (store_state_t *state, store_t *store)
{
  int i;

  for (i = 0; i < STORE_CACHE_SIZE; i++)
    {
      if (strcmp (store->id, state->cache[i].entry.id) == 0)
        {
          state->cache[i].refs--;
          return 0;
        }
    }
  if (store)
	{
	  munmap (store->data.data, store->data.length);
      store->data.data = NULL;
      if (store->data.fd != -1)
        close (store->data.fd);
	}
  return 0;
}

void
store_map_key (store_state_t *state, const arrow_id_t *id, char *result)
{
  store_sb_t *sb = (store_sb_t *) state->data.data;
  uint64_t key = do_map_key (state, id, sb->n);
  b64_encode (key, result);
}

int
store_put (store_state_t *state, const arrow_id_t *id, const void *buf, size_t len)
{
  store_t store;
  int ret;
  double loadfactor;

  store_map_key (state, id, store.id);
  store_log (STORE_TRACE, "mapped key %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x to %s",
             id->strong[ 8], id->strong[ 9], id->strong[10], id->strong[11],
             id->strong[12], id->strong[13], id->strong[14], id->strong[15],
             store.id);

  if (store_open (state, &store) != 0)
    {
      store_perror ("store_open");
      return -1;
    }

  ret = store_put_into_int (&store, id, buf, len, 1, state->rs);
  if (ret < 0)
    return ret;

  loadfactor = store_load_factor (&store);
  store_trace ("load factor now %f", loadfactor);

  store_close (state, &store);
  if (loadfactor > MAX_LOAD_FACTOR)
    split_next_store (state);

  return ret;
}

int
store_addref (store_state_t *state, const arrow_id_t *id)
{
  store_t store;
  int ret;
  double loadfactor;

  store_map_key (state, id, store.id);
  store_trace ("mapped key %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x to %s",
               id->strong[ 8], id->strong[ 9], id->strong[10], id->strong[11],
               id->strong[12], id->strong[13], id->strong[14], id->strong[15],
               store.id);

  if (store_open (state, &store) != 0)
    {
      store_perror ("store_open");
      return -1;
    }

  ret = store_addref_to (&store, id);
  if (ret < 0)
    return ret;

  store_close (state, &store);

  return ret;
}

size_t
store_get (store_state_t *state, const arrow_id_t *id, void *out, size_t maxlen)
{
  store_t store;
  size_t size;

  store_map_key (state, id, store.id);
  store_trace ("mapped key %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x to %s",
               id->strong[ 8], id->strong[ 9], id->strong[10], id->strong[11],
               id->strong[12], id->strong[13], id->strong[14], id->strong[15],
               store.id);

  if (store_open (state, &store) != 0)
    return -1;

  size = store_get_from (&store, id, out, maxlen);
  store_trace ("get_from result %ld", size);
  store_close (state, &store);
  return size;
}

size_t
store_get_len (store_state_t *state, const arrow_id_t *id)
{
  store_t store;
  size_t size;

  store_map_key (state, id, store.id);
  if (store_open (state, &store) != 0)
    return -1;

  size = store_get_len_from (&store, id);
  store_close (state, &store);
  return size;
}

int
store_contains (store_state_t *state, const arrow_id_t *id)
{
  store_t store;
  block_header_t *header;
  block_key_t *keys;
  int i;

  store_map_key (state, id, store.id);
  if (store_open (state, &store) != 0)
    return 0;

  header = (block_header_t *) store.data.data;
  keys = header->keys;

  for (i = 0; i < header->chunk_count; i++)
    {
      if (arrow_id_cmp (id, &keys[i]) == 0)
        {
          store_close (state, &store);
          return 1;
        }
    }
  store_close (state, &store);
  return 0;
}

int
store_put_into (store_t *store, const arrow_id_t *id, const void *buf, size_t len)
{
  struct rs_handle *rs = make_rs_handle();
  return store_put_into_int (store, id, buf, len, 1, rs);
}

static int
store_put_into_int (store_t *store, const arrow_id_t *id, const void *buf, size_t len,
                    int gen_rs, struct rs_handle *rs)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  int i = 0;
  size_t offset = 0; /* Is the point in store->data.data where we *can't* store data. */

  store_trace ("%p %p %p", store, header, keys);

  for (i = 0; i < header->chunk_count; i++)
	{
	  if (memcmp (&keys[i].id, id, sizeof (arrow_id_t)) == 0)
		{
          int begin, end;
		  /* We believe that it is already there. */
          keys[i].references++;
          store_trace ("put again, num references: %d", keys[i].references);
          if (gen_rs)
            {
              find_changed_subblocks(store_offset_of_key (store, i),
                                     sizeof (block_key_t), &begin, &end);
              generate_rscode (rs, store, begin, end);
            }
		  return 1;
		}
	  else if (memcmp (&keys[i], &null_key, sizeof (block_key_t)) == 0)
		{
          size_t remain = 0;
          store_trace ("found a slot at %d", i);
          if (i < header->chunk_count - 1
              || memcmp (&keys[i+1], &null_key, sizeof (block_key_t)) == 0)
            remain = header->alloc_size - store_header_size (store) - offset;
          else
            remain = keys[i+1].offset - offset;

          store_trace ("we have %ld bytes in this slot", remain);

          if (remain >= len)
            {
              int begin, end;
              memcpy (&keys[i].id, id, sizeof (arrow_id_t));
              keys[i].offset = offset;
              keys[i].length = len;
              keys[i].references = 1;
              memcpy (store_data_base (store) + offset, buf, len);
              store_trace ("placed %ld bytes at %ld", len, offset);
              if (gen_rs)
                {
                  find_changed_subblocks(store_offset_of_key (store, i),
                                         sizeof (block_key_t), &begin, &end);
                  generate_rscode (rs, store, begin, end);
                  find_changed_subblocks(store_offset_of_chunk (store, offset),
                                         len, &begin, &end);
                  generate_rscode (rs, store, begin, end);
                }
              return 0;
            }
        }
      else
        {
          offset += keys[i].length;
          store_trace ("keep looking; offset now %ld", offset);
        }
	}

  fail ("FIXME - implement growing stores");
  return -1;
}

int
store_addref_to (store_t *store, const arrow_id_t *id)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  int i;

  for (i = 0; i < header->chunk_count; i++)
    {
      if (arrow_id_cmp (&keys[i].id, id) == 0)
        {
          keys[i].references++;
          return 0;
        }
    }
  return -1;
}

size_t
store_get_from (store_t *store, const arrow_id_t *id, void *out, size_t maxlen)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  int i;

  for (i = 0; i < header->chunk_count; i++)
	{
	  if (memcmp (&keys[i].id, id, sizeof (arrow_id_t)) == 0)
		{
          store_trace ("found key at %d", i);
		  memcpy (out, store_data_base (store) + keys[i].offset,
				  keys[i].length < maxlen ? keys[i].length : maxlen);
		  return keys[i].length;
		}
	}

  return -1;
}

size_t
store_get_len_from (store_t *store, const arrow_id_t *id)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  int i;

  for (i = 0; i < header->chunk_count; i++)
	{
	  if (memcmp (&keys[i].id, id, sizeof (struct arrow_id_s)) == 0)
		return keys[i].length;
	}

  return -1;
}

int
store_verify_all (store_state_t *state)
{
  size_t pathlen;
  char *path;
  DIR *dir;
  store_t store;
  struct dirent *dent;
  int failures = 0;

  pathlen = strlen (state->rootdir) + strlen (ARROW_BLOCKS_DIR) + 2;
  path = (char *) malloc (pathlen);
  snprintf (path, pathlen, "%s/%s", state->rootdir, ARROW_BLOCKS_DIR);

  dir = opendir (path);
  if (dir == NULL)
    {
      free (path);
      return -1;
    }

  free (path);

  while ((dent = readdir (dir)) != NULL)
    {
      if (strcmp (dent->d_name, ".") == 0 || strcmp (dent->d_name, "..") == 0)
        continue;
      strcpy (store.id, dent->d_name);
      if (store_open (state, &store) != 0)
        continue;
      if (store_verify (&store, NULL) != 0)
        failures++;
      store_close (state, &store);
    }
  return failures;
}

/*

  EPIC FAIL WARNING

  Our block key might get corrupt in the length or offset field, which
  will render references to that block useless, so we can't verify the
  block hash. So, we need to be CAREFUL about 

 */

static uint32_t
compute_weak_key (store_t *store, block_key_t *key)
{
  block_header_t *header = (block_header_t *) store->data.data;
  Rollsum rs;

  if (key->offset > header->alloc_size || key->offset + key->length > header->alloc_size)
    {
      /* Guaranteed corrupt offset or length field, abort! */
      return 0;
    }

  RollsumInit (&rs);
  RollsumUpdate (&rs, store_data_base (store) + key->offset,
                 key->length);
  return RollsumDigest(&rs);
}

static void
compute_strong_key (store_t *store, block_key_t *key, uint8_t *digest)
{
  block_header_t *header = (block_header_t *) store->data.data;

  if (key->offset > header->alloc_size || key->offset + key->length > header->alloc_size)
    {
      /* Guaranteed corrupt offset or length field, abort! */
      memset (digest, 0, MD5_DIGEST_LENGTH);
      return;
    }

  MD5 (store_data_base (store) + key->offset, key->length, digest);
}

static int
verify_weak_key (store_t *store, int i)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  Rollsum rs;

  RollsumInit (&rs);
  RollsumUpdate (&rs, store_data_base (store) + keys[i].offset,
                 keys[i].length);
  if (keys[i].id.weak != RollsumDigest (&rs))
    {
      store_trace ("weak sum mismatch %u vs. %lu", keys[i].id.weak,
                   RollsumDigest (&rs));
      return 0;
    }
  return 1;
}

static int
verify_strong_key (store_t *store, int i)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  uint8_t digest[MD5_DIGEST_LENGTH];  
  MD5 (store_data_base (store) + keys[i].offset,
       keys[i].length, digest);
  if (memcmp (keys[i].id.strong, digest, MD5_DIGEST_LENGTH) != 0)
    {
      store_trace ("strong sum mismatch %02x:%02x:%02x:%02x:%02x:%02x:"
                   "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x "
                   "vs. %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:"
                   "%02x:%02x:%02x:%02x:%02x:%02x:%02x",
                   keys[i].id.strong[ 0], keys[i].id.strong[ 1],
                   keys[i].id.strong[ 2], keys[i].id.strong[ 3],
                   keys[i].id.strong[ 4], keys[i].id.strong[ 5],
                   keys[i].id.strong[ 6], keys[i].id.strong[ 7],
                   keys[i].id.strong[ 8], keys[i].id.strong[ 9],
                   keys[i].id.strong[10], keys[i].id.strong[11],
                   keys[i].id.strong[12], keys[i].id.strong[13],
                   keys[i].id.strong[14], keys[i].id.strong[15],
                   digest[ 0], digest[ 1], digest[ 2], digest[ 3],
                   digest[ 4], digest[ 5], digest[ 6], digest[ 7],
                   digest[ 8], digest[ 9], digest[10], digest[11],
                   digest[12], digest[13], digest[14], digest[15]);
      return 0;
    }
  return 1;
}

int
store_verify (store_t *store, store_error_t *errors)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  int i;
  int found_errors = 0;
  if (errors != NULL)
    {
      errors->count = 0;
      errors->keys = NULL;
    }

  for (i = 0; i < header->chunk_count; i++)
    {
      if (memcmp (&keys[i], &null_key, sizeof (block_key_t)) != 0)
        {
          if (!verify_weak_key (store, i))
            {
              found_errors++;
              if (errors != NULL)
                {
                  errors->count++;
                  if (errors->keys == NULL)
                    errors->keys = (int *) malloc (sizeof (int));
                  else
                    errors->keys = (int *) realloc (errors->keys, sizeof (int) * errors->count);
                  if (errors->keys != NULL)
                    errors->keys[errors->count - 1] = i;
                }
              continue;
            }
          if (!verify_strong_key (store, i))
            {              
              found_errors++;
              if (errors != NULL)
                {
                  errors->count++;
                  if (errors->keys == NULL)
                    errors->keys = (int *) malloc (sizeof (int));
                  else
                    errors->keys = (int *) realloc (errors->keys, sizeof (int) * errors->count);
                  if (errors->keys != NULL)
                    errors->keys[errors->count - 1] = i;
                }
            }
        }
    }
  return found_errors;
}

typedef uint8_t codeword_t[RS_CODEWORD_SIZE];

static int
try_fix_key (store_t *store, int idx)
{
  return -1;
/*   block_header_t *header = (block_header_t *) store->data.data; */
/*   block_key_t *keys = header->keys; */
/*   codeword_t *codewords; */
/*   codeword_t saved_codeword; */
/*   block_key_t *key; */
/*   size_t sb_len; */
/*   int begin, end; */
/*   int i, j; */
/*   uint8_t erasures[RS_CODEWORD_SIZE]; */
/*   struct rs_handle *rs = make_rs_handle(); */
/*   uint8_t syndrome[RS_PARITY_SIZE]; */

/*   find_changed_subblocks (store_offset_of_key (store, idx), sizeof (block_key_t), */
/*                           &begin, &end); */

/*   sb_len = (end - begin) * RS_CODEWORD_SIZE; */
/*   codewords = (codeword_t *) malloc (sb_len); */
/*   if (codewords == NULL) */
/*     return 0; */

/*   memcpy (codewords, store->data.data + (begin * RS_CODEWORD_SIZE), sb_len); */
/*   key = (block_key_t *) (((uint8_t *) codewords) */
/*                          + (store_offset_of_key(store, idx) - (begin * RS_CODEWORD_SIZE))); */

/*   store_trace ("trying RS fixup for block key %d, subblocks %d thru %d", */
/*                idx, begin, end); */

/*   for (i = 0; i < end - begin; i++) */
/*     { */
/*       rslib_parity_check (rs, (symbol_t *) &codewords[i], get_parity_bytes (store, begin+i), syndrome); */
/*       for (j = 0; j < RS_CODEWORD_SIZE; j++) */
/*         { */
/*           memcpy (&saved_codeword, &codewords[i], sizeof (codeword_t)); */
/*           memset (&erasures, 0, sizeof (erasures)); */
/*           erasures[j] = 1; */
/*           if (rslib_decode (rs, &codewords[i], erasures, 1) >= 0) */
/*             { */
/*               if (compute_weak_key (store, key) == key->id.weak) */
/*                 { */
/*                   uint8_t md5[MD5_DIGEST_LENGTH]; */
/*                   compute_strong_key (store, key, md5); */
/*                   if (memcmp (md5, key->id.strong, MD5_DIGEST_LENGTH) == 0) */
/*                     { */
/*                       /\* Happy day! *\/ */
/*                       store_trace ("SUCCESS fixing block key %d in subblock %d", */
/*                                    idx, i + begin); */
/*                       memcpy (store->data.data + (begin * RS_CODEWORD_SIZE), */
/*                               codewords, sb_len); */
/*                       free (codewords); */
/*                       rslib_free_rs (rs); */
/*                       return 1; */
/*                     } */
/*                 } */
/*             } */
/*           memcpy (&codewords[i], &saved_codeword, sizeof (codeword_t)); */
/*         } */
/*     } */

/*   rslib_free_rs (rs); */
/*   store_trace ("failed to fixup block key %d", i); */
/*   free (codewords); */
/*   return 0; */
}

static int
try_fix_value (store_t *store, int index)
{
  return -1;
/*   block_header_t *header = (block_header_t *) store->data.data; */
/*   block_key_t *keys = header->keys; */
/*   codeword_t *codewords; */
/*   codeword_t saved_codeword; */
/*   block_key_t *key; */
/*   size_t sb_len; */
/*   int begin, end; */
/*   int i, j; */
/*   uint8_t erasures[RS_CODEWORD_SIZE]; */
/*   struct rs_handle *rs = make_rs_handle(); */
/*   uint8_t syndrome[RS_PARITY_SIZE]; */

/*   find_changed_subblocks (store_offset_of_chunk (store, keys[index].offset), */
/*                           keys[index].length, &begin, &end); */

/*   sb_len = (end - begin) * RS_CODEWORD_SIZE; */
/*   codewords = (codeword_t *) malloc (sb_len); */
/*   if (codewords == NULL) */
/*     return 0; */

/*   memcpy (codewords, store->data.data + (begin * RS_CODEWORD_SIZE), sb_len); */
/*   key = &keys[index]; */

/*   store_trace ("trying RS fixup for block value %d, subblocks %d thru %d", */
/*                index, begin, end); */

/*   for (i = 0; i < end - begin; i++) */
/*     { */
/*       rslib_parity_check (rs, (symbol_t *) &codewords[i], get_parity_bytes (store, begin+i), syndrome); */
/*       for (j = 0; j < RS_CODEWORD_SIZE; j++) */
/*         { */
/*           memcpy (&saved_codeword, &codewords[i], sizeof (codeword_t)); */
/*           memset (&erasures, 0, sizeof (erasures)); */
/*           erasures[j] = 1; */
/*           if (rslib_decode (rs, &codewords[i], erasures, 1) >= 0) */
/*             { */
/*               if (compute_weak_key (store, key) == key->id.weak) */
/*                 { */
/*                   uint8_t md5[MD5_DIGEST_LENGTH]; */
/*                   compute_strong_key (store, key, md5); */
/*                   if (memcmp (md5, key->id.strong, MD5_DIGEST_LENGTH) == 0) */
/*                     { */
/*                       /\* Happy day! *\/ */
/*                       store_trace ("SUCCESS fixing block key %d in subblock %d", */
/*                                    index, i + begin); */
/*                       memcpy (store->data.data + (begin * RS_CODEWORD_SIZE), */
/*                               codewords, sb_len); */
/*                       free (codewords); */
/*                       rslib_free_rs (rs); */
/*                       return 1; */
/*                     } */
/*                 } */
/*             } */
/*           memcpy (&codewords[i], &saved_codeword, sizeof (codeword_t)); */
/*         } */
/*     } */

/*   rslib_free_rs (rs); */
/*   store_trace ("failed to fixup block key %d", i); */
/*   free (codewords); */
/*   return 0; */
}

int
store_repair (store_t *store, store_error_t *errors)
{
  int i;
  int numfixed = 0;

  for (i = 0; i < errors->count; i++)
    {
      if (try_fix_key (store, errors->keys[i]))
        {
          numfixed++;
          continue;
        }
      if (try_fix_value (store, errors->keys[i]))
        numfixed++;
    }
  return numfixed;
}

void
store_dump (FILE *out, store_state_t *store)
{
  store_sb_t *sb = (store_sb_t *) store->data.data;
  fprintf (out, "Store root dir: %s\n", store->rootdir);
  fprintf (out, "Store header: %c%c%c%c; version: %u\n", sb->header[0],
           sb->header[1], sb->header[2], sb->header[3], sb->version);
  fprintf (out, "i: %d; n: %llu\n", sb->i, sb->n);
}

void
store_dump_store (FILE *out, store_t *store)
{
  block_header_t *header = (block_header_t *) store->data.data;
  block_key_t *keys = header->keys;
  int i;

  fprintf (out, "Store %s:\n", store->id);
  fprintf (out, "Header: %c%c%c%c; version: %u\n", header->header[0],
           header->header[1], header->header[2], header->header[3],
           header->version);
  fprintf (out, "Chunks allocated: %u, bytes allocated: %u\n\n",
           header->chunk_count, header->alloc_size);
  for (i = 0; i < header->chunk_count; i++)
    {
      if (arrow_id_cmp (&keys[i], &null_key) != 0)
        {
          fprintf (out, " Weak: %08x  Strong: %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
                   keys[i].id.weak, keys[i].id.strong[0], keys[i].id.strong[1],
                   keys[i].id.strong[ 2], keys[i].id.strong[ 3], keys[i].id.strong[ 4],
                   keys[i].id.strong[ 5], keys[i].id.strong[ 6], keys[i].id.strong[ 7],
                   keys[i].id.strong[ 8], keys[i].id.strong[ 9], keys[i].id.strong[10],
                   keys[i].id.strong[11], keys[i].id.strong[12], keys[i].id.strong[13],
                   keys[i].id.strong[14], keys[i].id.strong[15]);
          fprintf (out, " Offset: %10d; Length: %10d; References: %5d\n\n",
                   keys[i].offset, keys[i].length, keys[i].references);
        }
    }
}

int
store_size (store_state_t *state, uint64_t *used, uint64_t *total)
{
  struct stat st;
  char *path;
  int len = strlen(state->rootdir) + strlen (ARROW_BLOCKS_DIR) + STORE_ID_LEN + 3;
  store_t store;
  uint64_t i;
  block_header_t *header;
  block_key_t *keys;
  int j;

  if (used == NULL || total == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  path = (char *) malloc (len);
  if (path == NULL)
    return -1;

  *used = 0;
  *total = 0;

  for (i = 0; ; i++)
    {
      uint64_t u = 0;
      b64_encode (i, store.id);
      snprintf (path, len, "%s/%s/%s", state->rootdir, ARROW_BLOCKS_DIR, store.id);
      if (stat (path, &st) != 0)
        break;
      if (store_open (state, &store) != 0)
        {
          free (path);
          return -1;
        }

      *total = (*total) + st.st_size;
      header = (block_header_t *) store.data.data;
      keys = header->keys;

      u = sizeof (block_header_t);
      for (j = 0; j < header->chunk_count; j++)
        {
          if (memcmp (&null_key, &keys[j], sizeof (block_key_t)) == 0)
            continue;
          u += sizeof (block_key_t);
          u += keys[j].length;
        }

      u += (align_up(u, RS_CODEWORD_SIZE) / RS_CODEWORD_SIZE) * RS_PARITY_SIZE;
      *used = *used + u;

      store_close (state, &store);
    }
  free (path);
  return 0;
}

/* Local Variables: */
/* indent-tabs-mode: nil */
/* c-basic-offset: 2 */
/* End: */
