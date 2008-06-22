/* sync.c -- 
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


#include <math.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <arrow.h>
#include <cbuf.h>
#include <fail.h>
#include <rollsum.h>
#include <fileinfo.h>
#include <store.h>
#include "sync.h"

#include <openssl/md5.h>

#define HASH_TABLE_SIZE (1 << 14)

#define SYNC_GENERATE  1
#define SYNC_FILE     (1 << 1)

#define sync_log(lvl,fmt,args...) if (SYNC_DEBUG & (lvl)) fprintf (stderr, "%s (%s:%d): " fmt "\n", __FUNCTION__, __FILE__, __LINE__, ##args)

int SYNC_DEBUG = 0;

static const arrow_id_t null_id = { 0, { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };

static void
hash_insert (arrow_id_t *table, const arrow_id_t *id)
{
  int idx = id->weak % HASH_TABLE_SIZE;
  while (1)
    {
      if (arrow_id_cmp (&table[idx], id) == 0)
        return; /* already there */
      if (arrow_id_cmp (&table[idx], &null_id) == 0) /* Found a slot. */
        {
          memcpy (&table[idx], id, sizeof (arrow_id_t));
          break;
        }
      idx++;
      if (idx >= HASH_TABLE_SIZE)
        idx = 0;
      if (idx == id->weak % HASH_TABLE_SIZE)
        fail ("FIXME hash table completely filled!!!");
    }
}

static int
hash_table_probe (arrow_id_t *table, uint32_t weaksum)
{
  int idx = weaksum % HASH_TABLE_SIZE;
  while (1)
    {
      if (table[idx].weak == weaksum)
        return 1;
      if (arrow_id_cmp (&table[idx], &null_id) == 0)
        return 0;
      idx++;
      if (idx >= HASH_TABLE_SIZE)
        idx = 0;
      if (idx == weaksum % HASH_TABLE_SIZE)
        return 0;
    }
  return 0;
}

static int
hash_table_contains (arrow_id_t *table, const arrow_id_t *id)
{
  int idx = id->weak % HASH_TABLE_SIZE;
  while (1)
    {
      if (arrow_id_cmp (&table[idx], id) == 0)
        return 1;
      if (arrow_id_cmp (&table[idx], &null_id) == 0)
        return 0;
      idx++;
      if (idx >= HASH_TABLE_SIZE)
        idx = 0;
      if (idx == id->weak % HASH_TABLE_SIZE)
        return 0;
    }
  return 0;
}

int
sync_generate (file_t *file, FILE *in, sync_callbacks_t *cb)
{
  uint8_t *buffer;
  int ret;
  uint32_t bufsize;
  struct stat st;
  file_chunk_t chunk;
  MD5_CTX md5;

  if (fstat (fileno (in), &st) != 0)
	return -1;

  if ((st.st_mode & S_IFREG) == 0)
	return -1;

  MD5_Init (&md5);

  bufsize = (uint32_t) sqrtl ((long double) st.st_size);
  if (bufsize < MIN_CHUNK_SIZE)
	bufsize = MIN_CHUNK_SIZE;
  if (bufsize > MAX_CHUNK_SIZE)
	bufsize = MAX_CHUNK_SIZE;

  sync_log (SYNC_GENERATE, "block size is %u", bufsize);

  file->file->chunk_size = bufsize;
  buffer = (uint8_t *) malloc (bufsize);
  if (buffer == NULL)
	return -1;

/*   fseek (out, sizeof (file_info_t), SEEK_SET); */

  while (!feof (in))
	{
	  ret = fread (buffer, 1, bufsize, in);
	  if (ret <= 0)
		break;
	  MD5_Update (&md5, buffer, ret);
	  if (ret <= MAX_DIRECT_CHUNK_SIZE)
		{
		  sync_log (SYNC_GENERATE, "DIRECT block of size %d", ret);
		  chunk.type = DIRECT_CHUNK;
		  chunk.chunk.data.length = ret;
		  memcpy (chunk.chunk.data.data, buffer, ret);
		  cb->emit_chunk (cb->state, &chunk);
/* 		  fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
		}
	  else
		{
		  sync_log (SYNC_GENERATE, "REFERENCE block of size %d", ret);
		  chunk.type = REFERENCE;
		  chunk.chunk.ref.length = ret;
		  arrow_compute_key (&(chunk.chunk.ref.ref), buffer, ret);
		  cb->emit_chunk (cb->state, &chunk);
/* 		  fwrite (&chunk, sizeof (file_chunk_t), 1, out); */

		  sync_log (SYNC_GENERATE, "key generated: %08x %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
					chunk.chunk.ref.ref.weak,
					chunk.chunk.ref.ref.strong[ 0], chunk.chunk.ref.ref.strong[ 1],
					chunk.chunk.ref.ref.strong[ 2], chunk.chunk.ref.ref.strong[ 3],
					chunk.chunk.ref.ref.strong[ 4], chunk.chunk.ref.ref.strong[ 5],
					chunk.chunk.ref.ref.strong[ 6], chunk.chunk.ref.ref.strong[ 7],
					chunk.chunk.ref.ref.strong[ 8], chunk.chunk.ref.ref.strong[ 9],
					chunk.chunk.ref.ref.strong[10], chunk.chunk.ref.ref.strong[11],
					chunk.chunk.ref.ref.strong[12], chunk.chunk.ref.ref.strong[13],
					chunk.chunk.ref.ref.strong[14], chunk.chunk.ref.ref.strong[15]);

		  if (!cb->store_contains (cb->state, &(chunk.chunk.ref.ref)))
			cb->put_block (cb->state, &(chunk.chunk.ref.ref), buffer, ret);
		}
	}

  memset (&chunk, 0, sizeof (file_chunk_t));
  chunk.type = END_OF_CHUNKS;
  cb->emit_chunk (cb->state, &chunk);
/*   fwrite (&chunk, sizeof (file_chunk_t), 1, out); */

  MD5_Final (file->file->hash, &md5);

  free (buffer);
  return 0;
}

int
sync_file (file_t *basis, file_t *newfile, FILE *datafile, sync_callbacks_t *cb,
           int *hash_match)
{
  arrow_id_t *table;
  int i = 0;
  circular_buffer_t buffer;
  int bufsize;
  arrow_id_t current;
  Rollsum runsum;
  int ret;
  MD5_CTX md5;
  off_t last_match = 0;
  int chunk_size = basis->file->chunk_size;
  file_chunk_t chunk;
  int matches = 0;

  sync_log (SYNC_FILE, "sync file %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:"
			"%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x to file %02x:%02x:%02x:"
			"%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x,"
			" chunk size %d",
			basis->uuid[ 0], basis->uuid[ 1],
			basis->uuid[ 2], basis->uuid[ 3],
			basis->uuid[ 4], basis->uuid[ 5],
			basis->uuid[ 6], basis->uuid[ 7],
			basis->uuid[ 8], basis->uuid[ 9],
			basis->uuid[10], basis->uuid[11],
			basis->uuid[12], basis->uuid[13],
			basis->uuid[14], basis->uuid[15],
			newfile->uuid[ 0], newfile->uuid[ 1],
			newfile->uuid[ 2], newfile->uuid[ 3],
			newfile->uuid[ 4], newfile->uuid[ 5],
			newfile->uuid[ 6], newfile->uuid[ 7],
			newfile->uuid[ 8], newfile->uuid[ 9],
			newfile->uuid[10], newfile->uuid[11],
			newfile->uuid[12], newfile->uuid[13],
			newfile->uuid[14], newfile->uuid[15],
			chunk_size);

  if (hash_match != NULL && *hash_match != 0)
    {
      unsigned char filebuf[1024];
      MD5_CTX filemd5;
      unsigned char filemd[MD5_DIGEST_LENGTH];
      MD5_Init (&filemd5);
      while (!feof (datafile))
        {
          int ret = fread (filebuf, 1, 1024, datafile);
          if (ret > 0)
            MD5_Update (&filemd5, filebuf, ret);
          else
            break;
        }
      MD5_Final (filemd, &filemd5);
      if (memcmp (filemd, basis->file->hash, MD5_DIGEST_LENGTH) == 0)
        {
          sync_log (SYNC_FILE, "file MD5 matches; skipping this file");
          *hash_match = 1;
          return 0;
        }
      rewind (datafile);
    }

  if (hash_match != NULL)
    *hash_match = 0;

  MD5_Init (&md5);

  table = (arrow_id_t *) malloc (sizeof (arrow_id_t) * HASH_TABLE_SIZE);
  if (table == NULL)
    {
      return -1;
    }
  memset (table, 0, sizeof (arrow_id_t) * HASH_TABLE_SIZE);

  if (cbuf_alloc (&buffer, chunk_size) != 0)
	{
	  free (table);
	  return -1;
	}

  i = 0;
  while (basis->file->chunks[i].type != END_OF_CHUNKS)
    {
      if (basis->file->chunks[i].type == REFERENCE
		  && basis->file->chunks[i].chunk.ref.length == chunk_size)
        hash_insert (table, &(basis->file->chunks[i].chunk.ref.ref));
	  i++;
    }

  newfile->file->chunk_size = chunk_size;
  uuid_copy (newfile->file->previous, basis->uuid);

/*   fseek (out, sizeof (file_info_t), SEEK_SET); */
  bufsize = fread (buffer.buffer, 1, chunk_size, datafile);
  if (bufsize < 0)
	{
	  free (table);
	  cbuf_free (&buffer);
	  return -1;
	}
  MD5_Update (&md5, buffer.buffer, bufsize);
  if (bufsize < chunk_size) /* File is smaller than a chunk. */
    {
	  sync_log (SYNC_FILE, "file size %d is smaller than chunk size %d",
				bufsize, chunk_size);
	  if (bufsize <= MAX_DIRECT_CHUNK_SIZE)
        {
		  sync_log (SYNC_FILE, "DIRECT chunk of %d bytes", bufsize);
		  /* note: struct naming is awful here. */
          chunk.type = DIRECT_CHUNK;
          chunk.chunk.data.length = MIN(bufsize, MAX_DIRECT_CHUNK_SIZE);
          memcpy (chunk.chunk.data.data, buffer.buffer, chunk.chunk.data.length);
		  cb->emit_chunk (cb->state, &chunk);
/*           fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
        }
	  else
		{
		  sync_log (SYNC_FILE, "REFERENCE chunk of %d bytes", bufsize);
		  arrow_compute_key (&current, buffer.buffer, bufsize);
		  chunk.type = REFERENCE;
		  chunk.chunk.ref.length = bufsize;
		  memcpy (&(chunk.chunk.ref.ref), &current, sizeof (arrow_id_t));
		  cb->emit_chunk (cb->state, &chunk);
/* 		  fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
		  if (cb->store_contains (cb->state, &current))
			cb->add_ref (cb->state, &current);
		  else
			cb->put_block (cb->state, &current, buffer.buffer, bufsize);
		}

	  memset (&chunk, 0, sizeof (file_chunk_t));
	  chunk.type = END_OF_CHUNKS;
	  cb->emit_chunk (cb->state, &chunk);
/* 	  fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
      MD5_Final (newfile->file->hash, &md5);
	  free (table);
	  cbuf_free (&buffer);
/* 	  fclose (out); */
      return 0;
    }
  RollsumInit (&runsum);
  RollsumUpdate (&runsum, buffer.buffer, bufsize);

  while (!feof (datafile))
    {
      if (hash_table_probe (table, RollsumDigest(&runsum)))
        {
          off_t where = ftell (datafile);
		  MD5_CTX blockmd;
          current.weak = RollsumDigest(&runsum);
		  sync_log (SYNC_FILE, "probe found %u; trying MD5...", current.weak);
		  cbuf_md5 (&buffer, &blockmd, current.strong);
          if (hash_table_contains (table, &current))
            {
              off_t cur = ftell (datafile);

			  matches++;
			  sync_log (SYNC_FILE, "key FOUND at %lld; copy bytes %lld to %lld",
						(long long) cur, (long long) last_match,
						(long long) (cur - chunk_size));

			  fseek (datafile, last_match, SEEK_SET);

              /* Found a match. Record bytes before this (if any). */
              if (last_match < cur - chunk_size)
                {
                  off_t l;
                  for (l = last_match; l < cur - chunk_size; )
                    {
                      int n = MIN(cur - chunk_size - l, chunk_size);
					  fread (buffer.buffer, 1, n, datafile);
                      if (n <= MAX_DIRECT_CHUNK_SIZE)
                        {
						  sync_log (SYNC_FILE, "DIRECT chunk of %d bytes", n);
                          /* Store some direct chunks. */
						  chunk.type = DIRECT_CHUNK;
						  chunk.chunk.data.length = n;
						  memcpy (chunk.chunk.data.data, buffer.buffer,
								  chunk.chunk.data.length);
						  cb->emit_chunk (cb->state, &chunk);
/* 						  fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
                        }
                      else
                        {
                          /* Generate a chunk reference, store the chunk. */
						  sync_log (SYNC_FILE, "REFERENCE chunk of %d bytes", n);
						  arrow_compute_key (&current, buffer.buffer, n);
						  chunk.type = REFERENCE;
						  chunk.chunk.ref.length = n;
						  memcpy (&(chunk.chunk.ref.ref), &current, sizeof (arrow_id_t));
						  cb->emit_chunk (cb->state, &chunk);
/* 						  fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
						  if (cb->store_contains (cb->state, &current))
							cb->add_ref (cb->state, &current);
						  else
							cb->put_block (cb->state, &current, buffer.buffer, n);
                        }
					  l += n;
                    }
                }

			  cbuf_reset (&buffer);

			  sync_log (SYNC_FILE, "REFERENCE chunk of %d bytes", chunk_size);
              /* Record the chunk reference. */
              chunk.type = REFERENCE;
              chunk.chunk.ref.length = bufsize;
              memcpy (&(chunk.chunk.ref.ref), &current, sizeof (arrow_id_t));
			  cb->emit_chunk (cb->state, &chunk);
/*               fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
			  cb->add_ref (cb->state, &(chunk.chunk.ref.ref));

			  /* Advance past the block we just matched. */
			  fseek (datafile, chunk_size, SEEK_CUR);
			  last_match = ftell (datafile);
			  sync_log (SYNC_FILE, "last_match is %lld", (long long) last_match);

              /* Read the next chunk, reset the weak sum. */
			  bufsize = fread (buffer.buffer, 1, chunk_size, datafile);
			  MD5_Update (&md5, buffer.buffer, bufsize);
			  if (bufsize < chunk_size)
				break;
			  RollsumInit (&runsum);
			  RollsumUpdate (&runsum, buffer.buffer, bufsize);
			  continue;
            }
        }

	  int c = getc_unlocked (datafile);
	  if (c == -1)
		{
		  /* we're done here. */
		  break;
		}
	  uint8_t cc = (uint8_t) c;
	  RollsumRotate (&runsum, cbuf_get(&buffer, 0), (uint8_t) c);
	  cbuf_addin (&buffer, cc);
    }

  sync_log (SYNC_FILE, "matched %d chunks", matches);

  {
	off_t cur = ftell (datafile);
	if (last_match < cur)
	  {
		sync_log (SYNC_FILE, "handling %lld trailing bytes",
				  (long long) (cur - last_match));
		off_t l;
		file_chunk_t chunk;
		fseek (datafile, last_match, SEEK_SET);
		for (l = last_match; l < cur; )
		  {
			int n = MIN(cur - l, chunk_size);
			fread (buffer.buffer, 1, n, datafile);
			if (n <= MAX_DIRECT_CHUNK_SIZE)
			  {
				sync_log (SYNC_FILE, "DIRECT chunk of %d bytes", n);
				/* Store some direct chunks. */
				chunk.type = DIRECT_CHUNK;
				chunk.chunk.data.length = n;
				memcpy (chunk.chunk.data.data, buffer.buffer,
						chunk.chunk.data.length);
				cb->emit_chunk (cb->state, &chunk);
/* 				fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
			  }
			else
			  {
				sync_log (SYNC_FILE, "REFERENCE chunk of %d bytes", n);
				/* Generate a chunk reference, store the chunk. */
				arrow_compute_key (&current, buffer.buffer, n);
				chunk.type = REFERENCE;
				chunk.chunk.ref.length = n;
				memcpy (&(chunk.chunk.ref.ref), &current, sizeof (arrow_id_t));
				cb->emit_chunk (cb->state, &chunk);
/* 				fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
				if (cb->store_contains (cb->state, &current))
				  cb->add_ref (cb->state, &current);
				else
				  cb->put_block (cb->state, &current, buffer.buffer, n);
			  }
			l += n;
		  }
	  }
  }

  MD5_Final (newfile->file->hash, &md5);

  memset (&chunk, 0, sizeof (file_chunk_t));
  chunk.type = END_OF_CHUNKS;
  cb->emit_chunk (cb->state, &chunk);
/*   fwrite (&chunk, sizeof (file_chunk_t), 1, out); */
/*   fflush (out); */
/*   fclose (out); */
  free (table);
  cbuf_free (&buffer);

  return 0;
}

int
sync_store_add_ref (void *baton, const arrow_id_t *id)
{
  store_state_t *state = ((sync_store_state_t *) baton)->store;
  return store_addref (state, id);
}

int
sync_store_put_block (void *baton, const arrow_id_t *id, const void *buf, size_t len)
{
  store_state_t *state = ((sync_store_state_t *) baton)->store;
  return store_put (state, id, buf, len);
}

int
sync_store_contains (void *baton, const arrow_id_t *id)
{
  store_state_t *state = ((sync_store_state_t *) baton)->store;
  return store_contains (state, id);
}

int
sync_store_emit_chunk (void *baton, const file_chunk_t *chunk)
{
  FILE *out = ((sync_store_state_t *) baton)->chunks_out;
  if (fwrite (chunk, sizeof (file_chunk_t), 1, out) != 1)
	return -1;
  return 0;
}

