/* client.c -- 
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


#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fileinfo.h>
#include "rpc.h"
#include "client.h"

int RPC_CLIENT_DEBUG = 0;

#define RPC_CLIENT_TRACE 1

#define rpc_client_log(lvl,fmt,args...) if ((lvl & RPC_CLIENT_DEBUG) != 0) fprintf (stderr, "%s (%s:%d): " fmt "\n", __FUNCTION__, __FILE__, __LINE__, ##args)

int
rpc_client_read_link (rpc_t *client, const char *path, uuid_t uuid)
{
  uint16_t response = 0;
  rpc_client_log (RPC_CLIENT_TRACE, "%s %llx-%llx", path,
				  arrow_bytes_to_long ((uint8_t *) uuid),
				  arrow_bytes_to_long ((uint8_t *) uuid + 8));
  if (write_short (client, (uint16_t) READ_LINK_FILE) != 1)
	return -1;
  if (write_string (client, path) != 1)
	return -1;
  fflush (client->out);

  if (read_short (client, &response) != 1)
	return -1;
  if (response != 0)
	return (int) response;
  if (read_value (client, sizeof (uuid_t), 1, uuid) != 1)
	return -1;
  rpc_client_log (RPC_CLIENT_TRACE, "%d", response);
  return 0;
}

int
rpc_client_read_file_hash (rpc_t *client, file_t *file)
{
  uint16_t response = 0;
  rpc_client_log (RPC_CLIENT_TRACE, "%p %p", client, file);

  if (write_short (client, (uint16_t) READ_FILE_HASH) != 1)
	return -1;
  if (write_value (client, sizeof (uuid_t), 1, file->uuid) != 1)
	return -1;
  fflush (client->out);

  if (read_short (client, &response) != 1)
	return -1;
  if (response != 0)
	return response;
  if (read_value (client, MD5_DIGEST_LENGTH, 1, file->file->hash) != 1)
	return -1;
  return 0;
}

int
rpc_client_fetch_file (rpc_t *client, file_t *file)
{
  int dupfd;
  FILE *fout;
  file_chunk_type_t type;
  int ret = 0;

  rpc_client_log (RPC_CLIENT_TRACE, "%p %p", client, file);

  dupfd = dup (file->data.fd);
  if (dupfd < 0)
	return -1;
  fout = fdopen (dupfd, "w");
  if (fout == NULL)
	{
	  close (dupfd);
	  return -1;
	}

  if (write_short (client, (uint16_t) FETCH_VERSION_FILE) != 1)
	{
	  fclose (fout);
	  return -1;
	}
  if (write_value (client, sizeof (uuid_t), 1, file->uuid) != 1)
	{
	  fclose (fout);
	  return -1;
	}
  fflush (client->out);

  if (read_value (client, MD5_DIGEST_LENGTH, 1, file->file->hash) != 1)
	{
	  fclose (fout);
	  return -1;
	}
  if (read_int (client, &(file->file->chunk_size)) != 1)
	{
	  fclose (fout);
	  return -1;
	}

  rpc_client_log (RPC_CLIENT_TRACE, "%llx-%llx MD5 %llx%llx chunk size %u",
				  arrow_bytes_to_long (file->uuid),
				  arrow_bytes_to_long (file->uuid + 8),
				  arrow_bytes_to_long (file->file->hash),
				  arrow_bytes_to_long (file->file->hash + 8),
				  file->file->chunk_size);

  fseek (fout, sizeof (file_info_t), SEEK_SET);

  do
	{
	  uint16_t code;
	  uint32_t ival;
	  file_chunk_t chunk;

	  if (read_short (client, &code) != 1)
		{
		  ret = -1;
		  break;
		}
	  type = (file_chunk_type_t) code;

	  switch (type)
		{
		case END_OF_CHUNKS:
		  rpc_client_log (RPC_CLIENT_TRACE, "END_OF_CHUNKS");
		  memset (&chunk, 0, sizeof (file_chunk_t));
		  break;

		case REFERENCE:
		  rpc_client_log (RPC_CLIENT_TRACE, "REFERENCE");
		  if (read_int (client, &ival) != 1)
			{
			  ret = -1;
			  break;
			}
		  chunk.chunk.ref.length = ival;
		  if (read_int (client, &ival) != 1)
			{
			  ret = -1;
			  break;
			}
		  chunk.chunk.ref.ref.weak = ival; /* bleah */
		  if (read_value (client, sizeof (arrow_key_t), 1, chunk.chunk.ref.ref.strong) != 1)
			{
			  ret = -1;
			  break;
			}
		  break;

		case DIRECT_CHUNK:
		  rpc_client_log (RPC_CLIENT_TRACE, "DIRECT_CHUNK");
		  if (read_value (client, 1, 1, &chunk.chunk.data.length) != 1)
			{
			  ret = -1;
			  break;
			}
		  if (read_value (client, 1, chunk.chunk.data.length, chunk.chunk.data.data)
			  != chunk.chunk.data.length)
			{
			  ret = -1;
			  break;
			}
		}

	  if (ret != 0)
		break;

	  if (fwrite (&chunk, sizeof (file_chunk_t), 1, fout) != 1)
		{
		  ret = -1;
		  break;
		}
	}
  while (type != END_OF_CHUNKS);

  fclose (fout);
  return ret;
}

int
rpc_client_create_file (rpc_t *client, file_t *file)
{
  uint16_t response;
  rpc_client_log (RPC_CLIENT_TRACE, "%p %p", client, file);
  if (write_short (client, (uint16_t) CREATE_VERSION_FILE) != 1)
	return -1;
  if (write_string (client, file->file->name) != 1)
	return -1;
  if (write_value (client, MD5_DIGEST_LENGTH, 1, file->file->hash) != 1)
	return -1;
  if (write_value (client, sizeof (uuid_t), 1, file->file->previous) != 1)
	return -1;
  if (write_long (client, file->file->size) != 1)
	return -1;
  if (write_int (client, (uint32_t) file->file->mode) != 1)
	return -1;
  if (write_int (client, file->file->chunk_size) != 1)
	return -1;
  if (write_int (client, (uint32_t) file->file->mtime.tv_sec) != 1)
	return -1;
  if (write_int (client, (uint32_t) file->file->mtime.tv_nsec) != 1)
	return -1;
  if (write_int (client, (uint32_t) file->file->ctime.tv_sec) != 1)
	return -1;
  if (write_int (client, (uint32_t) file->file->ctime.tv_nsec) != 1)
	return -1;
  fflush (client->out);

  if (read_short (client, &response) != 1)
	return -1;
  rpc_client_log (RPC_CLIENT_TRACE, "%d", response);
  if (response != 0)
	return -1;
  if (read_value (client, sizeof (uuid_t), 1, file->uuid) != 1)
	return -1;

  rpc_client_log (RPC_CLIENT_TRACE, "%llx-%llx",
				  arrow_bytes_to_long ((uint8_t *) file->uuid),
				  arrow_bytes_to_long ((uint8_t *) file->uuid + 8));

  return 0;
}

int
rpc_client_make_link (rpc_t *client, const char *path, const uuid_t uuid)
{
  uint16_t response;
  rpc_client_log (RPC_CLIENT_TRACE, "%s %llx-%llx", path,
				  arrow_bytes_to_long ((uint8_t *) uuid),
				  arrow_bytes_to_long ((uint8_t *) uuid + 8));
  if (write_short (client, (uint16_t) MAKE_FILE_LINK) != 1)
	return -1;
  if (write_string (client, path) != 1)
	return -1;
  if (write_value (client, sizeof (uuid_t), 1, uuid) != 1)
	return -1;
  fflush (client->out);

  if (read_short (client, &response) != 1)
	return -1;
  rpc_client_log (RPC_CLIENT_TRACE, "%d", response);
  return (int) response;
}

int
rpc_client_add_ref (void *baton, const arrow_id_t *id)
{
  uint16_t response;
  rpc_t *client = (rpc_t *) baton;
  rpc_client_log (RPC_CLIENT_TRACE, "%llx %016llx%016llx", id->weak,
				  arrow_bytes_to_long ((uint8_t *) id->strong),
				  arrow_bytes_to_long ((uint8_t *) id->strong + 8));
  if (write_short (client, (uint16_t) STORE_ADD_REF) != 1)
	return -1;
  if (write_int (client, id->weak) != 1)
	return -1;
  if (write_value (client, sizeof (arrow_key_t), 1, id->strong) != 1)
	return -1;
  fflush (client->out);

  if (read_short (client, &response) != 1)
	return -1;
  rpc_client_log (RPC_CLIENT_TRACE, "%d", response);
  return (int) response;
}

int
rpc_client_put_chunk (void *baton, const arrow_id_t *id, const void *buf, size_t len)
{
  uint16_t response;
  rpc_t *client = (rpc_t *) baton;
  rpc_client_log (RPC_CLIENT_TRACE, "%p %x %llx-%llx %p %d", baton, id->weak,
				  arrow_bytes_to_long (id->strong),
				  arrow_bytes_to_long (id->strong + 8), buf, len);

  if (write_short (client, (uint16_t) STORE_PUT_CHUNK) != 1)
	return -1;
  if (write_int (client, id->weak) != 1)
	return -1;
  if (write_value (client, sizeof (arrow_key_t), 1, id->strong) != 1)
	return -1;
  if (write_int (client, (uint32_t) len) != 1)
	return -1;
  if (write_value (client, 1, len, buf) != len)
	return -1;
  fflush (client->out);

  if (read_short (client, &response) != 1)
	return -1;
  rpc_client_log (RPC_CLIENT_TRACE, "%d", response);
  return (int) response;
}

int
rpc_client_contains (void *baton, const arrow_id_t *id)
{
  uint16_t response;
  rpc_t *client = (rpc_t *) baton;
  if (write_short (client, (uint16_t) STORE_BLOCK_EXISTS) != 1)
	return -1;
  if (write_int (client, id->weak) != 1)
	return -1;
  if (write_value (client, sizeof (arrow_key_t), 1, id->strong) != 1)
	return -1;
  fflush (client->out);

  if (read_short (client, &response) != 1)
	return -1;
  return (int) response;
}

int
rpc_client_emit_chunk (void *baton, const file_chunk_t *chunk)
{
  uint16_t response;
  rpc_t *client = (rpc_t *) baton;
  if (write_short (client, (uint16_t) FILE_EMIT_CHUNK) != 1)
	return -1;

  switch (chunk->type)
	{
	case END_OF_CHUNKS:
	  if (write_short (client, (uint16_t) END_OF_CHUNKS) != 1)
		return -1;
	  break;

	case REFERENCE:
	  if (write_short (client, (uint16_t) REFERENCE) != 1)
		return -1;
	  if (write_int (client, chunk->chunk.ref.length) != 1)
		return -1;
	  if (write_int (client, chunk->chunk.ref.ref.weak) != 1)
		return -1;
	  if (write_value (client, MD5_DIGEST_LENGTH, 1, chunk->chunk.ref.ref.strong) != 1)
		return -1;
	  break;

	case DIRECT_CHUNK:
	  if (write_short (client, (uint16_t) DIRECT_CHUNK) != 1)
		return -1;
	  if (write_value (client, 1, 1, &(chunk->chunk.data.length)) != 1)
		return -1;
	  if (write_value (client, 1, chunk->chunk.data.length, chunk->chunk.data.data)
		  != chunk->chunk.data.length)
		return -1;
	  break;
	}
  fflush (client->out);
  
  if (read_short (client, &response) != 1)
	return -1;
  return (int) response;
}

int
rpc_client_close_file (rpc_t *client, file_t *file, int abort_file)
{
  uint16_t response;
  if (write_short (client, (uint16_t) CLOSE_VERSION_FILE) != 1)
	return -1;
  if (write_value (client, sizeof (uuid_t), 1, file->uuid) != 1)
	return -1;
  if (write_value (client, MD5_DIGEST_LENGTH, 1, file->file->hash) != 1)
	return -1;
  if (write_short (client, (uint16_t) abort_file) != 1)
	return -1;
  fflush (client->out);

  if (read_short (client, &response) != 1)
	return -1;
  return (int) response;
}

int
rpc_client_goodbye (rpc_t *client)
{
  uint16_t response;
  if (write_short (client, (uint16_t) GOODBYE) != 1)
	return -1;
  fflush (client->out);

  if (read_short (client, &response) != 1)
	return -1;
  if (response != GOODBYE)
	return 1;
  return 0;
}
