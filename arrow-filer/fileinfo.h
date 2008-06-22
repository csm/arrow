/* fileinfo.h -- 
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


#ifndef __FILEINFO_H__
#define __FILEINFO_H__

#include <stdio.h>
#include <time.h>
#include <arrow.h>
#include <uuid.h>

#define FILE_ROOT_DIR "files"
#define MAX_FILE_NAME_LENGTH 256

typedef enum file_type_e
{
  File,
  Directory,
  Link
} file_type_t;

typedef enum file_chunk_type_e
{
  END_OF_CHUNKS = 0,
  REFERENCE,
  DIRECT_CHUNK
} file_chunk_type_t;

typedef struct filer_state_s
{
  char *rootdir;
} filer_state_t;

typedef struct file_chunk_ref_s
{
  uint32_t length;        /**< Chunk length. */
  arrow_id_t ref;         /**< Chunk reference. */
} file_chunk_ref_t;

/* file_chunk_data_t is 24 bytes long, to match ref_t. */

#define MAX_DIRECT_CHUNK_SIZE 23

typedef struct file_chunk_data_s
{
  uint8_t length;     /**< Number of bytes; 0..23. */
  uint8_t data[MAX_DIRECT_CHUNK_SIZE];   /**< Chunk bytes. */
} file_chunk_data_t;

typedef struct file_chunk_s
{
  file_chunk_type_t type;
  union {
	file_chunk_ref_t ref;   /**< If type == REFERENCE */
	file_chunk_data_t data; /**< If type == DIRECT_CHUNK */
  } chunk;
} file_chunk_t;

typedef struct file_info_s
{
  char name[MAX_FILE_NAME_LENGTH];  /**< File's name. */
  uint8_t hash[MD5_DIGEST_LENGTH]; /**< File's MD5. */
  uuid_t previous; /**< ID of previous version. */
  uint64_t size; /**< File size, in bytes. */
  mode_t mode;   /**< File mode. */
  uint32_t chunk_size;  /**< How large chunks are. */
  struct timespec mtime; /**< Data modification time. */
  struct timespec ctime; /**< Status change time. */
  file_chunk_t chunks[0];
} file_info_t;

typedef struct file_direntry_s
{
  file_type_t type;
  uuid_t uuid;
  char name[MAX_FILE_NAME_LENGTH];
} file_direntry_t;

typedef struct file_directory_s
{
  uuid_t previous; /**< ID of previous version. */
  uint32_t count; /**< Number of entries. */
  file_direntry_t entries[0]; /**< Actually `count' entries long. */
} file_directory_t;

typedef struct file_s
{
  uuid_t uuid;
  mapped_file_t data;
  file_info_t *file;
} file_t;

typedef struct dir_s
{
  uuid_t uuid;
  mapped_file_t data;
  file_directory_t *dir;
} dir_t;

typedef struct link_s
{
  uuid_t uuid;
  uint16_t length;
  char path[0];
} link_t;

int filer_init (filer_state_t *state, const char *rootdir);
void filer_destroy (filer_state_t *state);

/**
 * Precondition on \c dir: the \c uuid field is filled in with the
 * target dir's UUID. The null UUID (all zeros) is the "root," and can
 * be used to begin a search through the files.
 */
int dir_open (filer_state_t *state, dir_t *dir);
int dir_close (filer_state_t *state, dir_t *dir);

/**
 * Remap the given directory so it maps the entire dir.
 */
int dir_remap (filer_state_t *state, dir_t *dir);
int file_open (filer_state_t *state, file_t *file, int create);
int file_close (filer_state_t *state, file_t *file);
int file_remap (filer_state_t *state, file_t *file);
int file_delete (filer_state_t *state, file_t *file);

int file_initialize (file_t *file, const char *filename, FILE *f);

void file_print_path (FILE *out, filer_state_t *state, file_t *file);

/*

We need to:

  - open, close directories
  - open, close files
  - create and write files

 */

#endif /* __FILEINFO_H__ */
