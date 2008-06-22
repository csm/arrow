/* fileinfo.c -- 
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


#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <fail.h>
#include <base64.h>
#include "fileinfo.h"
#include "helpers.h"

int
filer_init (filer_state_t *state, const char *rootdir)
{
  size_t len = strlen(rootdir) + strlen(FILE_ROOT_DIR) + 3;
  struct stat st;

  state->rootdir = (char *) malloc (len);
  if (state->rootdir == NULL)
	return -1;

  snprintf (state->rootdir, len, "%s/%s", rootdir, FILE_ROOT_DIR);

  if (stat (state->rootdir, &st) != 0)
	{
	  if (errno != ENOENT)
		{
		  free (state->rootdir);
		  return -1;
		}
	  if (file_mkdirs (state->rootdir, 0700) != 0)
		{
		  free (state->rootdir);
		  return -1;
		}
	}
  else if ((st.st_mode & S_IFDIR) == 0)
	{
	  free (state->rootdir);
	  errno = ENOTDIR;
	  return -1;
	}

  return 0;
}

void
filer_destroy (filer_state_t *state)
{
  free (state->rootdir);
}

#define MAX_B64_UUID_LEN 23

int
dir_open (filer_state_t *state, dir_t *dir)
{
  char *path;
  size_t pathlen = strlen(state->rootdir) + MAX_B64_UUID_LEN + 6;
  char upbuf[12], lobuf[12];
  char *p;
  uint64_t upper, lower;
  struct stat st;
  size_t pagesize = getpagesize();

  path = (char *) malloc (pathlen);
  if (path == NULL)
    return -1;

  upper = arrow_bytes_to_long (dir->uuid);
  lower = arrow_bytes_to_long (dir->uuid + 8);
  b64_encode (upper, upbuf);
  b64_encode (lower, lobuf);

  snprintf (path, pathlen, "%s/%02x/%s.%s", state->rootdir, dir->uuid[0], upbuf, lobuf);

  p = strrchr (path, '/');
  *p = '\0';
  file_mkdirs (path, 0700);
  *p = '/';

  dir->data.fd = open (path, O_RDWR | O_CREAT);
  if (dir->data.fd < 0)
    {
      free (path);
      return -1;
    }

  if (fstat (dir->data.fd, &st) != 0)
    {
      arrow_push_errno();
      free (path);
      close (dir->data.fd);
      arrow_pop_errno();
      return -1;
    }

  if ((st.st_mode & S_IFREG) != 0)
    {
      free (path);
      close (dir->data.fd);
      errno = EINVAL;
      return -1;
    }

  if (st.st_size < (off_t) sizeof (file_directory_t))
    {
      if (ftruncate (dir->data.fd, (off_t) sizeof (file_directory_t)) != 0)
        {
          arrow_push_errno();
          free (path);
          close (dir->data.fd);
          arrow_pop_errno();
          return -1;
        }

      fstat (dir->data.fd, &st);
    }

  dir->data.length = align_up ((size_t) st.st_size, pagesize);
  dir->data.data = mmap (NULL, dir->data.length, PROT_READ | PROT_WRITE,
                         MAP_SHARED, dir->data.fd, 0);
  if (dir->data.data == (void *) -1)
    {
      arrow_push_errno();
      free (path);
      close (dir->data.fd);
      arrow_pop_errno();
      return -1;
    }

  dir->dir = (file_directory_t *) dir->data.data;
  return 0;
}

int
dir_close (filer_state_t *state, dir_t *dir)
{
  if (dir)
    {
      if (dir->data.data != NULL)
        munmap (dir->data.data, dir->data.length);
      close (dir->data.fd);
    }
  return 0;
}

int
dir_remap (filer_state_t *state, dir_t *dir)
{
  struct stat st;
  size_t maplen;
  size_t pagesize = getpagesize();

  if (fstat (dir->data.fd, &st) != 0)
    return -1;

  maplen = align_up ((size_t) st.st_size, pagesize);
  if (dir->data.length != maplen)
    {
      munmap (dir->data.data, dir->data.length);
      dir->data.length = maplen;
      dir->data.data = mmap (NULL, (off_t) maplen, PROT_READ | PROT_WRITE,
                             MAP_SHARED, dir->data.fd, 0);
      if (dir->data.data == (void *) -1)
        {
          dir->data.data = NULL;
          return -1;
        }
      dir->dir = (file_directory_t *) dir->data.data;
    }
  return 0;
}

int
file_open (filer_state_t *state, file_t *file, int create)
{
  char *path;
  size_t pathlen = strlen(state->rootdir) + MAX_B64_UUID_LEN + 3;
  char upbuf[12], lobuf[12];
  uint64_t upper, lower;
  struct stat st;
  size_t pagesize = getpagesize();
  int openmode = O_RDWR;
  if (create)
	openmode = O_RDWR | O_CREAT | O_EXCL;

  path = (char *) malloc (pathlen);
  if (path == NULL)
    return -1;

  upper = arrow_bytes_to_long (file->uuid);
  lower = arrow_bytes_to_long (file->uuid + 8);
  b64_encode (upper, upbuf);
  b64_encode (lower, lobuf);

  snprintf (path, pathlen, "%s/%02x/%s.%s", state->rootdir, file->uuid[0], upbuf, lobuf);

  char *p = strrchr (path, '/');
  *p = '\0';
  file_mkdirs (path, 0700);
  *p = '/';

  file->data.fd = open (path, openmode, 0600);
  if (file->data.fd < 0)
    {
	  fprintf (stderr, "open %s: %s\n", path, strerror (errno));
      free (path);
      return -1;
    }

  if (fstat (file->data.fd, &st) != 0)
    {
      arrow_push_errno();
      free (path);
      close (file->data.fd);
      arrow_pop_errno();
      return -1;
    }

  if ((st.st_mode & S_IFREG) == 0)
    {
      free (path);
      close (file->data.fd);
      errno = EINVAL;
      return -1;
    }

  if (st.st_size < (off_t) sizeof (file_info_t))
    {
      if (ftruncate (file->data.fd, (off_t) sizeof (file_info_t)) != 0)
        {
          arrow_push_errno();
          free (path);
          close (file->data.fd);
          arrow_pop_errno();
          return -1;
        }

      fstat (file->data.fd, &st);
    }

  file->data.length = align_up ((size_t) st.st_size, pagesize);
  file->data.data = mmap (NULL, file->data.length, PROT_READ | PROT_WRITE,
                          MAP_SHARED, file->data.fd, 0);
  if (file->data.data == (void *) -1)
    {
      arrow_push_errno();
      free (path);
      close (file->data.fd);
      arrow_pop_errno();
      return -1;
    }

  file->file = (file_info_t *) file->data.data;
  return 0;
}

int
file_close (filer_state_t *state, file_t *file)
{
  if (file)
    {
      munmap (file->data.data, file->data.length);
      close (file->data.fd);
    }
  return 0;
}

int
file_remap (filer_state_t *state, file_t *file)
{
  struct stat st;
  size_t maplen;
  size_t pagesize = getpagesize();

  if (fstat (file->data.fd, &st) != 0)
    return -1;

  maplen = align_up ((size_t) st.st_size, pagesize);
  if (file->data.length != maplen)
    {
      munmap (file->data.data, file->data.length);
      file->data.length = maplen;
      file->data.data = mmap (NULL, (off_t) maplen, PROT_READ | PROT_WRITE,
                              MAP_SHARED, file->data.fd, 0);
      if (file->data.data == (void *) -1)
        {
          file->data.data = NULL;
          return -1;
        }
      file->file = (file_info_t *) file->data.data;
    }
  return 0;
}

int
file_delete (filer_state_t *state, file_t *file)
{
  char *path;
  size_t pathlen = strlen(state->rootdir) + MAX_B64_UUID_LEN + 3;
  char upbuf[12], lobuf[12];
  uint64_t upper, lower;
  int ret;

  path = (char *) malloc (pathlen);
  if (path == NULL)
    return -1;

  upper = arrow_bytes_to_long (file->uuid);
  lower = arrow_bytes_to_long (file->uuid + 8);
  b64_encode (upper, upbuf);
  b64_encode (lower, lobuf);

  snprintf (path, pathlen, "%s/%02x/%s.%s", state->rootdir, file->uuid[0], upbuf, lobuf);

  ret = unlink (path);
  if (ret != 0)
	fprintf (stderr, "unlink: %s: %s\n", path, strerror (errno));
  free (path);
  return ret;
}

int
file_initialize (file_t *file, const char *filename, FILE *f)
{
  char *fname = strrchr (filename, '/');
  struct stat st;

  if (fname == NULL)
	fname = filename;
  else
	fname = fname + 1;

  strncpy (file->file->name, fname, MAX_FILE_NAME_LENGTH);
  if (fstat (fileno (f), &st) != 0)
	return -1;

  file->file->size = st.st_size;
  file->file->mode = st.st_mode;
  file->file->mtime.tv_sec = st.st_mtime;
  file->file->ctime.tv_sec = st.st_ctime;
  uint32_t bufsize = (uint32_t) sqrtl ((long double) st.st_size);
  if (bufsize < MIN_CHUNK_SIZE)
	bufsize = MIN_CHUNK_SIZE;
  if (bufsize > MAX_CHUNK_SIZE)
	bufsize = MAX_CHUNK_SIZE;
  file->file->chunk_size = bufsize;
  /* memcpy (&file->file->mtime, &st.st_mtimespec, sizeof (struct timespec)); */
  /* memcpy (&file->file->ctime, &st.st_ctimespec, sizeof (struct timespec)); */
  /* memcpy (&file->file->btime, &st.st_birthtimespec, sizeof (struct timespec)); */
  return 0;
}


void
file_print_path (FILE *out, filer_state_t *state, file_t *file)
{
  char upbuf[12], lobuf[12];
  uint64_t upper, lower;

  upper = arrow_bytes_to_long (file->uuid);
  lower = arrow_bytes_to_long (file->uuid + 8);
  b64_encode (upper, upbuf);
  b64_encode (lower, lobuf);

  fprintf (out, "%s/%02x/%s.%s", state->rootdir, file->uuid[0], upbuf, lobuf);
}
