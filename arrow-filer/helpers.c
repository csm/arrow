/* helpers.c -- 
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


#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <openssl/md5.h>

#include <arrow.h>
#include <base64.h>
#include <fail.h>
#include <uuid.h>
#include "helpers.h"

static void
file_pathsplit (const char *path, char **p)
{
  const char *l = strrchr (path, '/');
  if (l == NULL)
	{
	  p[0] = strdup (path);
	  p[1] = NULL;
	}
  else
	{
	  int i = l - path;
	  p[0] = strdup (path);
	  p[0][i] = '\0';
	  p[1] = strdup (l + 1);
	}
}

int
file_exists (const char *path)
{
  struct stat st;
  return (lstat (path, &st) == 0);
}

int
file_isfile (const char *path)
{
  struct stat st;
  if (lstat (path, &st) != 0)
	return 0;
/*   fprintf (stderr, "isfile %s %x %d\n", path, st.st_mode, st.st_mode & S_IFREG); */
  return ((st.st_mode & S_IFREG) != 0);
}

int
file_isdir (const char *path)
{
  struct stat st;
  if (lstat (path, &st) != 0)
	return 0;
/*   fprintf (stderr, "isdir %s %x %d\n", path, st.st_mode, st.st_mode & S_IFDIR); */
  return ((st.st_mode & S_IFDIR) != 0);
}

int
file_islink (const char *path)
{
  struct stat st;
  if (lstat (path, &st) != 0)
	return 0;
  return ((st.st_mode & S_IFLNK) != 0);
}

const char *
file_basename (const char *path)
{
  const char *ret = strrchr (path, '/');
  if (ret == NULL)
	return path;
  return ret + 1;
}

void
file_dirname (const char *path, char *dest, size_t len)
{
  const char *p = path;
  const char *slash = NULL;
  for (p = path; *p != '\0'; p++)
	{
	  if (*p == '/' && *(p+1) != '\0')
		slash = p;
	}
  if (slash == NULL)
	strncpy (dest, ".", len);
  else
	{
	  int l = (int) (slash - path);
	  strncpy (dest, path, len);
	  if (l < len)
		dest[l] = '\0';
	}
}

int
file_mkdirs (const char *path, int mode)
{
  char dir[1024];
  char *p;

  if (file_isdir (path))
	return 0;
  if (file_isfile (path))
	{
	  errno = EEXIST;
	  return -1;
	}
  file_dirname (path, dir, sizeof (dir));
  if (strcmp (dir, ".") != 0)
	file_mkdirs (dir, mode);
  if (mkdir (path, mode) != 0)
	return -1;
  return 0;
}

int
file_rmrf (const char *path)
{
  if (file_isdir (path))
	{
	  dirlist_t list;
	  if (file_listdir (path, &list) != 0)
		return -1;
	  int i;
	  for (i = 0; i < list.length; i++)
		{
		  if (strcmp (".", list.paths[i]) == 0
			  || strcmp ("..", list.paths[i]) == 0)
			continue;
		  char *next = path_join (path, list.paths[i]);
		  int ret = file_rmrf (next);
		  free (next);
		  if (ret != 0)
			{
			  file_free_dirlist (&list);
			  return ret;
			}
		}
	  file_free_dirlist (&list);
	  if (rmdir (path) != 0)
		return -1;
	}
  else if (file_isfile (path) || file_islink (path))
	{
	  return unlink (path);
	}
  return 0;
}

int
file_listdir (const char *path, dirlist_t *list)
{
  DIR *dir = opendir (path);
  struct dirent *ent;
  int i = 0;

  if (dir == NULL)
	return -1;

  list->mlen = 32;
  list->paths = (char **) malloc (list->mlen * sizeof (char *));
  if (list->paths == NULL)
	{
	  closedir (dir);
	  return -1;
	}
  while ((ent = readdir (dir)) != NULL)
	{
	  if (i >= list->mlen)
		{
		  list->mlen = list->mlen << 1;
		  list->paths = (char **) realloc (list->paths, list->mlen * sizeof (char *));
		  if (list->paths == NULL)
			fail ("argh");
		}
	  list->paths[i] = strdup (ent->d_name);
	  i++;
	}
  list->length = i;
  closedir (dir);
  return 0;
}

char *
path_join (const char *dir, const char *file)
{
  char *ret = (char *) malloc (strlen (dir) + strlen (file) + 2);
  strcpy (ret, dir);
  if (ret[strlen(ret) - 1] != '/')
	strcat (ret, "/");
  strcat (ret, file);
  return ret;
}

void
file_free_dirlist (dirlist_t *dir)
{
  int i;
  for (i = 0; i < dir->length; i++)
	free (dir->paths[i]);
  free (dir->paths);
}

int
make_link_file (const char *linkpath, uuid_t uuid)
{
  char *p;
  char uuidbuf[30];
  struct stat st;
  char upbuf[12], lobuf[12];
  uint64_t upper, lower;

  upper = arrow_bytes_to_long (uuid);
  lower = arrow_bytes_to_long (uuid + 8);
  b64_encode (upper, upbuf);
  b64_encode (lower, lobuf);

  snprintf (uuidbuf, sizeof (uuidbuf), "%02x/%s.%s", uuid[0], upbuf, lobuf);

  if (lstat (linkpath, &st) == 0)
	{
	  if ((st.st_mode & S_IFLNK) == 0)
		{
		  errno = EEXIST;
		  return -1;
		}
	  if (unlink (linkpath) != 0)
		return -1;
	}

  p = strrchr (linkpath, '/');
  *p = '\0';
/*   fprintf (stderr, "%s: file_mkdirs %s\n", __FUNCTION__, linkpath); */
  int ret;
  if ((ret = file_mkdirs (linkpath, 0700)) != 0)
	{
/* 	  fprintf (stderr, "file_mkdirs: %s (returned %d)\n", strerror (errno), ret); */
	  return -1;
	}
  *p = '/';
/*   fprintf (stderr, "%s: symlink %s -> %s\n", __FUNCTION__, uuidbuf, linkpath); */
  if (symlink (uuidbuf, linkpath) != 0)
	return -1;
  return 0;
}

int
read_link_file (const char *linkpath, uuid_t uuid)
{
  char uuidbuf[30];
  ssize_t len;
  char *p1, *p2;
  uint64_t hi, lo;

  len = readlink (linkpath, uuidbuf, sizeof(uuidbuf));
  if (len < 0)
	return -1;

  uuidbuf[len] = '\0';
  p1 = strchr (uuidbuf, '/');
  if (p1 == NULL)
	return -1;
  p1++;
  p2 = strchr (p1, '.');
  if (p2 == NULL)
	return -1;

  *p2 = '\0';
  p2++;

  /* I should just fucking write a function that parses a base-64
     UUID. FIXME. 'n shit. */

  if (b64_decode (p1, &hi) != 0)
	{
/* 	  fprintf (stderr, "%s\n", uuidbuf); */
	  errno = EINVAL; /* FIXME we need a better error reporting mechanism. */
	  return -1;
	}
  if (b64_decode (p2, &lo) != 0)
	{
/* 	  fprintf (stderr, "%s\n", p); */
	  errno = EINVAL;
	  return -1;
	}

  uuid_from_longs (uuid, hi, lo);
  return 0;
}

int
file_compare_hash (FILE *infile, const uint8_t *hash)
{
  MD5_CTX md5;
  uint8_t buffer[1024];
  uint8_t digest[MD5_DIGEST_LENGTH];
  int len;

  MD5_Init (&md5);

  while ((len = fread (buffer, 1, sizeof (buffer), infile)) > 0)
	  MD5_Update (&md5, buffer, len);
  rewind (infile);
  MD5_Final (digest, &md5);
  return memcmp (digest, hash, MD5_DIGEST_LENGTH);
}
