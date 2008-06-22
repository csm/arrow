/* helpers.h -- 
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


#ifndef __HELPERS_H__
#define __HELPERS_H__

#include <stdio.h>

typedef struct dirlist_s
{
  size_t mlen;
  size_t length;
  char **paths;
} dirlist_t;

int file_exists (const char *path);
int file_isfile (const char *path);
int file_isdir (const char *path);
int file_islink (const char *path);
const char *file_basename (const char *path);
void file_dirname (const char *path, char *dest, size_t len);
int file_mkdirs (const char *path, int mode);
int file_rmrf (const char *path);
char *path_join(const char *dir, const char *file);

int make_link_file (const char *linkfile, uuid_t uuid);
int read_link_file (const char *linkfile, uuid_t uuid);

int file_listdir (const char *path, dirlist_t *list);

void file_free_dirlist (dirlist_t *list);

int file_compare_hash (FILE *infile, const uint8_t *hash);

#endif /* __HELPERS_H__ */
