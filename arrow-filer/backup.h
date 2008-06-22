/* backup.h -- 
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


#ifndef __BACKUP_H__
#define __BACKUP_H__

#include "fileinfo.h"
#include <client.h>
#include <store.h>
#include <sync.h>
#include <rpc.h>

typedef struct backup_stats_s
{
  size_t files;
} backup_stats_t;

typedef struct file_backup_s
{
  enum { LOCAL, REMOTE } type;
  char *source_root;
  char *tree_root;
  filer_state_t filer;
  store_state_t *store;
  sync_callbacks_t sync_cb;
  backup_stats_t stats;
  rpc_t *rpcclient;
  char tmpdir[256];
} file_backup_t;

int file_init_local (file_backup_t *state, const char *rootdir, const char *sourcedir);
int file_init_remote (file_backup_t *state, FILE *in, FILE *out);
int file_reset_local_sourcedir (file_backup_t *state, const char *sourcedir);
int file_backup_file (file_backup_t *state, const char *path);
int file_recursive_backup (file_backup_t *state, const char *path);

#endif /* __BACKUP_H__ */
