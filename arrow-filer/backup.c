/* backup.c -- 
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <sync.h>
#include <store.h>
#include "fileinfo.h"
#include "backup.h"
#include "helpers.h"
#include <fail.h>
#include <base64.h>
#include <client.h>

#define TREE_ROOT_DIR "tree"

#define BACKUP_TRACE 1

#define backup_log(lvl,fmt,args...) if ((lvl & BACKUP_DEBUG) != 0) fprintf (stderr, "%s (%s:%d): " fmt "\n", __FUNCTION__, __FILE__, __LINE__, ##args)

int BACKUP_DEBUG = 0;

int
file_init_local (file_backup_t *state, const char *rootdir, const char *source_root)
{
  size_t len = strlen (rootdir) + strlen (TREE_ROOT_DIR) + 3;
  if (store_init (rootdir, &(state->store)) != 0)
	{
	  fprintf (stderr, "store_init: %s\n", strerror (errno));
	  return -1;
	}
  if (filer_init (&(state->filer), rootdir) != 0)
	{
	  fprintf (stderr, "filer_init: %s\n", strerror (errno));
	  store_destroy (state->store);
	  return -1;
	}
  state->source_root = NULL;
  if (file_reset_local_sourcedir (state, source_root) != 0)
	{
	  fprintf (stderr, "file_reset_local_sourcedir: %s\n", strerror (errno));
	  store_destroy (state->store);
	  filer_destroy (&state->filer);
	  return -1;
	}
  state->tree_root = (char *) malloc (len);
  if (state->tree_root == NULL)
	{
	  store_destroy (state->store);
	  filer_destroy (&state->filer);
	  free (state->source_root);
	  return -1;
	}
  snprintf (state->tree_root, len, "%s/%s", rootdir, TREE_ROOT_DIR);
  state->sync_cb.add_ref = sync_store_add_ref;
  state->sync_cb.put_block = sync_store_put_block;
  state->sync_cb.store_contains = sync_store_contains;
  state->sync_cb.emit_chunk = sync_store_emit_chunk;
  state->sync_cb.state = malloc (sizeof (sync_store_state_t));
  ((sync_store_state_t *) state->sync_cb.state)->store = state->store;
  state->type = LOCAL;
  state->stats.files = 0;
  return 0;
}

int
file_init_remote (file_backup_t *state, FILE *in, FILE *out)
{
  memset (state, 0, sizeof (file_backup_t));
  snprintf (state->tmpdir, 256, "/tmp/arrow-%d", getuid());
  if (file_mkdirs (state->tmpdir, 0700) != 0)
	return -1;
  if (filer_init (&(state->filer), state->tmpdir) != 0)
	return -1;
  state->rpcclient = (rpc_t *) malloc (sizeof (rpc_t));
  state->rpcclient->in = in;
  state->rpcclient->out = out;
  state->rpcclient->stats = (rpc_stats_t *) malloc (sizeof (rpc_stats_t));
  state->rpcclient->stats->bytes_in = 0;
  state->rpcclient->stats->bytes_out = 0;
  state->sync_cb.add_ref = rpc_client_add_ref;
  state->sync_cb.put_block = rpc_client_put_chunk;
  state->sync_cb.store_contains = rpc_client_contains;
  state->sync_cb.emit_chunk = rpc_client_emit_chunk;
  state->sync_cb.state = state->rpcclient;
  state->type = REMOTE;
  state->stats.files = 0;
  return 0;
}

int
file_reset_local_sourcedir (file_backup_t *state, const char *sourcepath)
{
  struct stat st;
  fprintf (stderr, "reset sourcedir: %s\n", sourcepath);
  if (stat (sourcepath, &st) != 0)
	return -1;
  if (state->source_root != NULL)
	{
	  free (state->source_root);
	  state->source_root = NULL;
	}
  state->source_root = (char *) malloc (strlen (sourcepath) + 3);
  if (state->source_root == NULL)
	return -1;
  strcpy (state->source_root, sourcepath);
  if ((st.st_mode & S_IFDIR) != 0)
	{
	  if (state->source_root[strlen(state->source_root) - 1] != '/')
		strcat (state->source_root, "/");
	}
  else
	{
	  char *p = strrchr (state->source_root, '/');
	  if (p == NULL)
		strcpy (state->source_root, "./");
	  else
		*(p+1) = '\0';
	}
  backup_log (BACKUP_TRACE, "source_root is %s", state->source_root);
  return 0;
}

static int
local_file_backup_file (file_backup_t *state, const char *path)
{
  size_t pathlen = (strlen(state->tree_root) + strlen(path)
					- strlen(state->source_root)) + 2;
  char *linkpath;

  backup_log (BACKUP_TRACE, "%p %s", state, path);

  linkpath = (char *) malloc (pathlen);
  if (linkpath == NULL)
	return -1;
  snprintf (linkpath, pathlen, "%s/%s", state->tree_root,
			path + strlen(state->source_root));

  backup_log (BACKUP_TRACE, "linkpath is %s", linkpath);

  if (!file_exists (linkpath))
	{
	  FILE *infile;
	  FILE *out;
	  file_t newfile;
	  uuid_generate (newfile.uuid);
	  if (file_open (&(state->filer), &newfile, 1) != 0)
		{
		  free (linkpath);
		  return -1;
		}

	  backup_log (BACKUP_TRACE, "new file %016llx-%016llx",
				  arrow_bytes_to_long ((uint8_t *) newfile.uuid),
				  arrow_bytes_to_long ((uint8_t *) newfile.uuid + 8));
	  
	  infile = fopen (path, "r");
	  if (infile == NULL)
		{
		  file_close (&(state->filer), &newfile);
		  file_delete (&(state->filer), &newfile);
		  free (linkpath);
		  return -1;
		}
	  out = fdopen (dup(newfile.data.fd), "w");
	  if (out == NULL)
		{
		  file_close (&(state->filer), &newfile);
		  file_delete (&(state->filer), &newfile);
		  fclose (infile);
		  free (linkpath);
		  return -1;
		}
	  fseek (out, sizeof (file_info_t), SEEK_SET);
	  ((sync_store_state_t *) state->sync_cb.state)->chunks_out = out;
	  if (sync_generate (&newfile, infile, &(state->sync_cb)) != 0)
		{
		  file_close (&(state->filer), &newfile);
		  file_delete (&(state->filer), &newfile);
		  fclose (infile);
		  fclose (out);
		  free (linkpath);
		  return -1;
		}
	  char xxx[24];
	  uuid_tostring(xxx, newfile.uuid);
	  backup_log (BACKUP_TRACE, "%s -> %s", linkpath, xxx);
	  int ret = make_link_file (linkpath, newfile.uuid);
	  file_close (&(state->filer), &newfile);
	  fclose (infile);
	  fclose (out);
	  state->stats.files++;
	  return ret;
	}
  else if (file_islink (path))
	{
	  char uuidbuf[24];
	  file_t newfile;
	  file_t basis;
	  ssize_t len;
	  FILE *infile;
	  FILE *out;
	  int hash_match = 1;
	  int ret = 0;

	  backup_log (BACKUP_TRACE, "syncing new version of existing file");

	  uuid_generate(newfile.uuid);
	  backup_log (BACKUP_TRACE, "new file %016llx-%016llx",
				  arrow_bytes_to_long ((uint8_t *) newfile.uuid),
				  arrow_bytes_to_long ((uint8_t *) newfile.uuid + 8));
	  
	  if (read_link_file (linkpath, basis.uuid) != 0)
		return -1;
	  backup_log (BACKUP_TRACE, "existing file %016llx-%016llx",
				  arrow_bytes_to_long ((uint8_t *) basis.uuid),
				  arrow_bytes_to_long ((uint8_t *) basis.uuid + 8));
	  if (file_open (&state->filer, &basis, 0) != 0)
		return -1;
	  backup_log (BACKUP_TRACE, "opened basis file");
	  infile = fopen (path, "r");
	  if (infile == NULL)
		{
		  backup_log (BACKUP_TRACE, "%s: %s\n", path, strerror (errno));
		  file_close (&state->filer, &basis);
		  return -1;
		}
	  if (file_open (&state->filer, &newfile, 1) != 0)
		{
		  fclose (infile);
		  file_close (&state->filer, &basis);
		  return -1;
		}
	  out = fdopen (dup(newfile.data.fd), "w");
	  if (out == NULL)
		{
		  backup_log (BACKUP_TRACE, "%d: %s\n", newfile.data.fd, strerror (errno));
		  file_close (&(state->filer), &newfile);
		  file_delete (&(state->filer), &newfile);
		  fclose (infile);
		  free (linkpath);
		  return -1;
		}
	  fseek (out, sizeof (file_info_t), SEEK_SET);
	  ((sync_store_state_t *) state->sync_cb.state)->chunks_out = out;
	  if (sync_file (&basis, &newfile, infile, &(state->sync_cb), &hash_match) != 0)
		{
		  file_close (&(state->filer), &newfile);
		  file_delete (&(state->filer), &newfile);
		  file_close (&(state->filer), &basis);
		  fclose (infile);
		  fclose (out);
		  free (linkpath);
		  return -1;
		}
	  backup_log (BACKUP_TRACE, "sync_file done; hash_match: %d", hash_match);
	  file_close (&(state->filer), &newfile);
	  if (hash_match)
		file_delete (&(state->filer), &newfile);
	  file_close (&(state->filer), &basis);

	  if (!hash_match)
		{
		  ret = make_link_file (linkpath, newfile.uuid);		  
		  state->stats.files++;
		}
	  fclose (out);
	  fclose (infile);
	  return ret;
	}
  else
	{
	  errno = EINVAL;
	  return -1;
	}
}

static int
remote_file_backup_file (file_backup_t *state, const char *path)
{
  uuid_t basisid;
  const char *trimpath = path + strlen (state->source_root);
  int exist;

  backup_log (BACKUP_TRACE, "%p %s [trimpath: %s]", state, path, trimpath);

  exist = rpc_client_read_link (state->rpcclient, trimpath, basisid);

  if (exist == 1) /* symlink does not exist */
	{
	  file_info_t fileinfo;
	  file_t newfile;
	  FILE *infile = fopen (path, "r");

	  backup_log (BACKUP_TRACE, "creating new file");
	  if (infile == NULL)
		return -1;
	  newfile.data.fd = -1;
	  newfile.data.data = &fileinfo;
	  newfile.file = &fileinfo;
	  memset(newfile.uuid, 0, sizeof (uuid_t));
	  if (file_initialize (&newfile, path, infile) != 0)
		{
		  fclose (infile);
		  return -1;
		}
	  if (rpc_client_create_file (state->rpcclient, &newfile) != 0)
		{
		  fclose (infile);
		  return -1;
		}
	  if (sync_generate (&newfile, infile, &(state->sync_cb)) != 0)
		{
		  rpc_client_close_file (state->rpcclient, &newfile, 1);
		  fclose (infile);
		  return -1;
		}
	  fclose (infile);
	  rpc_client_close_file (state->rpcclient, &newfile, 0);
	  rpc_client_make_link (state->rpcclient, trimpath, newfile.uuid);
	  state->stats.files++;
	  return 0;
	}
  else if (exist == 0) /* link exists, read into basisid */
	{
	  file_info_t fileinfo;
	  file_t newfile;
	  file_t basis;
	  FILE *infile = fopen (path, "r");

	  backup_log (BACKUP_TRACE, "syncing with file %llx-%llx",
				  arrow_bytes_to_long ((uint8_t *) basisid),
				  arrow_bytes_to_long ((uint8_t *) basisid + 8));

	  if (infile == NULL)
		return -1;
	  newfile.data.fd = -1;
	  newfile.data.data = &fileinfo;
	  newfile.file = &fileinfo; 
	  uuid_copy (basis.uuid, basisid);
	  if (file_open (&state->filer, &basis, 1) != 0)
		{
		  fclose (infile);
		  return -1;
		}
	  if (rpc_client_read_file_hash (state->rpcclient, &basis) != 0)
		{
		  file_close (&state->filer, &basis);
		  file_delete (&state->filer, &basis);
		  fclose (infile);
		  return -1;
		}
	  if (file_compare_hash (infile, basis.file->hash) == 0)
		{
		  backup_log (BACKUP_TRACE, "file MD5 matches; skipping this file");
		  file_close (&state->filer, &basis);
		  file_delete (&state->filer, &basis);
		  fclose (infile);
		  return 0;
		}
	  if (rpc_client_fetch_file (state->rpcclient, &basis) != 0)
		{
		  file_close (&state->filer, &basis);
		  file_delete (&state->filer, &basis);
		  fclose (infile);
		  return -1;
		}
	  uuid_copy (newfile.file->previous, basisid);
	  if (file_initialize (&newfile, path, infile) != 0)
		{
		  file_close (&state->filer, &basis);
		  file_delete (&state->filer, &basis);
		  fclose (infile);
		  return -1;
		}
	  if (rpc_client_create_file (state->rpcclient, &newfile) != 0)
		{
		  file_close (&state->filer, &basis);
		  file_delete (&state->filer, &basis);
		  fclose (infile);
		  return -1;
		}
	  if (sync_file (&basis, &newfile, infile, &state->sync_cb, NULL) != 0)
		{
		  rpc_client_close_file (state->rpcclient, &newfile, 1);
		  file_close (&state->filer, &basis);
		  file_delete (&state->filer, &basis);
		  fclose (infile);
		  return -1;
		}
	  rpc_client_close_file (state->rpcclient, &newfile, 0);
	  rpc_client_make_link (state->rpcclient, trimpath, newfile.uuid);
	  state->stats.files++;

	  /*
	   * FIXME -- I'd much rather not have this temp file BS usless it
	   * was absolutely necessary -- like a version file that was too
	   * big to fit into memory (though, we'd fail before now on that
	   * anyway, right?). I'd need to have a "write-y" way to push
	   * data into memory, though.
	   */
	  file_close (&state->filer, &basis);
	  file_delete (&state->filer, &basis);
	  fclose (infile);
	  return 0;
	}
  else
	{
	  errno = EEXIST;
	  return -1;
	}
}

int
file_backup_file (file_backup_t *state, const char *path)
{
  if (state->type == LOCAL)
	return local_file_backup_file (state, path);
  else if (state->type == REMOTE)
	return remote_file_backup_file (state, path);
  fail ("should not be reachable in a perfect world");
}

int
file_recursive_backup (file_backup_t *state, const char *path)
{
  dirlist_t dirlist;
  int i;

  backup_log (BACKUP_TRACE, "%p %s", state, path);

  if (file_isfile (path))
	{
	  backup_log (BACKUP_TRACE, "%s: is a file, backing it up", path);
	  return file_backup_file (state, path);
	}
  else if (file_isdir (path))
	{
	  backup_log (BACKUP_TRACE, "%s: is a directory", path);
	  if (file_listdir (path, &dirlist) != 0)
		{
		  backup_log (BACKUP_TRACE, "listing dir %s: %s", path, strerror (errno));
		  return -1;
		}
	  backup_log (BACKUP_TRACE, "%s: %d entries", path, dirlist.length);
	  for (i = 0; i < dirlist.length; i++)
		{
		  if (strcmp (dirlist.paths[i], ".") == 0
			  || strcmp (dirlist.paths[i], "..") == 0)
			continue;
		  char *p = path_join (path, dirlist.paths[i]);
		  int ret = file_recursive_backup (state, p);
		  free (p);
		  if (ret != 0)
			{
			  file_free_dirlist (&dirlist);
			  return ret;
			}
		}
	  file_free_dirlist (&dirlist);
	}
  else
	{
	  errno = ENOENT;
	  return -1;
	}
  return 0;
}

