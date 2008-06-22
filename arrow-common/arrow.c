/* arrow.c -- 
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


#include <assert.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>

#include <openssl/md5.h>

#include "arrow.h"
#include "rollsum.h"

void
arrow_compute_key (arrow_id_t *id, const void *data, size_t length)
{
  Rollsum rs;
  RollsumInit (&rs);
  RollsumUpdate (&rs, data, (unsigned int) length);
  id->weak = RollsumDigest (&rs);
  MD5 (data, length, id->strong);
}

uint64_t
arrow_bytes_to_long (const uint8_t *buf)
{
  return ((((uint64_t) buf[0] & 0xFFULL) << 56)
          | (((uint64_t) buf[1] & 0xFFULL) << 48)
          | (((uint64_t) buf[2] & 0xFFULL) << 40)
          | (((uint64_t) buf[3] & 0xFFULL) << 32)
          | (((uint64_t) buf[4] & 0xFFULL) << 24)
          | (((uint64_t) buf[5] & 0xFFULL) << 16)
          | (((uint64_t) buf[6] & 0xFFULL) <<  8)
          | ((uint64_t)  buf[7] & 0xFFULL));
}

void
arrow_long_to_bytes (uint8_t *buf, uint64_t value)
{
  buf[0] = (uint8_t) ((value >> 56) & 0xFF);
  buf[1] = (uint8_t) ((value >> 48) & 0xFF);
  buf[2] = (uint8_t) ((value >> 40) & 0xFF);
  buf[3] = (uint8_t) ((value >> 32) & 0xFF);
  buf[4] = (uint8_t) ((value >> 24) & 0xFF);
  buf[5] = (uint8_t) ((value >> 16) & 0xFF);
  buf[6] = (uint8_t) ((value >>  8) & 0xFF);
  buf[7] = (uint8_t)  (value        & 0xFF);
}

void
arrow_left_fill (char *buf, size_t len, char fill)
{
  size_t slen = strlen(buf) + 1;
  size_t filllen = len - slen;
  if (filllen > 0)
    {
      int i;
      memmove (buf + filllen, buf, slen);
      for (i = 0; i < filllen; i++)
        buf[i] = fill;
    }
}

static int __saved_errno = 0;

void
arrow_push_errno (void)
{
  __saved_errno = errno;
}

void
arrow_pop_errno (void)
{
  errno = __saved_errno;
  __saved_errno = 0;
}

int
arrow_popen (const char *cmd, char * const argv[], pid_t *pid, FILE **out, FILE **in)
{
  pid_t newpid;
  int pipein[2];
  int pipeout[2];

  if (pipe (pipein) != 0)
    return -1;
  if (pipe (pipeout) != 0)
    {
      close (pipein[0]);
      close (pipein[1]);
      return -1;
    }

  newpid = fork();
  if (newpid < 0)
    return -1;

  if (newpid == 0)
    {
      close (pipein[1]);
      if (dup2 (pipein[0], STDIN_FILENO) == -1)
        exit (1);
      close (pipeout[0]);
      if (dup2 (pipeout[1], STDOUT_FILENO) == -1)
        exit(1);

      char *buffer = malloc(256);
      setbuffer (stdout, buffer, 256);

      execvp (cmd, argv);
      exit (1); /* reached on error */
    }

  if (pid != NULL)
    *pid = newpid;
  close (pipein[0]);
  *out = fdopen (pipein[1], "w");
  if (*out == NULL)
    {
      int x;
      arrow_pclose (newpid, NULL, NULL, &x);
      return -1;
    }
  close (pipeout[1]);
  *in = fdopen (pipeout[0], "r");
  if (*in == NULL)
    {
      int x;
      arrow_pclose (newpid, *out, NULL, &x);
      return -1;
    }
  errno = 0;
  return 0;
}

pid_t
arrow_pclose (pid_t pid, FILE *out, FILE *in, int *status)
{
  pid_t ret;
/*   kill (pid, SIGTERM); */
  ret = waitpid (pid, status, 0);
  if (out != NULL)
    fclose (out);
  if (in != NULL)
    fclose (in);
  return ret;
}

#undef malloc
void *
arrow_malloc (size_t n)
{
  assert (n >= 0 && n < 1000000);
  return malloc (n);
}

/* Local Variables: */
/* tab-width: 8 */
/* indent-tabs-mode: nil */
/* c-basic-offset: 2 */
/* End: */
