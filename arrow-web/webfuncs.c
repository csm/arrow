/* webfuncs.c -- 
   Copyright (C) 2008  Casey Marshall

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.  */


#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include "webfuncs.h"

#include <bstrlib.h>

struct web_ctx_s
{
  CURL *curl_ctx;
  const char *baseurl;
  const char *userpass;
};

typedef struct iobuf_s
{
  void *data;
  size_t length;
  size_t position;
} iobuf_t;

int
web_init (web_ctx_t **ctx, const char *baseurl, const char *username,
          const char *password)
{
  size_t uplen;
  *ctx = (web_ctx_t *) malloc (sizeof (struct web_ctx_s));
  if (*ctx == NULL)
    return -1;
  (*ctx)->curl_ctx = curl_easy_init();
  if ((*ctx)->curl_ctx == NULL)
    {
      free (*ctx);
      return -1;
    }
  (*ctx)->baseurl = strdup(baseurl);
  (*ctx)->userpass = NULL;
  if (username != NULL && password != NULL)
    {
      uplen = strlen(username) + strlen(password) + 2;
      (*ctx)->userpass = malloc (uplen);
      if ((*ctx)->userpass == NULL)
        {
          curl_easy_cleanup((*ctx)->curl_ctx);
          free (*ctx);
          return -1;
        }
      snprintf ((*ctx)->userpass, uplen, "%s:%s", username, password);
    }
  return 0;
}

void
web_deinit (web_ctx_t *ctx)
{
  if (ctx->curl_ctx)
    curl_easy_cleanup(ctx->curl_ctx);
  if (ctx->baseurl)
    free (ctx->baseurl);
  if (ctx->userpass)
    free (ctx->userpass);
  if (ctx)
    free (ctx);
}

static int
read_cb (void *buf, size_t size, size_t nelem, void *stream)
{
  iobuf_t *iob = (iobuf_t *) stream;
  size_t count = size * nelem;
  if (count > iob->length - iob->position)
    count = align_down(iob->length - iob->position, size);
  memcpy(buf, iob->data, count);
  iob->position += count;
  return count / size;
}

int
web_add_ref (void *state, const arrow_id_t *id)
{
  web_ctx_t *ctx = (web_ctx_t *) state;
  CURL *curl = ctx->curl_ctx;
  const char *data = "addref=1\n";
  iobuf_t iob;
  char *urlbuf;
  int urllen;
  CURLcode curlret;

  iob.data = (void *) data;
  iob.length = strlen (data);
  iob.position = 0;
  
  urllen = strlen(ctx->baseurl) + strlen(WEB_STORE_PATH) + 33;
  urlbuf = (char *) malloc (urllen);
  if (urlbuf == NULL)
    return -1;
  snprintf (urlbuf, urllen, "%s%s%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
                            "%02x%02x%02x%02x%02x%02x"
            ctx->baseurl, WEB_STORE_PATH,
            id->strong[ 0], id->strong[ 1], id->strong[ 2], id->strong[ 3],
            id->strong[ 4], id->strong[ 5], id->strong[ 6], id->strong[ 7],
            id->strong[ 8], id->strong[ 9], id->strong[10], id->strong[11],
            id->strong[12], id->strong[13], id->strong[14], id->strong[15]);
  
  curl_easy_reset(curl);
  curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_cb);
  curl_easy_setopt(curl, CURLOPT_READDATA, &iob);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, NULL);
  curl_easy_setopt(curl, CURLOPT_POST, 1);
  curl_easy_setopt(curl, CURLOPT_URL, urlbuf);
  curl_easy_setopt(curl, CURLOPT_NETRC, CURL_NETRC_OPTIONAL);
  curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_ANY);
  if (ctx->userpass != NULL)
    curl_easy_setopt(curl, CURLOPT_USERPWD, ctx->userpass);
  
  curlret = curl_easy_perform(curl);
  if (curlret != 0)
    {
      free (urlbuf);
      return -1;
    }
  free (urlbuf);
  return 0;
}

int
web_put_block (void *state, const arrow_id_t *id, const void *buf, size_t len)
{
	return -1;
}

int
web_store_contains (void *state, const arrow_id_t *id)
{
	return -1;
}

int
web_emit_chunk (void *state, const file_chunk_t *chunk)
{
	return -1;
}
