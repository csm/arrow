/* rpc.c -- 
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


#include <arpa/inet.h>
#include <string.h>
#include <arrow.h>
#include "rpc.h"

int RPC_DEBUG = 0;

#define rpc_log(fmt,args...) if (RPC_DEBUG) fprintf (stderr, "%s (%s:%d) [%d]: " fmt "\n", \
													 __FUNCTION__, __FILE__, __LINE__, \
													 getpid(), ##args)

int
write_value (rpc_t *rpc, size_t esize, size_t count, const void *value)
{
  rpc_log ("%p %ld %ld %p", rpc, (long int) esize, (long int) count, value);
  int ret = fwrite (value, esize, count, rpc->out);
  if (rpc->stats != NULL)
	rpc->stats->bytes_out += ret * esize;
  rpc_log ("%d", ret);
  return ret;
}

int
write_short (rpc_t *rpc, uint16_t value)
{
  rpc_log ("%d", value);
  value = htons (value);
  return write_value (rpc, 2, 1, &value);
}

int
write_int (rpc_t *rpc, uint32_t value)
{
  rpc_log ("%u", value);
  value = htonl (value);
  return write_value (rpc, 4, 1, &value);
}

int
write_long (rpc_t *rpc, uint64_t value)
{
  int ret;
  uint8_t buf[8];
  rpc_log ("%llu", value);
  arrow_long_to_bytes (buf, value);
  return write_value (rpc, 8, 1, buf);
}

int
write_string (rpc_t *rpc, const char *str)
{
  uint16_t len = strlen (str);
  if (write_short (rpc, len) != 1)
	return 0;
  rpc_log ("%s", str);
  if (write_value (rpc, 1, len, str) != len)
	return 0;
  return 1;
}

int
read_value (rpc_t *rpc, size_t esize, size_t count, void *buf)
{
  rpc_log ("%p %ld %ld %p", rpc, (long int) esize, (long int) count, buf);
  int ret = fread (buf, esize, count, rpc->in);
  if (rpc->stats != NULL)
	rpc->stats->bytes_in += ret * esize;
  rpc_log ("%d", ret);
  return ret;
}

int
read_short (rpc_t *rpc, uint16_t *value)
{
  int ret = read_value (rpc, 2, 1, value);
  if (ret == 1)
	{
	  *value = ntohs(*value);
	  rpc_log ("%d", *value);
	}
  return ret;
}

int
read_int (rpc_t *rpc, uint32_t *value)
{
  int ret = read_value (rpc, 4, 1, value);
  if (ret == 1)
	{
	  *value = ntohl(*value);
	  rpc_log ("%u", *value);
	}
  return ret;
}

int
read_long (rpc_t *rpc, uint64_t *value)
{
  unsigned char buf[8];
  int ret = read_value (rpc, 8, 1, buf);
  if (ret == 1)
	{
	  *value = arrow_bytes_to_long (buf);
	  rpc_log ("%llu", *value);
	}
  return ret;
}

int
read_string (rpc_t *rpc, char *buf, int len)
{
  int ret = read_value (rpc, 1, len, buf);
  buf[len] = '\0';
  if (ret == len)
	return 1;
  return 0;
}
