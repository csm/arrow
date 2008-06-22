/* cbuf.h -- 
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


#ifndef __CBUF_H__
#define __CBUF_H__

#include <stdint.h>
#include <stdlib.h>

typedef struct circular_buffer_s
{
  size_t size;
  size_t idx;
  uint8_t *buffer;
} circular_buffer_t;

#define cbuf_addin(buf,val) do {					\
	(buf)->buffer[(buf)->idx] = val;				\
	(buf)->idx = ((buf)->idx + 1) % (buf)->size;	\
  } while (0)
#define cbuf_get(buf,i) ((buf)->buffer[((buf)->idx + i) % (buf)->size]) 
#define cbuf_reset(buf) (buf)->idx = 0

int cbuf_alloc (circular_buffer_t * cbuf, size_t size);

#define cbuf_free(buf) free ((buf)->buffer)

/**
 * Digest a circular buffer with MD5.
 */
#define cbuf_md5(buf,md5,digest) do {									\
	MD5_Init (md5);														\
	MD5_Update ((md5), &((buf)->buffer[(buf)->idx]),					\
				(buf)->size - (buf)->idx);								\
	if ((buf)->idx != 0)												\
	  MD5_Update ((md5), (buf)->buffer, (buf)->idx);					\
	MD5_Final ((digest), (md5));										\
  } while (0)

#endif /* __CBUF_H__ */
