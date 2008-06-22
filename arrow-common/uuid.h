/* uuid.h -- 
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


#ifndef __UUID_H__
#define __UUID_H__

#include <stdint.h>
#include <stdlib.h>
#include <openssl/rand.h>

/*
 * How's this for a UUID library, bitches?
 */

typedef uint8_t uuid_t[16];

#define uuid_generate(dst) (RAND_pseudo_bytes((unsigned char *) (dst), \
											  sizeof (uuid_t)))
#define uuid_cmp(u1,u2) (memcmp ((void *) (u1), (void *) (u2), sizeof (uuid_t)))
#define uuid_equal(u1,u2) (uuid_cmp(u1,u2) == 0)
#define uuid_copy(dst,src) (memcpy(dst, src, sizeof(uuid_t)))

#define uuid_from_longs(uuid,upper,lower) do {				\
	arrow_long_to_bytes ((uint8_t *) (uuid), (upper));		\
	arrow_long_to_bytes (((uint8_t *) (uuid))+8, (lower));	\
  } while (0)

#define uuid_to_longs(uuid,upper,lower) do {				\
	(upper) = arrow_bytes_to_long ((uint8_t *) (uuid));		\
	(lower) = arrow_bytes_to_long (((uint8_t *) (uuid))+8);	\
  } while (0)

#define uuid_tostring(buf,uuid) do {									\
	memset (buf, 0, 24);												\
	b64_encode (arrow_bytes_to_long ((uint8_t *) uuid), buf);			\
	strcat(buf, ".");													\
	b64_encode (arrow_bytes_to_long (((uint8_t *) uuid)+8),				\
				(buf) + strlen(buf));									\
  } while (0)

#endif /* __UUID_H__ */
