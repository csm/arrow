/* base64.h -- 
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


#ifndef __BASE64_H__
#define __BASE64_H__

#include <stdint.h>

/*
 * This is a modified base-64 encoder/decoder for 64-bit values. It is
 * modified to replace the '/' character with '*', to make the
 * resulting string filesystem friendly.
 */

/**
 * \param value The value to encode.
 * \param result Storage for the base-64 result. The value is not
 *        zero-padded, and the result buffer must be large enough for
 *        12 bytes.
 */
void b64_encode (uint64_t value, char *result);

/**
 * \param value The base-64 value to decode.
 * \param result Storage for the decode result.
 * \return Zero on success, nonzero if the value was malformed.
 */
int b64_decode (const char *value, uint64_t *result);

#endif /* __BASE64_H__ */
