/* base64.c -- Encode/decode integers in base64 format
 * Created: Mon Sep 23 16:55:12 1996 by faith@dict.org
 * Revised: Sat Mar 30 12:02:36 2002 by faith@dict.org
 * Copyright 1996, 2002 Rickard E. Faith (faith@dict.org)
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Library General Public License as published
 * by the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 * 
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 * 
 * $Id: base64.c,v 1.5 2002/08/02 19:43:15 faith Exp $
 *
 * \section{Base-64 Routines}
 *
 * \intro These routines use the 64-character subset of International
 * Alphabet IA5 discussed in RFC 1421 (printeable encoding) and RFC 1522
 * (base64 MIME).
 *

   Value Encoding  Value Encoding  Value Encoding  Value Encoding
       0 A            17 R            34 )            51 }
       1 B            18 S            35 ,            52 0
       2 C            19 T            36 -            53 1
       3 D            20 U            37 :            54 2
       4 E            21 V            38 ;            55 3
       5 F            22 W            39 <            56 4
       6 G            23 X            40 >            57 5
       7 H            24 Y            41 ?            58 6
       8 I            25 Z            42 @            59 7
       9 J            26 !            43 [            60 8
      10 K            27 "            44 ~            61 9
      11 L            28 #            45 ]            62 +
      12 M            29 $            46 ^            63 *
      13 N            30 %            47 _
      14 O            31 &            48 `         (pad) =
      15 P            32 '            49 {
      16 Q            33 (            50 |
 *
 */

/*
 * Summary of changes by Casey Marshall for arrow:
 *
 * - extend idea to 64-bit values.
 * - only use characters that can be parts of a file name on
 *   case-insensitive file systems.
 *
 * As a part of Arrow, this file is licensed 
 */

#include <stdint.h>
#include <string.h>

static unsigned char b64_list[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'(),-:;<>?@[~]^_`{|}0123456789+*";

#define XX 100

static int b64_index[256] = {
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,26,27,28, 29,30,31,32, 33,34,63,62, 35,36,XX,XX,
    52,53,54,55, 56,57,58,59, 60,61,37,38, 39,XX,40,41,
    42, 0, 1, 2,  3, 4, 5, 6,  7, 8, 9,10, 11,12,13,14,
    15,16,17,18, 19,20,21,22, 23,24,25,43, XX,45,46,47,
    48,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,49, 50,51,44,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
    XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX, XX,XX,XX,XX,
};

/* |b64_encode| encodes |val| in a printable base 64 format.  A MSB-first
   encoding is generated. */

void
b64_encode (uint64_t val, char *result)
{
  char *c;
  int i;

  result[ 0] = b64_list[ (val & 0xf000000000000000ULL) >> 60 ];
  result[ 1] = b64_list[ (val & 0x0fc0000000000000ULL) >> 54 ];
  result[ 2] = b64_list[ (val & 0x003f000000000000ULL) >> 48 ];
  result[ 3] = b64_list[ (val & 0x0000fc0000000000ULL) >> 42 ];
  result[ 4] = b64_list[ (val & 0x000003f000000000ULL) >> 36 ];
  result[ 5] = b64_list[ (val & 0x0000000fc0000000ULL) >> 30 ];
  result[ 6] = b64_list[ (val & 0x000000003f000000ULL) >> 24 ];
  result[ 7] = b64_list[ (val & 0x0000000000fc0000ULL) >> 18 ];
  result[ 8] = b64_list[ (val & 0x000000000003f000ULL) >> 12 ];
  result[ 9] = b64_list[ (val & 0x0000000000000fc0ULL) >>  6 ];
  result[10] = b64_list[ (val & 0x000000000000003fULL)       ];
  result[11] = 0;

  c = result + 10;
  for (i = 0; i < 10; i++)
	{
	  if (result[i] != b64_list[0])
		{
		  c = result + i;
		  break;
		}
	}
  memmove (result, c, strlen(c) + 1);
}

int
b64_decode (const char *val, uint64_t *out)
{
   uint64_t v = 0;
   int i;
   int offset = 0;
   int len = strlen (val);

   for (i = len - 1; i >= 0; i--)
	 {
	   uint64_t tmp = b64_index[(unsigned char) val[i]];

	   if (tmp == XX)
		 return -1;
      
	   v |= tmp << offset;
	   offset += 6;
	 }

   *out = v;
   return 0;
}
