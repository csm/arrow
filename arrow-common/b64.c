#include <stdio.h>
#include "base64.h"

int
main (int argc, char **argv)
{
  char buffer[12];
  uint64_t x;
  int i;

  for (i = 1; i < argc; i++)
    {
      x = (uint64_t) atoi (argv[i]);
      b64_encode (x, buffer);
      printf ("%llu -> %s\n", x, buffer);
    }
}
