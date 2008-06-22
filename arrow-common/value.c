/* value.c -- 
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

#include "fail.h"
#include "value.h"

const char *
value_type_name (value_type type)
{
  switch (type)
    {
    case STRING: return "STRING";
    case INTEGER: return "INTEGER";
    case REAL: return "REAL";
    case LIST: return "LIST";
    case STRUCT: return "STRUCT";
    default:
      fail("invalid type %d", type);
    }
}

value *
value_new (value_type type, value_result *result)
{
  value *ret = (value *) malloc (sizeof (value));
  if (ret == NULL)
    {
      if (result != NULL)
        *result = MALLOC_ERROR;
      return NULL;
    }
  ret->type = type;
  switch (type)
    {
    case STRING:
      ret->value.string = NULL;
      break;

    case INTEGER:
      ret->value.integer = 0;
      break;

    case REAL:
      ret->value.real = 0.0;
      break;

    case LIST:
      ret->value.list_head = NULL;
      break;

    case STRUCT:
      ret->value.struct_head = NULL;
      break;

    default:
      if (result != NULL)
        *result = INVALID_ARGUMENT;
      free (ret);
      return NULL;
    }

  if (result != NULL)
    *result = VALUE_OK;
  return ret;
}

value_result
value_free (value *val)
{
  if (!val)
    return VALUE_OK;

  switch (val->type)
    {
    case STRING:
      if (val->value.string)
        bdestroy (val->value.string);
      free (val);
      break;

    case INTEGER:
    case REAL:
      free (val);
      break;

    case LIST:
      {
        list_node *n = val->value.list_head;
        while (n)
          {
            list_node *next = n->next;
            if (n->value)
              value_free (n->value);
            free (n);
            n = next;
          }
        free (val);
      }
      break;

    case STRUCT:
      {
        struct_node *n = val->value.struct_head;
        while (n)
          {
            struct_node *next = n->next;
            if (n->name)
              bdestroy (n->name);
            if (n->value)
              value_free (n->value);
            free (n);
            n = next;
          }
        free (val);
      }
      break;

    default:
      fail ("invalid object passed to value_free");
      return INTERNAL_BUG;
    }

  return VALUE_OK;
}

value *
value_clone (value *val, value_result *result)
{
  if (!val)
    {
      if (result != NULL)
        *result = INVALID_ARGUMENT;
      return NULL;
    }

  switch (val->type)
    {
    case STRING:
      return value_new_string (val->value.string, result);

    case INTEGER:
      return value_new_int (val->value.integer, result);

    case REAL:
      return value_new_real (val->value.real, result);

    case LIST:
      {
        value_result r;
        list_node *n = val->value.list_head;
        value *ret = value_new (LIST, result);
        if (ret == NULL)  // result already filled in
          return NULL;
        while (n)
          {
            r = value_list_append (ret, n->value);
            if (r != VALUE_OK)
              {
                if (result != NULL)
                  *result = r;
                value_free (ret);
                return NULL;
              }
            n = n->next;
          }
        return ret;
      }
      break;

    case STRUCT:
      {
        value_result r;
        struct_node *n = val->value.struct_head;
        value *ret = value_new (STRUCT, result);
        if (ret == NULL)
          return NULL;
        while (n != NULL)
          {
            r = value_struct_put (ret, n->name, n->value);
            if (r != STRUCT_VALUE_ADDED)
              {
                if (result != NULL)
                  *result = r;
                value_free (ret);
                return NULL;
              }
            n = n->next;
          }
        return ret;
      }
      break;
    }
}

value *
value_new_cstring (const char *str, value_result *result)
{
  value *ret = value_new (STRING, result);
  if (ret == NULL)
    return NULL;
  ret->value.string = cstr2bstr (str);
  if (ret->value.string == NULL)
    {
      if (result != NULL)
        *result = MALLOC_ERROR;
      free (ret);
      return NULL;
    }
  return ret;
}

value *
value_new_string (const_bstring string, value_result *result)
{
  value *ret = value_new (STRING, result);
  if (ret == NULL)
    return NULL;
  ret->value.string = bstrcpy (string);
  if (ret->value.string == NULL)
    {
      if (result != NULL)
        *result = MALLOC_ERROR;
      free (ret);
      return NULL;
    }
  return ret;
}

value *
value_new_int (uint32_t i, value_result *result)
{
  value *ret = value_new (INTEGER, result);
  if (ret == NULL)
    return NULL;
  ret->value.integer = i;
  return ret;
}

value *
value_new_real (double d, value_result *result)
{
  value *ret = value_new (REAL, result);
  if (ret == NULL)
    return NULL;
  ret->value.real = d;
  return ret;
}

value_result
value_list_append (value *list, value *item)
{
  value_result result;
  list_node *node;
  if (list->type != LIST)
    return TYPE_MISMATCH;
  node = (list_node *) malloc (sizeof (list_node));
  if (node == NULL)
    return MALLOC_ERROR;
  node->value = value_clone (item, &result);
  if (node->value == NULL)
    {
      free (node);
      return result;
    }
  node->next = NULL;
  if (list->value.list_head == NULL)
    list->value.list_head = node;
  else
    {
      list_node *n = list->value.list_head;
      while (n->next)
        n = n->next;
      n->next = node;
    }
  return VALUE_OK;
}

value_result
value_list_insert (value *list, int index, value *item)
{
  list_node *node;
  value_result r;

  if (list->type != LIST)
    return TYPE_MISMATCH;
  if (index < 0)
    return INVALID_ARGUMENT;
  node = (list_node *) malloc (sizeof (list_node));
  if (node == NULL)
    return MALLOC_ERROR;
  node->value = value_clone (item, &r);
  if (node->value == NULL)
    {
      free (node);
      return r;
    }
  node->next = NULL;

  if (list->value.list_head == NULL)
    {
      if (index != 0)
        {
          value_free (node->value);
          free (node);
          return INDEX_OUT_OF_RANGE;
        }
      list->value.list_head = node;
    }
  else
    {
      int i = 0;
      list_node *n = list->value.list_head;
      while (n->next && i < index)
        n = n->next;
      if (i != index)
        {
          value_free (node->value);
          free (node);
          return INDEX_OUT_OF_RANGE;
        }
      if (i == 0)
        {
          node->next = list->value.list_head;
          list->value.list_head = node;
        }
      else
        {
          node->next = n->next;
          n->next = node;
        }
    }
  return VALUE_OK;
}

value*
value_list_remove (value *list, int index, value_result *result)
{
  if (list->type != LIST)
    {
      if (result != NULL)
        *result = TYPE_MISMATCH;
      return NULL;
    }
  if (index < 0)
    {
      if (result != NULL)
        *result = INVALID_ARGUMENT;
      return NULL;
    }

  if (index == 0)
    {
      value *ret;
      list_node *n = list->value.list_head;
      if (n == NULL)
        {
          if (result != NULL)
            *result = INDEX_OUT_OF_RANGE;
          return NULL;
        }
      if (result != NULL)
        *result = VALUE_OK;
      list->value.list_head = n->next;
      ret = n->value;
      free (n);
      return ret;
    }
  else
    {
      int i = 0;
      value *ret;
      list_node *n = list->value.list_head;
      list_node *p = NULL;
      while (n && i < index)
        {
          p = n;
          n = n->next;
          i++;
        }
      if (n == NULL || i != index)
        {
          if (result != NULL)
            *result = INDEX_OUT_OF_RANGE;
          return NULL;
        }
      if (result != NULL)
        *result = VALUE_OK;
      p->next = n->next;
      n->next = NULL;
      ret = n->value;
      free (n);
      return ret;
    }
}

value *
value_list_get (value *list, int index, value_result *result)
{
  int i;
  list_node *n;
  if (list->type != LIST)
    {
      if (result != NULL)
        *result = TYPE_MISMATCH;
      return NULL;
    }
  i = 0;
  n = list->value.list_head;
  while (n != NULL && i < index)
    {
      n = n->next;
      i++;
    }
  if (n == NULL || i < index)
    {
      if (result != NULL)
        *result = INDEX_OUT_OF_RANGE;
      return NULL;
    }
  return n->value;
}

int
value_list_size (value *list, value_result *result)
{
  if (list->type != LIST)
    {
      if (result != NULL)
        *result = TYPE_MISMATCH;
      return -1;
    }
  int size = 0;
  list_node *n = list->value.list_head;
  while (n)
    {
      size++;
      n = n->next;
    }
  if (result != NULL)
    *result = VALUE_OK;
  return size;
}

value_result
value_struct_put (value *strct, const_bstring name, value *val)
{
  struct_node *n, *p;
  value_result result;
  value *v;

  if (strct->type != STRUCT)
    return TYPE_MISMATCH;

  n = strct->value.struct_head;
  while (n != NULL)
    {
      if (bstrcmp (n->name, name) == 0)
        {
          val = value_clone (val, &result);
          if (result != VALUE_OK)
            return result;
          free (n->value);
          n->value = val;
          return STRUCT_VALUE_REPLACED;
        }
      p = n;
      n = n->next;
    }

  n = (struct_node *) malloc (sizeof (struct_node));
  if (n == NULL)
    return MALLOC_ERROR;
  n->name = bstrcpy (name);
  if (p->name == NULL)
    {
      free (n);
      /* Hmm, FIXME: can also get here if name is invalid? */
      return MALLOC_ERROR;
    }
  n->value = value_clone (val, &result);
  if (result != VALUE_OK)
    {
      bdestroy (n->name);
      free (n);
      return result;
    }
  p->next = n;
  return STRUCT_VALUE_ADDED;
}

value *
value_struct_get (value *strct, const_bstring name, value_result *result)
{
  struct_node *n;

  if (strct->type != STRUCT)
    {
      if (result != NULL)
        *result = TYPE_MISMATCH;
      return NULL;
    }

  n = strct->value.struct_head;
  while (n)
    {
      if (bstrcmp (n->name, name) == 0)
        {
          if (result != NULL)
            *result = VALUE_OK;
          return n->value;
        }
      n = n->next;
    }

  if (result != NULL)
    *result = STRUCT_KEY_ERROR;
  return NULL;
}

value *
value_struct_remove (value *strct, const_bstring name, value_result *result)
{
  struct_node *n, *p;

  if (strct->type != STRUCT)
    {
      if (result != NULL)
        *result = TYPE_MISMATCH;
      return NULL;
    }

  n = strct->value.struct_head;
  p = NULL;
  while (n != NULL)
    {
      if (bstrcmp (n->name, name) == 0)
        {
          value *v = n->value;
          if (p == NULL)
            {
              strct->value.struct_head = n->next;
              bdestroy (n->name);
              free (n);
            }
          else
            {
              p->next = n->next;
              bdestroy (n->name);
              free (n);
            }
          return v;
        }
      p = n;
      n = n->next;
    }

  if (result != NULL)
    *result = STRUCT_KEY_ERROR;
  return NULL;
}

int
value_cmp (value *v1, value *v2, value_result *result)
{
  if (v1->type != v2->type)
    {
      if (result != NULL)
        *result = TYPE_MISMATCH;
      return v1->type - v2->type;
    }
  if (result != NULL)
    *result = VALUE_OK;
  switch (v1->type)
    {
    case STRING:
      if (v1->value.string == NULL)
        {
          if (v2->value.string == NULL)
            return 0;
          return -1;
        }
      else
        {
          if (v2->value.string == NULL)
            return 1;
          return bstrcmp (v1->value.string, v2->value.string);
        }

    case INTEGER:
      if (v1->value.integer < v2->value.integer)
        return -1;
      else if (v1->value.integer > v2->value.integer)
        return 1;
      else
        return 0;

    case REAL:
      if (v1->value.real < v2->value.real)
        return -1;
      else if (v1->value.real > v2->value.real)
        return 1;
      else
        return 0;

    case LIST:
      {
        list_node *n1;
        list_node *n2;
        value_result r;
        int sz1, sz2;
        sz1 = value_list_size (v1, &r);
        if (r != VALUE_OK)
          {
            printf ("sz1 %d\n", r);
            if (result != NULL)
              *result = r;
            return -1;
          }
        sz2 = value_list_size (v2, &r);
        if (r != VALUE_OK)
          {
            printf ("sz2 %d\n", r);
            if (result != NULL)
              *result = r;
            return -1;
          }
        if (sz1 < sz2)
          return -1;
        if (sz1 > sz2)
          return 1;
        for (n1 = v1->value.list_head, n2 = v2->value.list_head;
             n1 != NULL && n2 != NULL; n1 = n1->next, n2 = n2->next)
          {
            int c = value_cmp (n1->value, n2->value, &r);
            if (r != VALUE_OK)
              {
                if (result != NULL)
                  *result = r;
                return c;
              }
            if (c != 0)
              return c;
          }
        return 0;
      }

    case STRUCT:
      fail ("implement me");

    default:
      fail ("invalid type seen %d\n", v1->type);
      if (result != NULL)
        *result = INTERNAL_BUG;
      return 0;
    }
}

/* Local Variables: */
/* tab-width: 8 */
/* indent-tabs-mode: nil */
/* c-basic-offset: 2 */
/* End: */
