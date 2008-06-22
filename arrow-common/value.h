/* value.h -- 
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


#ifndef __VALUE_H__
#define __VALUE_H__
#include <stdint.h>
#include <bstrlib.h>

/**
 * \file value.h
 *
 * The types and functions of this file represent a simple "value
 * coding" scheme, which encapsualtes a handful of simple types into a
 * unified interface, primarily for simple translation to an RPC
 * layer.
 *
 * The data types represented here are simple, but are expressive
 * enough to encode a variety of data. Also, the container types only
 * guarantee O(n) complexity, so they are not intended for large data
 * stuctures.
 */

typedef enum
{
  VALUE_OK,               /**< OK. No error. */
  TYPE_MISMATCH,          /**< A value of the wrong type was specified.*/
  STRUCT_VALUE_ADDED,     /**< A new value was added to a struct (not an error). */
  STRUCT_VALUE_REPLACED,  /**< An existing value was replaced in a struct
                               (not an error). */
  STRUCT_KEY_ERROR,       /**< The requested key was not found in a struct. */
  INDEX_OUT_OF_RANGE,     /**< The given index was not in the list's size range. */
  INVALID_ARGUMENT,       /**< An argument to a function was invalid. */
  MALLOC_ERROR,           /**< Allocating memory failed. */
  INTERNAL_BUG            /**< An internal, unexpected condition. */
} value_result;

typedef enum
{
  STRING,                 /**< A character string. Contains C strings as a subset. */
  INTEGER,                /**< An unsigned, 32-bit integer. */
  REAL,                   /**< A floating-point value. */
  LIST,                   /**< A list of values. */
  STRUCT                  /**< A list of string->value pairs. */
} value_type;

struct value_s;

/**
 * A list node. Lists are implemented as singly-linked lists.
 */
typedef struct list_node_s
{
  struct value_s *value;    /**< The node value. */
  struct list_node_s *next; /**< Pointer to the next node, or NULL. */
} list_node;

/**
 * A struct node. Structs are implemented as singly-linked lists.
 */
typedef struct struct_node_s
{
  bstring name;               /**< The node's name (the key). */
  struct value_s *value;      /**< The node's value. */
  struct struct_node_s *next; /**< Pointer to the next node, or NULL. */
} struct_node;

/**
 * A value. Values are generic wrapper objects around one of five
 * defined types.
 */
typedef struct value_s
{
  value_type type;  /**< This values type. */
  union
  {
    bstring string;
    uint32_t integer;
    double real;
    list_node *list_head;
    struct_node *struct_head;
  } value;
} value;

/**
 * Convenience macro, to get a value's type.
 *
 * \param v The value.
 * \return The value's type.
 */
#define value_get_type(v) (((value *) v)->type)

/**
 * Return a printable name for a type.
 *
 * \param type The type.
 * \reutrn A string for the type.
 */
const char *value_type_name (value_type type);

/**
 * Create a new, empty value with the given type. The new value is
 * returned, and must be freed by the caller with value_free. If
 * memory cannot be allocated, or if the specified type is not a
 * value_type, an error will be placed in result, and NULL is
 * returned.
 *
 * \param type The type of value to create.
 * \param result Storage for the result code. May be NULL.
 * \return The new value, or NULL if an error occurs.
 */
value *value_new (value_type type, value_result *result);

/**
 * Release the memory used by the given value.
 *
 * If val is a container type -- a list or struct -- then everything
 * in that container will also be freed.
 *
 * \param val The value to free.
 * \return VALUE_OK if the value was properly freed..
 */
value_result value_free (value *val);

/**
 * Copy a value.
 *
 * This does a deep copy of the given value, so strings, lists, and
 * structs will have all their values copied. The returned pointer
 * must be freed by the caller.
 *
 * \param val The value to clone.
 * \result Storage for the result code. May be NULL.
 * \return The copied object, or NULL if an error occurs.
 */
value *value_clone (value *val, value_result *result);

/**
 * Create a new STRING value that contains the given C string.
 *
 * \param str The C string value.
 * \param result Storage for the result code. May be NULL.
 * \return The newly-created string, or NULL if an error occurs.
 */
value *value_new_cstring (const char *str, value_result *result);

/**
 * Create a new STRING from an existing bstring. The given string is
 * copied by this function.
 *
 * \param string The string value.
 * \param result Storage for the result code. May be NULL.
 * \return The newly-created string, or NULL if an error occurs.
 */
value *value_new_string (const_bstring string, value_result *result);

/**
 * Create a new INTEGER value from an int value.
 *
 * \param i The integer value.
 * \param result Storage for the result code. May be NULL.
 * \reutrn The newly-created value, or NULL if an error occurs.
 */
value *value_new_int (uint32_t i, value_result *result);

/**
 * Create a new REAL value from a double.
 *
 * \param d The double value.
 * \param result Storage for the result code. May be NULL.
 * \return The newly-created value, or NULL if an error occurs.
 */
value *value_new_real (double d, value_result *result);

/**
 * Append a value to the end of a list. The value to be appended is
 * copied by this function, and the argument is not used after that.
 *
 * \param list The list to append to.
 * \param item The item to append. May not be NULL.
 * \return VALUE_OK if the item was appended, or an error code.
 */
value_result value_list_append (value *list, value *item);

/**
 * Insert a value somewhere in a list. The value to be inserted is
 * copied by this function, and the target list will grow by one
 * element.
 *
 * Complexity is O(n).
 *
 * \param list The list to insert into.
 * \param index The index to insert the element at.
 * \param item The value to insert.
 * \return VALUE_OK if the item was inserted, or an error code.
 */
value_result value_list_insert (value *list, int index, value *item);

/**
 * Remove a value from a list at the given index. The removed value is
 * returned, and must be freed by the caller.
 *
 * Complexity is O(n).
 *
 * \param list The list to removed the item from.
 * \param index The index of the item to remove.
 * \param result Storage for the result code. May be NULL.
 * \return The removed item, or NULL if an error occurs.
 */
value *value_list_remove (value *list, int index, value_result *result);

/**
 * Get an item from a list. The returned item need not be freed by the
 * caller.
 *
 * This is largely a convenience function. One can acheive the same
 * results by iterating through the linked list.
 *
 * Complexity is O(n).
 *
 * \param list The list to get the item from.
 * \param index The index of the item to get.
 * \param result Storage for the result code. May be NULL.
 * \return The item at the given index, or NULL if an error occurs.
 */
value *value_list_get (value *list, int index, value_result *result);

/**
 * Get the number of elements in a list.
 *
 * Complexity is O(n).
 *
 * \param list The list.
 * \param Storage for the result code. May be NULL.
 * \return The number of elements in the list, or -1 if an error occurs.
 */
int value_list_size (value *list, value_result *result);

/**
 * Put a key/value pair into a struct. Both the name and the value are
 * copied by this function.
 *
 * Complexity is O(n).
 *
 * \param strct The struct to add the value to.
 * \param name The name to put.
 * \param value The value to put.
 * \return STRUCT_VALUE_ADDED or STRUCT_VALUE_REPLACED on success, or
 * an error code.
 */
value_result value_struct_put (value *strct, const_bstring name, value *value);

/**
 * Get a value from a struct for the given key. The returned value
 * does not need to be freed by the caller.
 *
 * Complexity is O(n). 
 *
 * \param strct The struct to get the value from.
 * \param name The name of the value to get.
 * \param result Storage for the result code. May be NULL.
 * \return The value, or NULL if an error occurs.
 */
value *value_struct_get (value *strct, const_bstring name, value_result *result);

/**
 * Remove a named item from a struct. The removed value is returned to
 * the caller, and it is the caller's responsibility to free the
 * memory taken by that value.
 *
 * \param strct The structure to remove the value from.
 * \param name The name of the key to remove.
 * \param result Storage for the result code. May be NULL.
 * \return The removed value, or NULL if an error occurs.
 */
value *value_struct_remove (value *strct, const_bstring name, value_result *result);

/**
 * Compare two values, returning an integer less than, equal to, or
 * greater than zero corresponding to the comparison result.
 *
 * Integers and reals are compared with the greater than/less than
 * operators. Strings are compare lexicographically.
 *
 * Lists are compared by length, then itemwise. If in list A value 2
 * is greater than value 2 in list B, then list A is considered
 * greater than list B.
 *
 * Structs are compared similarly, but the itemwise comparision works
 * first by name, then by value.
 *
 * It is considered an error if you compare values of different
 * type. The result of such a comparision is simply the difference of
 * the two value types, and thus may not be cleanly defined.
 *
 * \param v1 The first value.
 * \param v2 The second value.
 * \param result Storage for the result code. May be NULL.
 * \return An integer less than, equal to, or greater than zero if the
 * first value is less than, equal to, or greater than the second
 * value, respectively.
 */
int value_cmp (value *v1, value *v2, value_result *result);

#endif /* __VALUE_H__ */

/* Local Variables: */
/* tab-width: 8 */
/* indent-tabs-mode: nil */
/* c-basic-offset: 2 */
/* End: */
