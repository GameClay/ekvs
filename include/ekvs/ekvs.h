/* -*- Mode: C; tab-width: 3; c-basic-offset: 3; indent-tabs-mode: nil -*- */
/* vim: set filetype=C tabstop=3 softtabstop=3 shiftwidth=3 expandtab: */

/* ekvs -- Copyright (C) 2011 GameClay LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _EKVS_H_
#define _EKVS_H_

#include <stdlib.h>
#include <stdint.h>

#ifndef EKVS_API
#  define EKVS_API
#endif

typedef void* (*ekvs_malloc_ptr)(size_t size);
typedef void* (*ekvs_realloc_ptr)(void* ptr, size_t size);
typedef void (*ekvs_free_ptr)(void* ptr);

/**
 * Options for operation and initialization of the ekvs database
 */
typedef struct ekvs_opts ekvs_opts;
struct ekvs_opts {
   uint64_t initial_table_size;  /**< Initial table size for new, or in-memory databases. If 0, the value EKVS_INITIAL_TABLE_SIZE will be used. */
   ekvs_malloc_ptr user_malloc;  /**< Pointer to a malloc function. Specify NULL to use standard malloc. */
   ekvs_realloc_ptr user_realloc;/**< Pointer to a realloc function. Specify NULL to use standard realloc. */
   ekvs_free_ptr user_free;      /**< Pointer to a free function. Specify NULL to use standard free. */
};

typedef struct _ekvs_db* ekvs;

#define EKVS_OK               0x00 /**< Operation successful */
#define EKVS_FAIL             0x10 /**< Operation failed due to a non-specific error */
#define EKVS_ALLOCATION_FAIL  0x11 /**< Operation failed due to a memory allocation error */
#define EKVS_FILE_FAIL        0x12 /**< Operation failed due to a file i/o error */
#define EKVS_NO_KEY           0x13 /**< Operation failed because the key did not exist */

extern EKVS_API int ekvs_open(ekvs* store, const char* path, const ekvs_opts* opts);

extern EKVS_API void ekvs_close(ekvs store);

extern EKVS_API int ekvs_last_error(ekvs store);

extern EKVS_API int ekvs_set(ekvs store, const char* key, const void* data, size_t data_sz);

extern EKVS_API int ekvs_get(ekvs store, const char* key, const void** data, size_t* data_sz);

extern EKVS_API int ekvs_del(ekvs store, const char* key);

/* lists/sets ala redis? */

#define EKVS_INITIAL_TABLE_SIZE 128

#endif
