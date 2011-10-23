#include <ekvs/ekvs.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include <cspec.h>
#include <cspec_output_verbose.h>

#include "../src/ekvs_internal.h"

int test_malloc_count = 0;
size_t test_allocation_total = 0;
size_t test_free_total = 0;
void* test_malloc(size_t size)
{
   char* ret = malloc(size + sizeof(size_t));
   test_malloc_count++;
   test_allocation_total += size;
   *((size_t*)ret) = size;
   return (ret + sizeof(size_t));
}

int test_realloc_count = 0;
void* test_realloc(void* ptr, size_t size)
{
   test_realloc_count++;
   if(ptr != NULL)
   {
      char* fptr = ((char*)ptr) - sizeof(size_t);
      test_free_total += *((size_t*)fptr);
      test_allocation_total += size;
      *((size_t*)fptr) = size;
      return realloc(fptr, size + sizeof(size_t));
   }
   
   char* ret = realloc(ptr, size + sizeof(size_t));
   test_allocation_total += size;
   *((size_t*)ret) = size;
   return (ret + sizeof(size_t));
}

int test_free_count = 0;
void test_free(void* ptr)
{
   if(ptr != NULL)
   {
      char* fptr = ((char*)ptr) - sizeof(size_t);
      test_free_count++;
      test_free_total += *((size_t*)fptr);
      free(fptr);
   }
}

DESCRIBE(ekvs_open, "int ekvs_open(ekvs* store, const char* path, const ekvs_opts* opts)")
   IT("returns EKVS_FAIL if store is NULL")
      SHOULD_EQUAL(ekvs_open(NULL, NULL, NULL), EKVS_FAIL)
   END_IT

   IT("returns EKVS_OK if store is valid, and other parameters are NULL")
      ekvs teststore;
      SHOULD_EQUAL(ekvs_open(&teststore, NULL, NULL), EKVS_OK)
      ekvs_close(teststore);
   END_IT

   IT("returns EKVS_FAIL if some custom allocators are specified, but not all")
      ekvs teststore;
      ekvs_opts testopts;
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.user_malloc = test_malloc;
      SHOULD_EQUAL(ekvs_open(&teststore, NULL, &testopts), EKVS_FAIL)
   END_IT
   
   IT("returns EKVS_OK if all custom allocators are sepcified")
      ekvs teststore;
      ekvs_opts testopts;
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.user_malloc = test_malloc;
      testopts.user_realloc = test_realloc;
      testopts.user_free = test_free;
      SHOULD_EQUAL(ekvs_open(&teststore, NULL, &testopts), EKVS_OK)
      ekvs_close(teststore);
   END_IT

   IT("creates a table with the specified initial table size")
      ekvs teststore;
      ekvs_opts testopts;
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.initial_table_size = 42;
      SHOULD_EQUAL(ekvs_open(&teststore, NULL, &testopts), EKVS_OK)
      SHOULD_EQUAL(teststore->serialized.table_sz, 42)
      ekvs_close(teststore);
   END_IT

   IT("creates a table with the specified grow threshold")
      ekvs teststore;
      ekvs_opts testopts;
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.grow_threshold = 4.2f;
      SHOULD_EQUAL(ekvs_open(&teststore, NULL, &testopts), EKVS_OK)
      SHOULD_EQUAL(teststore->grow_threshold, 4.2f)
      ekvs_close(teststore);
   END_IT

   IT("uses custom allocators for memory functions")
      ekvs teststore;
      ekvs_opts testopts;
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.user_malloc = test_malloc;
      testopts.user_realloc = test_realloc;
      testopts.user_free = test_free;
      SHOULD_EQUAL(ekvs_open(&teststore, NULL, &testopts), EKVS_OK)
      ekvs_close(teststore);
      SHOULD_NOT_EQUAL(test_malloc_count, 0)
      SHOULD_NOT_EQUAL(test_free_count, 0)
   END_IT

   IT("does not leak memory")
      ekvs teststore;
      ekvs_opts testopts;
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.user_malloc = test_malloc;
      testopts.user_realloc = test_realloc;
      testopts.user_free = test_free;
      ekvs_open(&teststore, NULL, &testopts);
      ekvs_close(teststore);
      SHOULD_EQUAL(test_allocation_total, test_free_total)
   END_IT
END_DESCRIBE

DESCRIBE(ekvs_set_ex, "int ekvs_set_ex(ekvs store, const char* key, const void* data, size_t data_sz, char flags)")
   IT("returns EKVS_FAIL if store is NULL")
      SHOULD_EQUAL(ekvs_set_ex(NULL, NULL, NULL, 0, 0), EKVS_FAIL)
   END_IT
   
   IT("returns EKVS_FAIL if key is NULL")
      ekvs teststore;
      ekvs_open(&teststore, NULL, NULL);
      SHOULD_EQUAL(ekvs_set_ex(teststore, NULL, NULL, 0, 0), EKVS_FAIL)
      ekvs_close(teststore);
   END_IT
   
   IT("returns EKVS_OK if key is valid, and data is NULL")
      ekvs teststore;
      const void* get_ptr;
      size_t get_sz = 0;
      ekvs_open(&teststore, NULL, NULL);
      SHOULD_EQUAL(ekvs_set_ex(teststore, "key", NULL, 0, 0), EKVS_OK)
      SHOULD_EQUAL(ekvs_get(teststore, "key", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_EQUAL(get_sz, 0)
      ekvs_close(teststore);
   END_IT
   
   IT("should not grow the table if the ekvs_set_no_grow flag is specified")
      ekvs teststore;
      ekvs_opts testopts;
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.initial_table_size = 1;
      ekvs_open(&teststore, NULL, &testopts);
      SHOULD_EQUAL(ekvs_set_ex(teststore, "key", NULL, 0, ekvs_set_no_grow), EKVS_OK)
      SHOULD_EQUAL(ekvs_set_ex(teststore, "key2", NULL, 0, ekvs_set_no_grow), EKVS_OK)
      SHOULD_EQUAL(ekvs_set_ex(teststore, "key3", NULL, 0, ekvs_set_no_grow), EKVS_OK)
      SHOULD_EQUAL(teststore->serialized.table_sz, 1)
      ekvs_close(teststore);
   END_IT
   
   IT("should the table if the grow threshold is exceeded by adding a key")
      ekvs teststore;
      ekvs_opts testopts;
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.initial_table_size = 1;
      ekvs_open(&teststore, NULL, &testopts);
      SHOULD_EQUAL(ekvs_set_ex(teststore, "key", NULL, 0, 0), EKVS_OK)
      SHOULD_EQUAL(ekvs_set_ex(teststore, "key2", NULL, 0, 0), EKVS_OK)
      SHOULD_EQUAL(ekvs_set_ex(teststore, "key3", NULL, 0, 0), EKVS_OK)
      SHOULD_NOT_EQUAL(teststore->serialized.table_sz, 1)
      ekvs_close(teststore);
   END_IT
END_DESCRIBE

DESCRIBE(ekvs_get, "int ekvs_get(ekvs store, const char* key, const void** data, size_t* data_sz)")
   IT("returns EKVS_FAIL if store is NULL")
      const void* get_ptr;
      size_t get_sz = 0;
      SHOULD_EQUAL(ekvs_get(NULL, "key", &get_ptr, &get_sz), EKVS_FAIL)
   END_IT
   
   IT("returns EKVS_FAIL if key is NULL")
      ekvs teststore;
      const void* get_ptr;
      size_t get_sz = 0;
      ekvs_open(&teststore, NULL, NULL);
      SHOULD_EQUAL(ekvs_get(teststore, NULL, &get_ptr, &get_sz), EKVS_FAIL)
      ekvs_close(teststore);
   END_IT
   
   IT("returns EKVS_NO_KEY if the key does not exist")
      ekvs teststore;
      const void* get_ptr;
      size_t get_sz = 0;
      ekvs_open(&teststore, NULL, NULL);
      SHOULD_EQUAL(ekvs_get(teststore, "does_not_exist", &get_ptr, &get_sz), EKVS_NO_KEY)
      ekvs_close(teststore);
   END_IT
   
   IT("returns the assigned value if the key is set")
      ekvs teststore;
      const void* get_ptr;
      size_t get_sz = 0;
      ekvs_open(&teststore, NULL, NULL);
      ekvs_set(teststore, "key", "value", 6);
      SHOULD_EQUAL(ekvs_get(teststore, "key", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value")
      ekvs_close(teststore);
   END_IT
   
   IT("returns the assigned value if the key is set, and chaining has occurred")
      ekvs teststore;
      const void* get_ptr;
      size_t get_sz = 0;
      ekvs_opts testopts;
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.initial_table_size = 1;
      ekvs_open(&teststore, NULL, &testopts);
      ekvs_set_ex(teststore, "key1", "value1", 7, ekvs_set_no_grow);
      ekvs_set_ex(teststore, "key2", "value2", 7, ekvs_set_no_grow);
      ekvs_set_ex(teststore, "key3", "value3", 7, ekvs_set_no_grow);
      SHOULD_EQUAL(teststore->serialized.table_sz, 1)
      SHOULD_EQUAL(ekvs_get(teststore, "key1", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value1")
      SHOULD_EQUAL(ekvs_get(teststore, "key2", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value2")
      SHOULD_EQUAL(ekvs_get(teststore, "key3", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value3")
      ekvs_close(teststore);
   END_IT
END_DESCRIBE

int main()
{
   CSpec_Run(DESCRIPTION(ekvs_open), CSpec_NewOutputVerbose());
   CSpec_Run(DESCRIPTION(ekvs_set_ex), CSpec_NewOutputVerbose());
   CSpec_Run(DESCRIPTION(ekvs_get), CSpec_NewOutputVerbose());
   return 0;
}
