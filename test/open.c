#include <ekvs/ekvs.h>
#include "../src/ekvs_internal.h"

#include <cspec.h>
#include <cspec_output_verbose.h>

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
