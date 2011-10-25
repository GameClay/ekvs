#include <ekvs/ekvs.h>
#include "../src/ekvs_internal.h"

#include <cspec.h>
#include <cspec_output_verbose.h>

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
