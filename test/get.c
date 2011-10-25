#include <ekvs/ekvs.h>
#include "../src/ekvs_internal.h"

#include <cspec.h>
#include <cspec_output_verbose.h>

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
