#include <ekvs/ekvs.h>
#include "../src/ekvs_internal.h"

#include <cspec.h>
#include <cspec_output_verbose.h>

DESCRIBE(ekvs_del, "int ekvs_del(ekvs store, const char* key)")
   IT("returns EKVS_FAIL if store is NULL")
      SHOULD_EQUAL(ekvs_del(NULL, "key"), EKVS_FAIL)
   END_IT

   IT("returns EKVS_FAIL if key is NULL")
      ekvs teststore;
      ekvs_open(&teststore, NULL, NULL);
      SHOULD_EQUAL(ekvs_del(teststore, NULL), EKVS_FAIL)
      ekvs_close(teststore);
   END_IT

   IT("returns EKVS_NO_KEY if the key does not exist")
      ekvs teststore;
      ekvs_open(&teststore, NULL, NULL);
      SHOULD_EQUAL(ekvs_del(teststore, "does_not_exist"), EKVS_NO_KEY)
      ekvs_close(teststore);
   END_IT

   IT("deletes the value for the specified key")
      ekvs teststore;
      const void* get_ptr;
      size_t get_sz = 0;
      ekvs_open(&teststore, NULL, NULL);
      ekvs_set(teststore, "key", "value", 6);
      SHOULD_EQUAL(ekvs_del(teststore, "key"), EKVS_OK)
      SHOULD_EQUAL(ekvs_get(teststore, "key", &get_ptr, &get_sz), EKVS_NO_KEY)
      ekvs_close(teststore);
   END_IT

   IT("deletes the proper value if chaining has occurred")
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
      SHOULD_EQUAL(ekvs_del(teststore, "key2"), EKVS_OK)
      SHOULD_EQUAL(ekvs_get(teststore, "key1", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value1")
      SHOULD_EQUAL(ekvs_get(teststore, "key2", &get_ptr, &get_sz), EKVS_NO_KEY)
      SHOULD_EQUAL(ekvs_get(teststore, "key3", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value3")
      ekvs_close(teststore);
   END_IT
   
   IT("deletes the proper value if chaining has occurred, and a key has been re-assigned")
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
      ekvs_set_ex(teststore, "key1", "value4", 7, ekvs_set_no_grow);
      ekvs_set_ex(teststore, "key2", "value5", 7, ekvs_set_no_grow);
      SHOULD_EQUAL(teststore->serialized.table_sz, 1)
      SHOULD_EQUAL(ekvs_del(teststore, "key2"), EKVS_OK)
      SHOULD_EQUAL(ekvs_get(teststore, "key1", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value4")
      SHOULD_EQUAL(ekvs_get(teststore, "key2", &get_ptr, &get_sz), EKVS_NO_KEY)
      SHOULD_EQUAL(ekvs_get(teststore, "key3", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value3")
      ekvs_close(teststore);
   END_IT
END_DESCRIBE
