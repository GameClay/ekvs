#include <ekvs/ekvs.h>
#include "../src/ekvs_internal.h"

#include <cspec.h>
#include <cspec_output_verbose.h>

DESCRIBE(ekvs_binlog, "ekvs binlog functionality for set/del instructions")
   IT("replays simple set instructions")
      ekvs teststore;
      const void* get_ptr;
      size_t get_sz = 0;
      const char* testfile = "binlog_test";
      ekvs_open(&teststore, testfile, NULL);
      ekvs_set_ex(teststore, "key1", "value1", 7, ekvs_set_no_grow);
      ekvs_close(teststore);
      ekvs_open(&teststore, testfile, NULL);
      SHOULD_EQUAL(ekvs_get(teststore, "key1", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value1")
      ekvs_close(teststore);
      remove(testfile);
   END_IT
   
   IT("replays set and del instructions")
      ekvs teststore;
      const void* get_ptr;
      size_t get_sz = 0;
      const char* testfile = "binlog_test";
      ekvs_open(&teststore, testfile, NULL);
      ekvs_set_ex(teststore, "key1", "value1", 7, ekvs_set_no_grow);
      ekvs_del(teststore, "key1");
      ekvs_close(teststore);
      ekvs_open(&teststore, testfile, NULL);
      SHOULD_EQUAL(ekvs_get(teststore, "key1", &get_ptr, &get_sz), EKVS_NO_KEY)
      ekvs_close(teststore);
      remove(testfile);
   END_IT
   
   IT("replays set and del instructions when chaining has occurred")
      ekvs teststore;
      const void* get_ptr;
      size_t get_sz = 0;
      const char* testfile = "binlog_test";
      ekvs_open(&teststore, testfile, NULL);
      ekvs_set_ex(teststore, "key1", "value1", 7, ekvs_set_no_grow);
      ekvs_del(teststore, "key1");
      ekvs_close(teststore);
      ekvs_open(&teststore, testfile, NULL);
      SHOULD_EQUAL(ekvs_get(teststore, "key1", &get_ptr, &get_sz), EKVS_NO_KEY)
      ekvs_close(teststore);
      remove(testfile);
   END_IT
END_DESCRIBE
