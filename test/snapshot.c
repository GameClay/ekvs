#include <ekvs/ekvs.h>
#include "../src/ekvs_internal.h"

#include <cspec.h>
#include <cspec_output_verbose.h>

DESCRIBE(ekvs_snapshot, "int ekvs_snapshot(ekvs store, const char* snapshot_to)")
   IT("returns EKVS_FAIL if store is NULL")
      SHOULD_EQUAL(ekvs_snapshot(NULL, NULL), EKVS_FAIL)
   END_IT
   
   IT("returns EKVS_FILE_FAIL if a filename is not provided, and the database is in-memory only")
      ekvs teststore;
      ekvs_open(&teststore, NULL, NULL);
      SHOULD_EQUAL(ekvs_snapshot(teststore, NULL), EKVS_FILE_FAIL)
      SHOULD_EQUAL(ekvs_snapshot(teststore, ""), EKVS_FILE_FAIL)
      ekvs_close(teststore);
   END_IT

   IT("should store a snapshot of the table which can be loaded properly")
      ekvs teststore;
      ekvs_opts testopts;
      const void* get_ptr;
      size_t get_sz = 0;
      const char* snapshot_testfile = "snapshot_test";
      memset(&testopts, 0, sizeof(ekvs_opts));
      testopts.initial_table_size = 1;
      ekvs_open(&teststore, NULL, &testopts);
      ekvs_set(teststore, "key1", "value1", 7);
      ekvs_set(teststore, "key2", "value2", 7);
      ekvs_set(teststore, "key3", "value3", 7);
      SHOULD_EQUAL(ekvs_snapshot(teststore, snapshot_testfile), EKVS_OK)
      ekvs_close(teststore);
      SHOULD_EQUAL(ekvs_open(&teststore, snapshot_testfile, NULL), EKVS_OK)
      SHOULD_EQUAL(ekvs_get(teststore, "key1", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value1")
      SHOULD_EQUAL(ekvs_get(teststore, "key2", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value2")
      SHOULD_EQUAL(ekvs_get(teststore, "key3", &get_ptr, &get_sz), EKVS_OK)
      SHOULD_MATCH(get_ptr, "value3")
      ekvs_close(teststore);
      remove(snapshot_testfile);
   END_IT
END_DESCRIBE
