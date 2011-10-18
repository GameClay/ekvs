#include <ekvs/ekvs.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

int main()
{
   ekvs mystore;
   ekvs_opts opts;
   const char* test_str = "facepunch!";
   const char* get_str = NULL;
   size_t get_str_sz;

   memset(&opts, 0, sizeof(ekvs_opts));
   opts.initial_table_size = 2;
   opts.grow_threshold = 0.1f;

   ekvs_open(&mystore, "test_db", &opts);
   assert(ekvs_set(mystore, "face?", test_str, strlen(test_str) + 1) == EKVS_OK);
   ekvs_get(mystore, "face?", (const void**)&get_str, &get_str_sz);
   printf("Got back: '%s'\n", get_str);
   assert(ekvs_snapshot(mystore, NULL) == EKVS_OK);
   assert(ekvs_del(mystore, "face?") == EKVS_OK);
   get_str = NULL;
   ekvs_get(mystore, "face?", (const void**)&get_str, &get_str_sz);
   printf("Got back: '%s'\n", get_str);
   ekvs_close(mystore);
   return 0;
}
