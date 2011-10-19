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
   struct _ekvs_db_entry* entry;

   memset(&opts, 0, sizeof(ekvs_opts));
   opts.initial_table_size = 2;
   opts.grow_threshold = 0.5f;

   ekvs_open(&mystore, "test_db", &opts);

   assert(ekvs_set(mystore, "face?", test_str, strlen(test_str) + 1) == EKVS_OK);
   assert(ekvs_get(mystore, "face?", (const void**)&get_str, &get_str_sz) == EKVS_OK);
   printf("Got back: '%s'\n", get_str);
   assert(ekvs_snapshot(mystore, NULL) == EKVS_OK);
   assert(ekvs_del(mystore, "face?") == EKVS_OK);
   get_str = NULL;
   assert(ekvs_get(mystore, "face?", (const void**)&get_str, &get_str_sz) == EKVS_NO_KEY);
   printf("Got back: '%s'\n", get_str);
   char buffer[L_tmpnam];
   char buffer2[L_tmpnam];
   for(int i = 0; i < 5000; i++)
   {
      tmpnam(buffer);
      tmpnam(buffer2);
      assert(ekvs_set_ex(mystore, buffer, buffer2, strlen(buffer2) + 1, ekvs_set_no_grow) == EKVS_OK);
      ekvs_get(mystore, buffer, (const void**)&get_str, &get_str_sz);
      if(get_str == NULL || strcmp(get_str, buffer2) != 0)
      {
         printf("key: '%s' value differs, chain error ('%s' vs '%s')\n", buffer, get_str, buffer2);
      }
   }

   ekvs_close(mystore);
   return 0;
}
