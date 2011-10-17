#include <ekvs/ekvs.h>
#include <string.h>
#include <stdio.h>

int main()
{
   ekvs mystore;
   const char* test_str = "facepunch!";
   const char* get_str = NULL;
   size_t get_str_sz;
   ekvs_open(&mystore, "test_db", NULL);
   /*ekvs_set(mystore, "test", test_str, strlen(test_str) + 1);*/
   ekvs_get(mystore, "test", (const void**)&get_str, &get_str_sz);
   printf("Got back: '%s'\n", get_str);
   ekvs_close(mystore);
   return 0;
}
