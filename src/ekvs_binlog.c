/* -*- Mode: C; tab-width: 3; c-basic-offset: 3; indent-tabs-mode: nil -*- */
/* vim: set filetype=C tabstop=3 softtabstop=3 shiftwidth=3 expandtab: */

/* ekvs -- Copyright (C) 2011 GameClay LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ekvs_internal.h"

int _ekvs_replay_binlog_entry(ekvs store, char operation, const struct _ekvs_db_entry* entry)
{
   int ret = EKVS_OK;
   char* key = ekvs_malloc(entry->key_sz + 1);
   memcpy(key, entry->key_data, entry->key_sz);
   key[entry->key_sz] = '\0';

   switch(operation)
   {
      case EKVS_BINLOG_SET:
      {
         const void* data = &entry->key_data[entry->key_sz];
         ret = ekvs_set(store, key, data, entry->data_sz);
         break;
      }
      case EKVS_BINLOG_DEL:
      {
         ret = ekvs_del(store, key);
         break;
      }
   }

   ekvs_free(key);

   return ret;
}

int _ekvs_binlog(ekvs store, char operation, char flags, const char* key, const void* data, size_t data_sz)
{
   FILE* binlog = store->db_file;
   size_t temp;
   struct _ekvs_db_entry entry;
   long int binlog_end = store->serialized.binlog_end;
   entry.flags = flags;
   entry.key_sz = strlen(key);
   entry.data_sz = data_sz;

   /* Write binlog entry */
   if(fseek(binlog, binlog_end, SEEK_SET) != 0) goto _ekvs_binlog_fail;
   if(fwrite(&operation, sizeof(operation), 1, binlog) != 1) goto _ekvs_binlog_fail;
   if(fwrite(&entry, sizeof(struct _ekvs_db_entry) - 1, 1, binlog) != 1) goto _ekvs_binlog_fail;
   if(fwrite(key, 1, entry.key_sz, binlog) != entry.key_sz) goto _ekvs_binlog_fail;
   if(fwrite(data, 1, entry.data_sz, binlog) != entry.data_sz) goto _ekvs_binlog_fail;

   /* Update binlog end */
   store->serialized.binlog_end = ftell(binlog);
   if(store->serialized.binlog_end == -1L) goto _ekvs_binlog_fail;

   /* TODO: Check size of binlog to see if we should write a snapshot */

   /* Write new binlog end */
   if(fseek(binlog, 0, SEEK_SET) != 0) goto _ekvs_binlog_fail;
   if(fwrite(&store->serialized, sizeof(store->serialized), 1, binlog) != 1) goto _ekvs_binlog_fail;

   /* Flush to disk */
   if(fflush(binlog) != 0) goto _ekvs_binlog_fail;

   return EKVS_OK;

_ekvs_binlog_fail:
   /* Rollback, and return file error */
   store->serialized.binlog_end = binlog_end;
   return EKVS_FILE_FAIL;
}
