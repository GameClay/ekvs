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
#include <string.h>

int _ekvs_replay_binlog_entry(ekvs store, struct _kevs_binlog_entry* entry)
{
   int ret = EKVS_OK;
   switch(entry->operation)
   {
      case EKVS_BINLOG_SET:
      {
         void* data = &entry->key_data[entry->key_sz + 1];
         ret = ekvs_set(store, entry->key_data, data, entry->data_sz);
         break;
      }
      case EKVS_BINLOG_DEL:
      {
         ret = ekvs_del(store, entry->key_data);
         break;
      }
   }

   return ret;
}

int _ekvs_binlog(ekvs store, char operation, char flags, const char* key, const void* data, size_t data_sz)
{
   FILE* binlog = store->db_file;
   size_t key_size = strlen(key) + 1;

   fseek(binlog, 0, SEEK_END);
   fwrite(&operation, sizeof(operation), 1, binlog);
   fwrite(&flags, sizeof(flags), 1, binlog);
   fwrite(&key_size, sizeof(key_size), 1, binlog);
   fwrite(&data_sz, sizeof(data_sz), 1, binlog);
   fwrite(key, sizeof(char), key_size, binlog);
   fwrite(data, sizeof(char), data_sz, binlog);

   return EKVS_OK;
}
