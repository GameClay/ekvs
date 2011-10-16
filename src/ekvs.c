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

int ekvs_open(ekvs* store, const char* path, const ekvs_opts* opts)
{
   *store = malloc(sizeof(struct _ekvs_db));
   if(*store == NULL)
   {
      return EKVS_ALLOCATION_FAIL;
   }
   else
   {
      /* If a cache file is specified, open it */
      ekvs db = *store;
      FILE* dbfile = NULL;
      int file_created = 0;
      if(path != NULL)
      {
         FILE* dbfile = fopen(path, "rb+");
         if(dbfile == NULL)
         {
            file_created = 1;
            dbfile = fopen(path, "wb+");

            if(dbfile == NULL)
            {
               free(*store);
               return EKVS_FILE_FAIL;
            }
         }
      }
      db->db_file = dbfile;

      /* Read in the serialized attributes of the db */
      if(dbfile != NULL && file_created == 0)
      {
         fseek(dbfile, 0, SEEK_SET);
         fread(&db->serialized, sizeof(db->serialized), 1, dbfile);
      }
      else
      {
         /* Set the initial table size */
         if(opts == NULL || opts->initial_table_size == 0)
         {
            db->serialized.table_sz = EKVS_INITIAL_TABLE_SIZE;
         }
         else
         {
            db->serialized.table_sz = opts->initial_table_size;
         }

         /* No binlog yet */
         db->serialized.binlog_start = 0;
      }

      /* Set up the hash-table */
      db->table = malloc(sizeof(struct _ekvs_db_entry) * db->serialized.table_sz);
      if(db->table == NULL)
      {
         fclose(dbfile);
         free(*store);
         return EKVS_ALLOCATION_FAIL;
      }

      /* Read the snapshot */
      while((db->serialized.binlog_start == 0 || ftell(dbfile) < db->serialized.binlog_start) && !feof(dbfile))
      {
         
      }

      /* Read/replay the binlog */

      (*store)->last_error = EKVS_OK;
   }
   
   return (*store)->last_error;
}

void ekvs_close(ekvs store)
{
   if(store != NULL) free(store);
}

int ekvs_last_error(ekvs store)
{
   return store->last_error;
}
