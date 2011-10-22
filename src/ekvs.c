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

ekvs_malloc_ptr ekvs_malloc = malloc;
ekvs_realloc_ptr ekvs_realloc = realloc;
ekvs_free_ptr ekvs_free = free;

int ekvs_open(ekvs* store, const char* path, const ekvs_opts* opts)
{
   ekvs db;
   FILE* dbfile = NULL;
   int file_created = 0;
   /* Check for NULL store */
   if(store == NULL)
   {
      fprintf(stderr, "ekvs: NULL store parameter passed to ekvs_open.\n");
      return EKVS_FAIL;
   }

   /* Check for user-specified allocators */
   if(opts != NULL && (opts->user_malloc != NULL || opts->user_realloc != NULL || opts->user_free != NULL))
   {
      if(opts->user_malloc == NULL || opts->user_realloc == NULL || opts->user_free == NULL)
      {
         fprintf(stderr, "ekvs: Specifying one user-allocation function requires specifying all allocation functions.\n");
         return EKVS_FAIL;
      }
      ekvs_malloc = opts->user_malloc;
      ekvs_realloc = opts->user_realloc;
      ekvs_free = opts->user_free;
   }

   /* Allocate structure */
   *store = ekvs_malloc(sizeof(struct _ekvs_db));
   if(*store == NULL) return EKVS_ALLOCATION_FAIL;
   db = *store;

   /* If a cache file is specified, open it */
   db->db_fname = NULL;
   if(path != NULL)
   {
      dbfile = fopen(path, "rb+");
      if(dbfile == NULL)
      {
         file_created = 1;
         dbfile = fopen(path, "wb+");

         if(dbfile == NULL)
         {
            ekvs_free(*store);
            return EKVS_FILE_FAIL;
         }
      }
      db->db_fname = ekvs_malloc(strlen(path) + 1);
      strcpy(db->db_fname, path);
   }
   else
   {
      /* Disable binlog */
      db->binlog_enabled = 0;
   }
   db->db_file = dbfile;

   /* Assign some of the opts */
   if(opts == NULL || opts->grow_threshold == 0.0f)
   {
      db->grow_threshold = EKVS_GROW_THRESHOLD;
   }
   else
   {
      db->grow_threshold = opts->grow_threshold;
   }

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
      db->serialized.binlog_start = db->serialized.binlog_end = sizeof(struct _ekvs_db_serialized);

      /* Serialize initial DB settings */
      if(dbfile != NULL)
      {
         fwrite(&db->serialized, sizeof(db->serialized), 1, dbfile);
         fflush(dbfile);
      }
   }

   /* Set up the hash-table */
   db->table = ekvs_malloc(sizeof(struct _ekvs_db_entry*) * db->serialized.table_sz);
   if(db->table == NULL)
   {
      fclose(dbfile);
      ekvs_free(*store);
      return EKVS_ALLOCATION_FAIL;
   }

   /* Initialize table to NULL, and set population to 0 */
   memset(db->table, 0, sizeof(struct _ekvs_db_entry*) * db->serialized.table_sz);
   db->table_population = 0;

   /* Load up the table */
   if(dbfile != NULL)
   {
      struct _ekvs_db_entry entry;
      struct _ekvs_db_entry* new_entry;
      struct _ekvs_db_entry* cur_entry;
      long int binlog_start = db->serialized.binlog_start;
      long int binlog_end = db->serialized.binlog_end;
      long int filepos = ftell(dbfile);
      uint64_t hash;
      uint32_t pc, pb;
      char operation;

      entry.chain = NULL;

      /* Read the snapshot */
      while(filepos < binlog_start && !feof(dbfile))
      {
         fread(&entry.flags, sizeof(struct _ekvs_db_entry) - sizeof(struct _ekvs_db_entry*) - 1, 1, dbfile);

         /* Allocate enough space for the entry. */
         new_entry = ekvs_malloc(sizeof(struct _ekvs_db_entry) + entry.key_sz + entry.data_sz - 1);
         memcpy(new_entry, &entry, sizeof(struct _ekvs_db_entry) - 1);
         fread(new_entry->key_data, 1, entry.key_sz + entry.data_sz, dbfile);
         filepos = ftell(dbfile);

         /* Assign to table, and increment population */
         pc = pb = 0;
         hashlittle2(new_entry->key_data, entry.key_sz, &pc, &pb);
         hash = pc + (((uint64_t)pb) << 32);
         cur_entry = db->table[hash % (*store)->serialized.table_sz];
         if(cur_entry == NULL) 
         {
            db->table[hash % (*store)->serialized.table_sz] = new_entry;
         }
         else
         {
            while(cur_entry->chain != NULL)
            {
               cur_entry = cur_entry->chain;
            }
            cur_entry->chain = new_entry;
         }
         db->table_population++;
      }

      /* Read/replay the binlog */
      db->binlog_enabled = 0;
      fseek(dbfile, binlog_start, SEEK_SET);
      new_entry = ekvs_malloc(sizeof(struct _ekvs_db_entry));
      while(binlog_start != binlog_end && filepos < binlog_end)
      {
         fread(&operation, sizeof(operation), 1, dbfile);
         fread(&entry.flags, sizeof(struct _ekvs_db_entry) - sizeof(struct _ekvs_db_entry*) - 1, 1, dbfile);

         new_entry = ekvs_realloc(new_entry, sizeof(struct _ekvs_db_entry) + entry.key_sz + entry.data_sz - 1);
         memcpy(new_entry, &entry, sizeof(struct _ekvs_db_entry) - 1);
         fread(new_entry->key_data, 1, entry.key_sz + entry.data_sz, dbfile);
         filepos = ftell(dbfile);

         /* Replay */
         db->last_error = _ekvs_replay_binlog_entry(*store, operation, new_entry);
         if(db->last_error != EKVS_OK)
         {
            fprintf(stderr, "Error replaying binlog.");
            break;
         }
      }
      ekvs_free(new_entry);
      db->binlog_enabled = 1;
   }

   (*store)->last_error = EKVS_OK;
   
   return (*store)->last_error;
}

void ekvs_close(ekvs store)
{
   if(store != NULL)
   {
      uint64_t i;
      struct _ekvs_db_entry* cur_entry;
      struct _ekvs_db_entry* del_entry;
      for(i = 0; i < store->serialized.table_sz; i++)
      {
         cur_entry = store->table[i];
         while(cur_entry != NULL)
         {
            del_entry = cur_entry;
            cur_entry = cur_entry->chain;
            ekvs_free(del_entry);
         }
      }
      ekvs_free(store->db_fname);
      ekvs_free(store->table);
      fclose(store->db_file);
      ekvs_free(store);
   }
}

int ekvs_snapshot(ekvs store, const char* snapshot_to)
{
   uint64_t i;
   uint64_t table_sz = store->serialized.table_sz;
   FILE* dbfile;
   struct _ekvs_db_entry* entry;
   char* tmp_fname = NULL;
   size_t key_data_sz;
   struct _ekvs_db_serialized new_serialized;

   /* Create a temporary file */
   if(snapshot_to == NULL)
   {
      if(store->db_fname == NULL)
      {
         fprintf(stderr, "[ekvs]: db created using in-memory only mode, snapshot requires use of snapshot_to parameter.\n");
         return EKVS_FILE_FAIL;
      }
      tmp_fname = ekvs_malloc(strlen(store->db_fname) + 6);
      sprintf(tmp_fname, "%s.lock", store->db_fname);
      dbfile = fopen(tmp_fname, "wb+"); /* TODO: fopen_s? */
   }
   else
   {
      dbfile = fopen(snapshot_to, "wb+"); /* TODO: fopen_s? */
   }
   
   /* Leave room for the serialized blob, then write out the table */
   if(fseek(dbfile, sizeof(struct _ekvs_db_serialized), SEEK_SET) != 0) goto ekvs_snapshot_err;
   for(i = 0; i < table_sz; i++)
   {
      entry = store->table[i];
      while(entry != NULL)
      {
         key_data_sz = entry->key_sz + entry->data_sz;
         if(fwrite(&entry->flags, sizeof(struct _ekvs_db_entry) - sizeof(struct _ekvs_db_entry*) - 1, 1, dbfile) != 1) goto ekvs_snapshot_err;
         if(fwrite(entry->key_data, 1, key_data_sz, dbfile) != key_data_sz) goto ekvs_snapshot_err;
         entry = entry->chain;
      }
   }

   /* Now write serialization blob */
   new_serialized.table_sz = table_sz;
   new_serialized.binlog_start = new_serialized.binlog_end = ftell(dbfile);
   if(new_serialized.binlog_end == -1L) goto ekvs_snapshot_err;
   if(fseek(dbfile, 0, SEEK_SET) != 0) goto ekvs_snapshot_err;
   if(fwrite(&new_serialized, sizeof(struct _ekvs_db_serialized), 1, dbfile) != 1) goto ekvs_snapshot_err;
   memcpy(&store->serialized, &new_serialized, sizeof(struct _ekvs_db_serialized));

   /* Close temporary file, rename */
   fclose(dbfile);
   if(snapshot_to == NULL)
   {
      fclose(store->db_file);
      remove(store->db_fname);
      rename(tmp_fname, store->db_fname);
      store->db_file = fopen(store->db_fname, "rb+");
   }
   ekvs_free(tmp_fname);

   store->last_error = EKVS_OK;
   return EKVS_OK;

ekvs_snapshot_err:
   ekvs_free(tmp_fname);
   fclose(dbfile);
   remove(tmp_fname);
   store->last_error = EKVS_FILE_FAIL;
   return EKVS_FILE_FAIL;
}

int ekvs_last_error(ekvs store)
{
   return store->last_error;
}

int ekvs_grow_table(ekvs store, size_t new_sz)
{
   struct _ekvs_db_entry* entry;
   struct _ekvs_db_entry* cur_entry;
   uint64_t hash;
   uint32_t pc, pb;
   uint64_t i, old_table_sz = store->serialized.table_sz;
   struct _ekvs_db_entry** new_table = ekvs_malloc(sizeof(struct _ekvs_db_entry*) * new_sz);

   if(new_table == NULL) return EKVS_ALLOCATION_FAIL;
   memset(new_table, 0, sizeof(struct _ekvs_db_entry*) * new_sz);

   for(i = 0; i < old_table_sz; i++)
   {
      entry = store->table[i];
      while(entry != NULL)
      {
         pc = pb = 0;
         hashlittle2(entry->key_data, entry->key_sz, &pc, &pb);
         hash = pc + (((uint64_t)pb) << 32);
         
         cur_entry = new_table[hash % new_sz];
         if(cur_entry != NULL)
         {
            while(cur_entry->chain != NULL)
            {
               cur_entry = cur_entry->chain;
            }
            cur_entry->chain = entry;
         }
         else
         {
            new_table[hash % new_sz] = entry;
         }
         cur_entry = entry;
         entry = entry->chain;
         cur_entry->chain = NULL;
      }
   }

   /* Update and serialize */
   store->serialized.table_sz = new_sz;
   if(store->db_file != NULL)
   {
      if(fseek(store->db_file, 0, SEEK_SET) != 0) goto ekvs_grow_table_err;
      if(fwrite(&store->serialized, sizeof(store->serialized), 1, store->db_file) != 1) goto ekvs_grow_table_err;
      if(fflush(store->db_file) != 0) goto ekvs_grow_table_err;
   }

   ekvs_free(store->table);
   store->table = new_table;
   return EKVS_OK;

ekvs_grow_table_err:
   ekvs_free(new_table);
   store->serialized.table_sz = old_table_sz;
   return EKVS_FILE_FAIL;
}

int ekvs_set_ex(ekvs store, const char* key, const void* data, size_t data_sz, char flags)
{
   uint64_t hash, old_hash;
   uint32_t pc = 0, pb = 0;
   struct _ekvs_db_entry* new_entry = NULL;
   size_t key_sz = strlen(key);
   
   hashlittle2(key, key_sz, &pc, &pb);
   hash = pc + (((uint64_t)pb) << 32);

   new_entry = _ekvs_insert(store, hash, key, data, key_sz, data_sz, flags);
   if(new_entry == NULL)
   {
      store->last_error = EKVS_ALLOCATION_FAIL;
   }
   else if(store->binlog_enabled)
   {
      store->last_error = _ekvs_binlog(store, EKVS_BINLOG_SET, 0 /* flags */, key, data, data_sz);
   }
   else
   {
      store->last_error = EKVS_OK;
   }

   return store->last_error;
}

int ekvs_get(ekvs store, const char* key, const void** data, size_t* data_sz)
{
   struct _ekvs_db_entry* entry = NULL;
   uint64_t hash;
   uint32_t pc = 0, pb = 0;
   size_t key_sz = 0;

   if(store == NULL)
   {
      fprintf(stderr, "ekvs: NULL store parameter passed to ekvs_get.\n");
      return EKVS_FAIL;
   }

   if(key == NULL)
   {
      fprintf(stderr, "ekvs: NULL key parameter passed to ekvs_get.\n");
      return EKVS_FAIL;
   }

   key_sz = strlen(key);
   hashlittle2(key, key_sz, &pc, &pb);
   hash = pc + (((uint64_t)pb) << 32);
   entry = _ekvs_retrieve(store, hash, key, key_sz);
   if(entry == NULL)
   {
      *data = NULL;
      *data_sz = 0;
      store->last_error = EKVS_NO_KEY;
   }
   else
   {
      *data = &entry->key_data[entry->key_sz];
      *data_sz = entry->data_sz;
      store->last_error = EKVS_OK;
   }
   
   return store->last_error;
}

int ekvs_del(ekvs store, const char* key)
{
   struct _ekvs_db_entry* entry = NULL;
   struct _ekvs_db_entry* prev_entry = NULL;
   uint64_t hash;
   uint32_t pc = 0, pb = 0;
   size_t key_sz = strlen(key);
   hashlittle2(key, key_sz, &pc, &pb);
   hash = pc + (((uint64_t)pb) << 32);
   entry = store->table[hash % store->serialized.table_sz];
   if(entry == NULL)
   {
      store->last_error = EKVS_NO_KEY;
   }
   else
   {
      /* Traverse chain */
      while(key_sz != entry->key_sz || memcmp(key, entry->key_data, key_sz) != 0)
      {
         prev_entry = entry;
         entry = entry->chain;
      }

      /* Unlink or remove from table, then deallocate */
      if(prev_entry != NULL)
      {
         prev_entry->chain = entry->chain;
      }
      else
      {
         store->table[hash % store->serialized.table_sz] = NULL;
      }
      ekvs_free(entry);

      store->table_population--;

      if(store->binlog_enabled)
      {
         store->last_error = _ekvs_binlog(store, EKVS_BINLOG_DEL, 0 /* flags */, key, NULL, 0);
      }
      else
      {
         store->last_error = EKVS_OK;
      }
   }

   return store->last_error;
}

/********************** Helpers and debugging aids **********************/

struct _ekvs_db_entry* _ekvs_insert(ekvs store, uint64_t hash, const char* key, const void* data,
   size_t key_sz, size_t data_sz, char flags)
{
   struct _ekvs_db_entry* cur_entry = NULL;
   struct _ekvs_db_entry* new_entry = NULL;
   int test_grow = 0;

   cur_entry = store->table[hash % store->serialized.table_sz];
   if(cur_entry != NULL)
   {
      if(key_sz != cur_entry->key_sz || memcmp(key, cur_entry->key_data, key_sz) != 0)
      {
         /* Collision */
         new_entry = ekvs_malloc(sizeof(struct _ekvs_db_entry) + key_sz + data_sz - 1);
         test_grow = 1;
      }
      else
      {
         new_entry = ekvs_realloc(cur_entry, sizeof(struct _ekvs_db_entry) + key_sz + data_sz - 1);
         cur_entry = NULL;
      }
   }
   else
   {
      new_entry = ekvs_malloc(sizeof(struct _ekvs_db_entry) + key_sz + data_sz - 1);
      test_grow = 1;
   }

   if(new_entry != NULL)
   {
      new_entry->chain = NULL;
      new_entry->flags = 0;
      new_entry->key_sz = key_sz;
      new_entry->data_sz = data_sz;
      memcpy(new_entry->key_data, key, key_sz);
      memcpy(&new_entry->key_data[key_sz], data, data_sz);

      if(cur_entry != NULL)
      {
         while(cur_entry->chain != NULL)
         {
            cur_entry = cur_entry->chain;
         }
         cur_entry->chain = new_entry;
      }
      else
      {
         store->table[hash % store->serialized.table_sz] = new_entry;
      }
   }

   /* Grow table if needed */
   if((flags & ekvs_set_no_grow == 0) && test_grow > 0)
   {
      float table_saturation;
      store->table_population++;
      table_saturation = (float)store->table_population / (float)store->serialized.table_sz;
      while(table_saturation > store->grow_threshold)
      {
         /* TODO: Double table size reasonable? */
         ekvs_grow_table(store, store->serialized.table_sz * 2);
         table_saturation = (float)store->table_population / (float)store->serialized.table_sz;
      }
   }

   return new_entry;
}

struct _ekvs_db_entry* _ekvs_retrieve(ekvs store, uint64_t hash, const char* key, size_t key_sz)
{
   struct _ekvs_db_entry* entry = store->table[hash % store->serialized.table_sz];

   if(entry != NULL)
   {
      while(key_sz != entry->key_sz || memcmp(key, entry->key_data, key_sz) != 0)
      {
         entry = entry->chain;
         if(entry == NULL) break;
      }
   }
   
   return entry;
}
