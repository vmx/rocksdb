// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rocksdb/c.h"

struct slice_t {
  const char *data;
  size_t size;
};

const char DBPath[] = "/tmp/rocksdb_rtree_example_c";

int main(int argc, char **argv) {
  rocksdb_t *db;
  rocksdb_options_t *options = rocksdb_options_create();
  // create the DB if it's not already present
  rocksdb_options_set_create_if_missing(options, 1);

  // Set R-tree index
  rocksdb_block_based_table_options_t *block_based_options =
      rocksdb_block_based_options_create();
  rocksdb_block_based_options_set_index_type(
      block_based_options, rocksdb_block_based_table_index_type_rtree_search);
  // Use Mbb filtering MemTable
  rocksdb_options_set_memtable_skip_list_mbb_rep(options);

  // open DB
  char *err = NULL;
  db = rocksdb_open(options, DBPath, &err);
  assert(!err);

  const char *keypath = "somekeypath";

  // Put key-value
  rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();

  const char key[] = "key";
  const char *value = "value";
  rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value) + 1,
              &err);
  assert(!err);
  // Get value
  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  size_t len;
  char *returned_value =
      rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
  assert(!err);
  assert(strcmp(returned_value, "value") == 0);
  free(returned_value);

  const char *query_mbb = "nottherealquerymbbyet";
  rocksdb_iterator_context_t *iterator_context =
      rocksdb_create_rtree_iterator_context(query_mbb, strlen(query_mbb));
  rocksdb_readoptions_set_iterator_context(readoptions, iterator_context);

  rocksdb_iterator_t *iter = rocksdb_create_iterator(db, readoptions);
  printf("query 1\n");
  // Iterate over the results and print the value
  for (rocksdb_iter_seek_to_first(iter); rocksdb_iter_valid(iter);
       rocksdb_iter_next(iter)) {
    size_t len;
    printf("%s\n", rocksdb_iter_value(iter, &len));
  }
  rocksdb_iter_destroy(iter);
  rocksdb_release_rtree_iterator_context(iterator_context);

  rocksdb_close(db);

  // cleanup
  rocksdb_writeoptions_destroy(writeoptions);
  rocksdb_readoptions_destroy(readoptions);
  rocksdb_options_destroy(options);
  rocksdb_block_based_options_destroy(block_based_options);
  rocksdb_close(db);

  return 0;
}
