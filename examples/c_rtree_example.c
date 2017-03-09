#include <stdio.h>
#include <string.h>

#include "rocksdb/c.h"

int main(int argc, char **argv) {
  // Set the Table, Memtable and Comparator for the R-tree
  rocksdb_options_t *options = rocksdb_options_create();
  rocksdb_rtree_table_options_t *table_options = rocksdb_rtree_options_create();
  rocksdb_rtree_options_set_dimensions(table_options, 2);
  rocksdb_options_set_rtree_table_factory(options, table_options);
  rocksdb_options_set_memtable_skip_list_mbb(options);
  rocksdb_comparator_t* comparator = rocksdb_comparator_lowx_create();
  rocksdb_options_set_comparator(options, comparator);

  // Setup the RocksDB database as usual
  rocksdb_t *db;
  rocksdb_options_set_create_if_missing(options, 1);
  char *err = NULL;
  db = rocksdb_open(options, "/tmp/rocksdb_c_rtree_example", &err);

  // The data that you want to insert into your database
  rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
  const double augsburg_key[4] = {10.75, 11.11, 48.24, 48.50};
  const char *augsburg_value = "augsburg";
  rocksdb_put(db, writeoptions,
              (const char *)augsburg_key, 4 * sizeof(double),
              augsburg_value, strlen(augsburg_value) + 1,
              &err);
  const double alameda_key[4] = {-122.34, -122.22, 37.71, 37.80};
  const char *alameda_value = "alameda";
  rocksdb_put(db, writeoptions,
              (const char *)alameda_key, 4 * sizeof(double),
              alameda_value, strlen(alameda_value) + 1,
              &err);

  // Specify the desired bounding box on the iterator
  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  const double query[4] = {10, 11, 48, 49};
  //const double query[4] = {-150, 0, 20, 40};
  //const double query[4] = {-180, 180, -90, 90};
  rocksdb_iterator_context_t *iterator_context =
      rocksdb_create_rtree_iterator_context((const char *)query,
                                            4 * sizeof(double));
  rocksdb_readoptions_set_iterator_context(readoptions, iterator_context);
  rocksdb_iterator_t *iter = rocksdb_create_iterator(db, readoptions);

  // Iterate over the results and print the value
  for (rocksdb_iter_seek_to_first(iter);
       rocksdb_iter_valid(iter);
       rocksdb_iter_next(iter)) {
    size_t len;
    printf("%s\n", rocksdb_iter_value(iter, &len));
  }

  // Cleanup
  rocksdb_iter_destroy(iter);
  rocksdb_release_rtree_iterator_context(iterator_context);
  rocksdb_readoptions_destroy(readoptions);
  rocksdb_writeoptions_destroy(writeoptions);
  rocksdb_comparator_destroy(comparator);
  rocksdb_rtree_options_destroy(table_options);
  rocksdb_options_destroy(options);
  rocksdb_close(db);

  return 0;
}
