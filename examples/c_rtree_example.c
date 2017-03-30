#include <stdio.h>
#include <string.h>

#include "rocksdb/c.h"

int main(int argc, char **argv) {
  // Set the Table, Memtable and Comparator for the R-tree
  rocksdb_options_t *options = rocksdb_options_create();
  rocksdb_rtree_table_options_t *table_options = rocksdb_rtree_options_create();
  rocksdb_options_set_rtree_table_factory(options, table_options);
  rocksdb_options_set_memtable_skip_list_mbb(options);
  rocksdb_comparator_t* comparator = rocksdb_comparator_lowx_create();
  rocksdb_options_set_comparator(options, comparator);

  // Setup the RocksDB database as usual
  rocksdb_t *db;
  rocksdb_options_set_create_if_missing(options, 1);
  char *err = NULL;
  db = rocksdb_open(options, "/tmp/rocksdb_c_rtree_example", &err);
  rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();

  // The data that you want to insert into your database
  rocksdb_rtree_key_t* augsburg_key = rocksdb_rtree_key_create();
  rocksdb_rtree_key_push_double(augsburg_key, 10.75);
  rocksdb_rtree_key_push_double(augsburg_key, 11.11);
  rocksdb_rtree_key_push_double(augsburg_key, 48.24);
  rocksdb_rtree_key_push_double(augsburg_key, 48.50);
  const char *augsburg_cityhall = "Rathausplatz";
  rocksdb_rtree_key_push_string(augsburg_key,
                                augsburg_cityhall,
                                strlen(augsburg_cityhall));
  rocksdb_rtree_key_push_string(augsburg_key,
                                augsburg_cityhall,
                                strlen(augsburg_cityhall));
  const char* augsburg_value = "augsburg";
  size_t augsburg_key_size;
  const char* augsburg_key_data = rocksdb_rtree_key_data(augsburg_key,
                                                         &augsburg_key_size);
  rocksdb_put(db,
              writeoptions,
              augsburg_key_data,
              augsburg_key_size,
              augsburg_value,
              strlen(augsburg_value) + 1,
              &err);
  rocksdb_rtree_key_destroy(augsburg_key);

  rocksdb_rtree_key_t* alameda_key = rocksdb_rtree_key_create();
  rocksdb_rtree_key_push_double(alameda_key, -122.34);
  rocksdb_rtree_key_push_double(alameda_key, -122.22);
  rocksdb_rtree_key_push_double(alameda_key, 37.71);
  rocksdb_rtree_key_push_double(alameda_key, 37.80);
  const char *alameda_cityhall = "Santa Clara Avenue";
  rocksdb_rtree_key_push_string(alameda_key,
                                alameda_cityhall,
                                strlen(alameda_cityhall));
  rocksdb_rtree_key_push_string(alameda_key,
                                alameda_cityhall,
                                strlen(alameda_cityhall));
  const char* alameda_value = "alameda";
  size_t alameda_key_size;
  const char* alameda_key_data = rocksdb_rtree_key_data(alameda_key,
                                                        &alameda_key_size);
  rocksdb_put(db,
              writeoptions,
              alameda_key_data,
              alameda_key_size,
              alameda_value,
              strlen(alameda_value) + 1,
              &err);
  rocksdb_rtree_key_destroy(alameda_key);

  // Specify the desired bounding box on the iterator
  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  const double query_coords[4] = {10.0, 11.0, 48.0, 49.0};
  //const double query_coords[4] = {-150.0, 0.0, 20.0, 40.0};
  //const double query_coords[4] = {-180.0, 180.0, -90.0, 90.0};
  rocksdb_rtree_key_t* query = rocksdb_rtree_key_create();
  rocksdb_rtree_key_push_double(query, query_coords[0]);
  rocksdb_rtree_key_push_double(query, query_coords[1]);
  rocksdb_rtree_key_push_double(query, query_coords[2]);
  rocksdb_rtree_key_push_double(query, query_coords[3]);
  rocksdb_rtree_key_push_string(query, "A", strlen("A"));
  rocksdb_rtree_key_push_string(query, "Z", strlen("Z"));

  rocksdb_iterator_context_t *iterator_context =
      rocksdb_create_rtree_iterator_context(query);
  rocksdb_rtree_key_destroy(query);
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
