#include <iostream>

#include "db/memtable.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "table/rtree_table_util.h"


int main() {
  // Set the Table, Memtable and Comparator for the R-tree
  rocksdb::Options options;
  rocksdb::RtreeTableOptions table_options;
  table_options.dimensions = 2;
  options.table_factory.reset(rocksdb::NewRtreeTableFactory(table_options));
  options.memtable_factory.reset(new rocksdb::SkipListMbbFactory);
  options.comparator = rocksdb::LowxComparator();

  // Setup the RocksDB database as usual
  rocksdb::DB* raw_db;
  std::unique_ptr<rocksdb::DB> db;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/rocksdb_rtree_example", &raw_db);
  db.reset(raw_db);

  // The data that you want to insert into your database
  std::vector<double> augsburg_key = {10.75, 11.11, 48.24, 48.50};
  rocksdb::Slice augsburg_slice = rocksdb::Slice(reinterpret_cast<const char*>(
      augsburg_key.data()), sizeof(augsburg_key[0]) * augsburg_key.size());
  status = raw_db->Put(rocksdb::WriteOptions(), augsburg_slice, "augsburg");
  std::vector<double> alameda_key = {-122.34, -122.22, 37.71, 37.80};
  rocksdb::Slice alameda_slice = rocksdb::Slice(reinterpret_cast<const char*>(
      alameda_key.data()), sizeof(alameda_key[0]) * alameda_key.size());
  status = db->Put(rocksdb::WriteOptions(), alameda_slice, "alameda");

  // Specify the desired bounding box on the iterator
  rocksdb::ReadOptions read_options;
  rocksdb::RtreeTableIteratorContext iterator_context;
  std::vector<double> query = {10, 11, 48, 49};
  //std::vector<double> query = {-150, 0, 20, 40};
  //std::vector<double> query = {-180, 180, -90, 90};
  iterator_context.query_mbb = std::string(
      reinterpret_cast<const char*>(query.data()),
      sizeof(query[0]) * query.size());
  read_options.iterator_context = &iterator_context;
  std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

  // Iterate over the results and print the value
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->value().ToString() << std::endl;
  }

  return 0;
}
