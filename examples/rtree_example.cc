#include <iostream>

#include "db/memtable.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "table/rtree_table_util.h"


int main() {
  // Set the Table, Memtable and Comparator for the R-tree
  rocksdb::Options options;
  rocksdb::RtreeTableOptions table_options;
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
  rocksdb::RtreeKeyBuilder augsburg_key;
  augsburg_key.push_double(10.75);
  augsburg_key.push_double(11.11);
  augsburg_key.push_double(48.24);
  augsburg_key.push_double(48.50);
  augsburg_key.push_string("Rathausplatz");
  augsburg_key.push_string("Rathausplatz");
  rocksdb::Slice augsburg_slice = rocksdb::Slice(reinterpret_cast<const char*>(
      augsburg_key.data()), sizeof(uint8_t) * augsburg_key.size());
  status = raw_db->Put(rocksdb::WriteOptions(), augsburg_slice, "augsburg");

  rocksdb::RtreeKeyBuilder alameda_key;
  alameda_key.push_double(-122.34);
  alameda_key.push_double(-122.22);
  alameda_key.push_double(37.71);
  alameda_key.push_double(37.80);
  alameda_key.push_string("Santa Clara Avenue");
  alameda_key.push_string("Santa Clara Avenue");
  rocksdb::Slice alameda_slice = rocksdb::Slice(reinterpret_cast<const char*>(
      alameda_key.data()), sizeof(uint8_t) * alameda_key.size());
  status = db->Put(rocksdb::WriteOptions(), alameda_slice, "alameda");

  // Specify the desired bounding box on the iterator
  rocksdb::ReadOptions read_options;
  rocksdb::RtreeTableIteratorContext iterator_context;
  std::vector<double> query_coords = {10, 11, 48, 49};
  //std::vector<double> query_coords = {-150, 0, 20, 40};
  //std::vector<double> query_coords = {-180, 180, -90, 90};
  rocksdb::RtreeKeyBuilder query;
  query.push_double(query_coords[0]);
  query.push_double(query_coords[1]);
  query.push_double(query_coords[2]);
  query.push_double(query_coords[3]);
  query.push_string("A");
  query.push_string("Z");

  iterator_context.query_mbb = std::string(query.data(), query.size());
  read_options.iterator_context = &iterator_context;
  std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

  // Iterate over the results and print the value
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->value().ToString() << std::endl;
  }

  return 0;
}
