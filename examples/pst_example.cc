// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <cstdio>
#include <string>
#include <iostream>
#include <sstream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "util/coding.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_pst_example";

std::string serialize_key(std::string keypath, double value, uint64_t iid) {
  std::string key;
  PutVarint32(&key, static_cast<uint32_t>(keypath.size()));
  key.append(keypath);
  key.append(reinterpret_cast<const char*>(&value), sizeof(double));
  key.append(reinterpret_cast<const char*>(&iid), sizeof(uint64_t));
  return key;
}

std::string serialize_query(std::string keypath, double min, double max) {
  std::string key;
  PutVarint32(&key, static_cast<uint32_t>(keypath.size()));
  key.append(keypath);
  key.append(reinterpret_cast<const char*>(&min), sizeof(double));
  key.append(reinterpret_cast<const char*>(&max), sizeof(uint64_t));
  return key;
}

uint64_t decode_value(std::string& value) {
  return *reinterpret_cast<const uint64_t*>(value.data());
}

//std::pair<const double, const uint64_t> deserialize_key(std::string& key) {
//  const double value = *reinterpret_cast<const double*>(key.data());
//  const uint64_t iid = *reinterpret_cast<const uint64_t*>(
//      key.data() + sizeof(double));
//  return std::make_pair(value, iid);
//}

// A comparator that interprets keys from Noise. It's a length prefixed
// string first (the keypath) followed by the value and the Internal Id.
class NoiseComparator : public rocksdb::Comparator {
 public:
  const char* Name() const {
    return "rocksdb.NoiseComparator";
  }

  int Compare(const rocksdb::Slice& const_a, const rocksdb::Slice& const_b) const {
    Slice slice_a = Slice(const_a);
    Slice slice_b = Slice(const_b);
    Slice keypath_a;
    Slice keypath_b;
    GetLengthPrefixedSlice(&slice_a, &keypath_a);
    GetLengthPrefixedSlice(&slice_b, &keypath_b);

    int keypath_compare = keypath_a.compare(keypath_b);
    if (keypath_compare != 0) {
      return keypath_compare;
    }

    // keypaths are the same, compre the value. The previous
    // `GetLengthPrefixedSlice()` did advance the Slice already, hence a call
    // to `.data()` can directly be used.
    const double* value_a = reinterpret_cast<const double*>(slice_a.data());
    const double* value_b = reinterpret_cast<const double*>(slice_b.data());

    if (*value_a < *value_b) {
        return -1;
    } else if (*value_a > *value_b) {
      return 1;
    } else {
      return 0;
    }
  }

  void FindShortestSeparator(std::string* start,
      const rocksdb::Slice& limit) const {
    return;
  }

  void FindShortSuccessor(std::string* key) const  {
    return;
  }
};


int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  // create the DB if it's not already present
  options.create_if_missing = true;

  NoiseComparator cmp;
  options.comparator = &cmp;
  
  BlockBasedTableOptions block_based_options;
  //block_based_options.index_type = BlockBasedTableOptions::kBinarySearch;
  //block_based_options.index_type = BlockBasedTableOptions::kHashSearch;
  //options.prefix_extractor.reset(NewNoopTransform());
  block_based_options.index_type = BlockBasedTableOptions::kPstSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  const std::string keypath = "somekeypath";
  std::string key1 = serialize_key(keypath, 22.214, 516);
  //std::pair<const double, const uint64_t> deserialize = deserialize_key(key1);
  //std::cout << deserialize.first << " " << deserialize.second << std::endl;
  
  // Put key-value
  s = db->Put(WriteOptions(), key1, "");
  assert(s.ok());

  std::string key2 = serialize_key(keypath, 4.1432, 1124);
  s = db->Put(WriteOptions(), key2, "");
  assert(s.ok());

  std::string value;
  std::string query1 = serialize_query(keypath, 2.1, 10.8);
  s = db->Get(ReadOptions(), query1, &value);
  std::cout << "result1: " << decode_value(value) << std::endl;
  //assert(s.IsNotFound());

  std::string query2 = serialize_query(keypath, 12.4, 30.3);
  s = db->Get(ReadOptions(), query2, &value);
  std::cout << "result2: " << decode_value(value) << std::endl;
  //assert(s.IsNotFound());

  std::string query3 = serialize_query(keypath, 0.0, 100.0);
  s = db->Get(ReadOptions(), query3, &value);
  std::cout << "result2: " << decode_value(value) << std::endl;
  //assert(s.IsNotFound());


  //std::string value;
  //// get value
  //s = db->Get(ReadOptions(), "key1", &value);
  //assert(s.ok());
  //assert(value == "value");
  // 
  //// atomically apply a set of updates
  //{
  //  WriteBatch batch;
  //  batch.Delete("key1");
  //  batch.Put("key2", value);
  //  s = db->Write(WriteOptions(), &batch);
  //}
  // 
  //s = db->Get(ReadOptions(), "key1", &value);
  //assert(s.IsNotFound());
  // 
  //db->Get(ReadOptions(), "key2", &value);
  //assert(value == "value");

  delete db;

  return 0;
}
