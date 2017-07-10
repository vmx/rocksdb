// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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
#include "util/rtree.h"


using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_rtree_example";

std::string serialize_key(std::string keypath, uint64_t iid, double value) {
  std::string key;
  PutVarint32(&key, static_cast<uint32_t>(keypath.size()));
  key.append(keypath);
  // The R-tree stores boxes, hence duplicate the input values
  key.append(reinterpret_cast<const char*>(&iid), sizeof(uint64_t));
  key.append(reinterpret_cast<const char*>(&value), sizeof(double));
  key.append(reinterpret_cast<const char*>(&value), sizeof(double));
  key.append(reinterpret_cast<const char*>(&value), sizeof(double));
  key.append(reinterpret_cast<const char*>(&value), sizeof(double));
  return key;
}

std::string serialize_query(std::string keypath, uint64_t iid_min,
                            uint64_t iid_max, double value_min,
                            double value_max) {
  std::string key;
  PutVarint32(&key, static_cast<uint32_t>(keypath.size()));
  key.append(keypath);
  key.append(reinterpret_cast<const char*>(&iid_min), sizeof(uint64_t));
  key.append(reinterpret_cast<const char*>(&iid_max), sizeof(uint64_t));
  key.append(reinterpret_cast<const char*>(&value_min), sizeof(double));
  key.append(reinterpret_cast<const char*>(&value_max), sizeof(double));
  key.append(reinterpret_cast<const char*>(&value_min), sizeof(double));
  key.append(reinterpret_cast<const char*>(&value_max), sizeof(double));
  return key;
}

uint64_t decode_value(std::string& value) {
  return *reinterpret_cast<const uint64_t*>(value.data());
}

struct Key {
  std::string keypath;
  Mbb mbb;
};


Key deserialize_key(Slice key_slice) {
  Key key;
  //Slice key_slice = Slice(key_string);
  Slice keypath_slice;
  GetLengthPrefixedSlice(&key_slice, &keypath_slice);
  key.keypath = keypath_slice.ToString();
  key.mbb = ReadKeyMbb(key_slice);
  return key;
}

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

    // keypaths are the same, compare the value. The previous
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

  NoiseComparator cmp;
  options.comparator = &cmp;

  BlockBasedTableOptions block_based_options;

  block_based_options.index_type = BlockBasedTableOptions::kRtreeSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
  options.memtable_factory.reset(new rocksdb::SkipListMbbFactory);

  const std::string keypath = "somekeypath";

  Status s;
  s = DB::Open(options, kDBPath, &db);

  // Failed to open, probably it doesn't exist yet. Try to create it and
  // insert data
  if (!s.ok()) {
    options.create_if_missing = true;
    s = DB::Open(options, kDBPath, &db);
    assert(s.ok());

    std::string key1 = serialize_key(keypath, 516, 22.214);

    // Put key-value
    s = db->Put(WriteOptions(), key1, "");
    assert(s.ok());

    std::string key2 = serialize_key(keypath, 1124, 4.1432);
    s = db->Put(WriteOptions(), key2, "");
    assert(s.ok());
  }

  // Query the R-tree

  std::string value;

  // Specify the desired bounding box on the iterator
  rocksdb::ReadOptions read_options;
  rocksdb::RtreeIteratorContext iterator_context;

  // This scope is needed so that the unique pointer of the iterator runs
  // out of scope and cleans up things correctly
  {
    iterator_context.query_mbb =
        serialize_query(keypath, 0, 1000000, 2.1, 10.8);
    read_options.iterator_context = &iterator_context;
    std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

    std::cout << "query 1" << std::endl;
    // Iterate over the results and print the value
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      Key key = deserialize_key(it->key());
      std::cout << key.keypath << " " << key.mbb << std::endl;
    }
  }

  {
    iterator_context.query_mbb =
        serialize_query(keypath, 0, 1000000, 12.4, 30.3);
    read_options.iterator_context = &iterator_context;
    std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

    std::cout << "query 2" << std::endl;
    // Iterate over the results and print the value
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      Key key = deserialize_key(it->key());
      std::cout << key.keypath << " " << key.mbb << std::endl;
    }
  }

  {
    iterator_context.query_mbb =
        serialize_query(keypath, 0, 1000000, 0.0, 100.0);
    read_options.iterator_context = &iterator_context;
    std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

    std::cout << "query 3" << std::endl;
    // Iterate over the results and print the value
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      Key key = deserialize_key(it->key());
      std::cout << key.keypath << " " << key.mbb << std::endl;
    }
  }

  delete db;

  return 0;
}
