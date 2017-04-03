// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run this test... Skipping...\n");
  return 0;
}
#else

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <gflags/gflags.h>
#include <vector>
#include <string>
#include <map>

#include "db/memtable.h"
#include "table/rtree_table_builder.h"
#include "table/rtree_table_reader.h"
#include "table/rtree_table_factory.h"
#include "util/testharness.h"

using GFLAGS::ParseCommandLineFlags;
using GFLAGS::SetUsageMessage;

DEFINE_string(file_dir, "", "Directory where the files will be created"
    " for benchmark. Added for using tmpfs.");

namespace rocksdb {

class RtreeReaderTest : public testing::Test {
 public:
  using testing::Test::SetUp;

  RtreeReaderTest() {
    env = options.env;
    env_options = EnvOptions(options);
  }

  std::string fname;
  Options options;
  Env* env;
  EnvOptions env_options;
};

TEST_F(RtreeReaderTest, QueryDoubles) {
  fname = test::TmpDir() + "/CuckooReader_QueryDoubles";

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env->NewWritableFile(fname, &writable_file, env_options));
  unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), env_options));

  const ImmutableCFOptions ioptions(options);
  RtreeTableOptions table_options;
  options.table_factory.reset(NewRtreeTableFactory(table_options));
  options.memtable_factory.reset(new SkipListMbbFactory);
  options.comparator = LowxComparator();

  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      int_tbl_prop_collector_factories;

  RtreeTableBuilder builder(
      ioptions, table_options,
      &int_tbl_prop_collector_factories,
      0 /* column_family_id */,
      file_writer.get(),
      kDefaultColumnFamilyName);

    ASSERT_OK(builder.status());

    rocksdb::RtreeKeyBuilder alameda_key;
    alameda_key.push_double(-122.34);
    alameda_key.push_double(-122.22);
    alameda_key.push_double(37.71);
    alameda_key.push_double(37.80);
    rocksdb::Slice alameda_slice = rocksdb::Slice(reinterpret_cast<const char*>(
        alameda_key.data()), sizeof(uint8_t) * alameda_key.size());

    rocksdb::RtreeKeyBuilder augsburg_key;
    augsburg_key.push_double(10.75);
    augsburg_key.push_double(11.11);
    augsburg_key.push_double(48.24);
    augsburg_key.push_double(48.50);
    rocksdb::Slice augsburg_slice = rocksdb::Slice(reinterpret_cast<const char*>(
        augsburg_key.data()), sizeof(uint8_t) * augsburg_key.size());

    InternalKey ikey;
    ikey.Set(alameda_slice, 0, ValueType::kTypeValue);
    builder.Add(ikey.Encode(), Slice("alameda"));
    ikey.Set(augsburg_slice, 0, ValueType::kTypeValue);
    builder.Add(ikey.Encode(), Slice("augsburg"));

    ASSERT_OK(builder.Finish());
    ASSERT_EQ(2, builder.NumEntries());
    uint64_t file_size = builder.FileSize();
    ASSERT_OK(file_writer->Close());


    std::unique_ptr<RandomAccessFile> read_file;
    ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));
    unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(read_file)));
    const InternalKeyComparator icmp(options.comparator);
    RtreeTableReader reader(ioptions,
                            std::move(file_reader),
                            env_options,
                            icmp,
                            file_size);
    ASSERT_OK(reader.status());

    {
      std::vector<double> query_coords = {10, 11, 48, 49};
      rocksdb::RtreeKeyBuilder query;
      query.push_double(query_coords[0]);
      query.push_double(query_coords[1]);
      query.push_double(query_coords[2]);
      query.push_double(query_coords[3]);

      ReadOptions read_options;
      RtreeTableIteratorContext iterator_context;
      iterator_context.query_mbb = std::string(query.data(), query.size());
      read_options.iterator_context = &iterator_context;
      InternalIterator* it = reader.NewIterator(read_options, nullptr);
      ASSERT_OK(it->status());

      it->SeekToFirst();
      ASSERT_TRUE(it->Valid());
      ASSERT_TRUE(Slice("augsburg") == it->value());
      it->Next();
      ASSERT_TRUE(!it->Valid());
    }

    {
      std::vector<double> query_coords = {10, 11, 0.0, 0.1};
      rocksdb::RtreeKeyBuilder query;
      query.push_double(query_coords[0]);
      query.push_double(query_coords[1]);
      query.push_double(query_coords[2]);
      query.push_double(query_coords[3]);

      ReadOptions read_options;
      RtreeTableIteratorContext iterator_context;
      iterator_context.query_mbb = std::string(query.data(), query.size());
      read_options.iterator_context = &iterator_context;
      InternalIterator* it = reader.NewIterator(read_options, nullptr);
      ASSERT_OK(it->status());

      it->SeekToFirst();
      // No match
      ASSERT_TRUE(!it->Valid());
    }

    {
      std::vector<double> query_coords = {-180, 180, -90, 90};
      rocksdb::RtreeKeyBuilder query;
      query.push_double(query_coords[0]);
      query.push_double(query_coords[1]);
      query.push_double(query_coords[2]);
      query.push_double(query_coords[3]);

      ReadOptions read_options;
      RtreeTableIteratorContext iterator_context;
      iterator_context.query_mbb = std::string(query.data(), query.size());
      read_options.iterator_context = &iterator_context;
      InternalIterator* it = reader.NewIterator(read_options, nullptr);
      ASSERT_OK(it->status());

      it->SeekToFirst();
      ASSERT_TRUE(it->Valid());
      ASSERT_TRUE(Slice("alameda") == it->value());
      it->Next();

      ASSERT_TRUE(it->Valid());
      ASSERT_TRUE(Slice("augsburg") == it->value());
      it->Next();
      ASSERT_TRUE(!it->Valid());
    }

}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

#endif  // GFLAGS.
