//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <stdint.h>
#include <string>
#include <vector>
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "table/table_builder.h"
#include "util/arena.h"

namespace rocksdb {

class BlockBuilder;
class BlockHandle;
class WritableFile;
class TableBuilder;

class RtreeTableBuilder: public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish(). The output file
  // will be part of level specified by 'level'.  A value of -1 means
  // that the caller does not know which level the output file will reside.
  RtreeTableBuilder(
      const ImmutableCFOptions& ioptions,
      const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
          int_tbl_prop_collector_factories,
      uint32_t column_family_id, WritableFileWriter* file,
      const std::string& column_family_name);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~RtreeTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  TableProperties GetTableProperties() const override { return properties_; }

 private:
  Arena arena_;
  const ImmutableCFOptions& ioptions_;
  std::vector<std::unique_ptr<IntTblPropCollector>>
      table_properties_collectors_;

  WritableFileWriter* file_;
  Status status_;
  TableProperties properties_;

  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  // No copying allowed
  RtreeTableBuilder(const RtreeTableBuilder&) = delete;
  void operator=(const RtreeTableBuilder&) = delete;
};

}  // namespace rocksdb
