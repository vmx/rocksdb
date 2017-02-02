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

class BlockHandle;
class WritableFile;
class TableBuilder;

// This is a simplified version of the `BlockBuilder`
class RtreeLeafBuilder {
 public:
  //RtreeLeafBuilder(const RtreeBlockBuilder&) = delete;
  //RtreeLeafBuilder& operator=(const RtreeBlockBuilder&) = delete;

  RtreeLeafBuilder(uint8_t dimensions);
  //~RtreeLeafBuilder();

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns the key that should be used for the parent node
  std::string ParentKey();

  // The current size of the buffer
  size_t Size() const {
    return buffer_.size();
  };

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

 private:
  std::string buffer_;
  // Has Finish() been called?
  bool finished_;

  // The key that will be used by the parent node to point to its children
  //const double* parent_key_;
  std::vector<double> parent_key_;
  uint8_t dimensions_;

  // No copying allowed
  RtreeLeafBuilder(const RtreeLeafBuilder&) = delete;
  void operator=(const RtreeLeafBuilder&) = delete;
};


// This is a simplified version of the `BlockBuilder`
class RtreeInnerBuilder {
 public:
  //RtreeInnerBuilder(const RtreeBlockBuilder&) = delete;
  //RtreeInnerBuilder& operator=(const RtreeBlockBuilder&) = delete;

  RtreeInnerBuilder();
  //~RtreeInnerBuilder();

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key,
           const BlockHandle& block_handle);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // The current size of the buffer
  size_t Size() const {
    return buffer_.size();
  };

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

 private:
  std::string buffer_;
  // Has Finish() been called?
  bool finished_;

  // No copying allowed
  RtreeInnerBuilder(const RtreeInnerBuilder&) = delete;
  void operator=(const RtreeInnerBuilder&) = delete;
};


class RtreeTableBuilder: public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish(). The output file
  // will be part of level specified by 'level'.  A value of -1 means
  // that the caller does not know which level the output file will reside.
  RtreeTableBuilder(
      const ImmutableCFOptions& ioptions,
      const RtreeTableOptions& table_options,
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
  Status Flush();

  Status WriteBlock(const Slice& block_contents, WritableFileWriter* file,
                    BlockHandle* block_handle);

  Status WriteBlockCompressed(const Slice& block_contents,
                              WritableFileWriter* file,
                              BlockHandle* block_handle);

  // Sets the block_handle to the root node
  Status BuildTree(const Slice& block_contents, WritableFileWriter* file,
                   BlockHandle* block_handle);

  Arena arena_;
  const ImmutableCFOptions& ioptions_;
  std::vector<std::unique_ptr<IntTblPropCollector>>
      table_properties_collectors_;

  WritableFileWriter* file_;
  Status status_;
  TableProperties properties_;

  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  const RtreeTableOptions table_options_;
  RtreeLeafBuilder data_block_;
  RtreeInnerBuilder parents_block_;

  // No copying allowed
  RtreeTableBuilder(const RtreeTableBuilder&) = delete;
  void operator=(const RtreeTableBuilder&) = delete;
};

}  // namespace rocksdb
