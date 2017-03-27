// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <algorithm>                  // for move
#include <unordered_map>
#include <memory>
#include <vector>
#include <string>
#include <stdint.h>


#include "db/dbformat.h"              // for InternalKeyComparator, ParsedIn...
#include "rocksdb/slice.h"            // for Slice
#include "rocksdb/status.h"           // for Status
#include "rocksdb/env.h"              // for EnvOptions
#include "table/format.h"             // for BlockHandle
#include "table/table_reader.h"       // for TableReader
#include "util/arena.h"               // for Arena
#include "util/file_reader_writer.h"  // for RandomAccessFileReade
#include "util/cf_options.h"

namespace rocksdb {

struct ReadOptions;
class GetContext;
class InternalIterator;

using std::unique_ptr;
using std::unordered_map;
using std::vector;

struct RtreeTableReaderFileInfo {
  bool is_mmap_mode;
  Slice file_data;
  uint32_t data_end_offset;
  unique_ptr<RandomAccessFileReader> file;

  RtreeTableReaderFileInfo(unique_ptr<RandomAccessFileReader>&& _file,
                           const EnvOptions& storage_options,
                           uint32_t _data_size_offset)
      : is_mmap_mode(storage_options.use_mmap_reads),
        data_end_offset(_data_size_offset),
        file(std::move(_file)) {}
};

// Based on following output file format shown in plain_table_factory.h
// When opening the output file, IndexedTableReader creates a hash table
// from key prefixes to offset of the output file. IndexedTable will decide
// whether it points to the data offset of the first key with the key prefix
// or the offset of it. If there are too many keys share this prefix, it will
// create a binary search-able index from the suffix to offset on disk.
//
// The implementation of IndexedTableReader requires output file is mmaped
class RtreeTableReader: public TableReader {
 public:
  InternalIterator* NewIterator(const ReadOptions&, Arena* arena = nullptr,
                                bool skip_filters = false) override;

  void Prepare(const Slice& target) override;

  Status status() const { return status_; }

  Status Get(const ReadOptions&, const Slice& key, GetContext* get_context,
             bool skip_filters = false) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override;

  void SetupForCompaction() override;

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return table_properties_;
  }

  virtual size_t ApproximateMemoryUsage() const override {
    return arena_.MemoryAllocatedBytes();
  }

  RtreeTableReader(const ImmutableCFOptions& ioptions,
                   unique_ptr<RandomAccessFileReader>&& file,
                   const EnvOptions& env_options,
                   const InternalKeyComparator& internal_comparator,
                   uint64_t file_size);
  virtual ~RtreeTableReader();

  Status MmapDataIfNeeded();

  uint64_t DataSize() const {
    return table_properties_->data_size;
  }

  BlockHandle* RootBlockHandle() {
    return &root_block_handle_;
  }

 private:
  const InternalKeyComparator internal_comparator_;
  // represents plain table's current status.
  Status status_;

  // data_start_offset_ and data_end_offset_ defines the range of the
  // sst file that stores data.
  const uint32_t data_start_offset_ = 0;

  Arena arena_;

  unique_ptr<RandomAccessFileReader> file_;
  uint64_t file_size_;
  //TableProperties table_properties_;
  std::shared_ptr<const TableProperties> table_properties_;
  BlockHandle root_block_handle_;

  friend class RtreeTableIterator;

  // Read some compressed data from file
  std::string ReadCompressed(size_t offset, size_t size) const;
  std::string ReadCompressed(BlockHandle* block_handle) const;

  // No copying allowed
  explicit RtreeTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};
}  // namespace rocksdb
