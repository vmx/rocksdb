// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <unordered_map>
#include <memory>
#include <vector>
#include <string>
#include <stdint.h>

#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "table/table_reader.h"
#include "table/rtree_table_factory.h"
#include "util/arena.h"
#include "util/cf_options.h"
#include "util/dynamic_bloom.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

class Block;
struct BlockContents;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;
class TableReader;
class InternalKeyComparator;
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
  static Status Open(const ImmutableCFOptions& ioptions,
                     const EnvOptions& env_options,
                     const InternalKeyComparator& internal_comparator,
                     unique_ptr<RandomAccessFileReader>&& file,
                     uint64_t file_size, unique_ptr<TableReader>* table);

  InternalIterator* NewIterator(const ReadOptions&, Arena* arena = nullptr,
                                bool skip_filters = false) override;

  void Prepare(const Slice& target) override;

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
                   uint64_t file_size,
                   const TableProperties* table_properties);
  virtual ~RtreeTableReader();

  Status MmapDataIfNeeded();

 private:
  const InternalKeyComparator internal_comparator_;
  // represents plain table's current status.
  Status status_;

  // data_start_offset_ and data_end_offset_ defines the range of the
  // sst file that stores data.
  const uint32_t data_start_offset_ = 0;

  RtreeTableReaderFileInfo file_info_;
  Arena arena_;

  const ImmutableCFOptions& ioptions_;
  uint64_t file_size_;
  std::shared_ptr<const TableProperties> table_properties_;

  friend class RtreeTableIterator;

  // Read the key and value at `offset` to parameters for keys, the and
  // `seekable`.
  // On success, `offset` will be updated as the offset for the next key.
  // `parsed_key` will be key in parsed format.
  // if `internal_key` is not empty, it will be filled with key with slice
  // format.
  // if `seekable` is not null, it will return whether we can directly read
  // data using this offset.
  Status Next(uint32_t* offset, ParsedInternalKey* parsed_key,
              Slice* internal_key, Slice* value) const;

  // No copying allowed
  explicit RtreeTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
