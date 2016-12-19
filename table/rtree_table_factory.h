// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <memory>
#include <string>
#include <stdint.h>

#include "rocksdb/status.h"          // for Status
#include "rocksdb/table.h"           // for RtreeTableOptions, TableFactory

namespace rocksdb {

struct EnvOptions;

using std::unique_ptr;
class Status;
class RandomAccessFile;
class WritableFile;
class Table;
class TableBuilder;

class RtreeTableFactory : public TableFactory {
 public:
  ~RtreeTableFactory() {}

  explicit RtreeTableFactory(
      const RtreeTableOptions& _table_options = RtreeTableOptions())
      : table_options_(_table_options) {}

  const char* Name() const override { return "RtreeTable"; }
  Status NewTableReader(const TableReaderOptions& table_reader_options,
                        unique_ptr<RandomAccessFileReader>&& file,
                        uint64_t file_size,
                        unique_ptr<TableReader>* table,
                        bool prefetch_index_and_filter_in_cache) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const override;

  std::string GetPrintableTableOptions() const override;

  const RtreeTableOptions& table_options() const;

  static const char kValueTypeSeqId0 = char(0xFF);

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override {
    return Status::OK();
  }

  void* GetOptions() override { return &table_options_; }

 private:
  RtreeTableOptions table_options_;
};

}  // namespace rocksdb
