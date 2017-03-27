// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/rtree_table_factory.h"

#include <memory>
#include <stdint.h>
#include "table/rtree_table_builder.h"
#include "table/rtree_table_reader.h"
#include "table/table_builder.h"

namespace rocksdb {

Status RtreeTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    unique_ptr<RandomAccessFileReader>&& file,
    uint64_t file_size,
    unique_ptr<TableReader>* table,
    bool prefetch_index_and_filter_in_cache) const {
  std::unique_ptr<RtreeTableReader> new_reader(new RtreeTableReader(
      table_reader_options.ioptions,
      std::move(file),
      table_reader_options.env_options,
      table_reader_options.internal_comparator,
      file_size));

  Status status = new_reader->status();
  if (status.ok()) {
    *table = std::move(new_reader);
  }

  return status;
}

TableBuilder* RtreeTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  return new RtreeTableBuilder(
      table_builder_options.ioptions, table_options_,
      table_builder_options.int_tbl_prop_collector_factories, column_family_id,
      file, table_builder_options.column_family_name);
}

std::string RtreeTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  block_size: %" ROCKSDB_PRIszt "\n",
           table_options_.block_size);
  ret.append(buffer);

  return ret;
}

const RtreeTableOptions& RtreeTableFactory::table_options() const {
  return table_options_;
}

extern TableFactory* NewRtreeTableFactory(const RtreeTableOptions& options) {
  return new RtreeTableFactory(options);
}

}  // namespace rocksdb
