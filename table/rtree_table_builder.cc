//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/rtree_table_builder.h"

#include <assert.h>

#include <string>
#include "db/dbformat.h"
#include "db/table_properties_collector.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

namespace {

// a utility that helps writing block content to the file
//   @block_handle the block handle this particular block.
Status WriteBlock(const Slice& block_contents, WritableFileWriter* file,
                  BlockHandle* block_handle) {
  block_handle->set_offset(file->GetFileSize());
  block_handle->set_size(block_contents.size());
  Status s = file->Append(block_contents);
  return s;
}

}  // namespace

// kRtreeTableMagicNumber was picked by running
//    echo rocksdb.table.rtree | sha1sum
// and taking the leading 64 bits.
extern const uint64_t kRtreeTableMagicNumber = 0xf930fbe5f9f3a28full;

RtreeTableBuilder::RtreeTableBuilder(
    const ImmutableCFOptions& ioptions,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, WritableFileWriter* file,
    const std::string& column_family_name)
    : ioptions_(ioptions),
      file_(file) {
  // for the rtree table, we put all the data in a big chuck.
  properties_.num_data_blocks = 1;

  properties_.column_family_id = column_family_id;
  properties_.column_family_name = column_family_name;

  for (auto& collector_factories : *int_tbl_prop_collector_factories) {
    table_properties_collectors_.emplace_back(
        collector_factories->CreateIntTblPropCollector(column_family_id));
  }
}

RtreeTableBuilder::~RtreeTableBuilder() {
}

void RtreeTableBuilder::Add(const Slice& key, const Slice& value) {
  ParsedInternalKey internal_key;
  if (!ParseInternalKey(key, &internal_key)) {
    assert(false);
    return;
  }
  if (internal_key.type == kTypeRangeDeletion) {
    status_ = Status::NotSupported("Range deletion unsupported");
    return;
  }

  // The WritableFile expects a slice. Hence we prepare a slice of the form
  // key size | key | value size | value
  std::string key_value;
  // We need to store the internal key as that's expected for further
  // operations within RocksDB
  PutFixed64(&key_value, key.size());
  key_value.append(key.data(), key.size());
  PutFixed64(&key_value, value.size());
  key_value.append(value.data(), value.size());
  file_->Append(key_value);

  properties_.num_entries++;
  properties_.raw_key_size += key.size();
  properties_.raw_value_size += value.size();

  // notify property collectors
  NotifyCollectTableCollectorsOnAdd(
      key, value, FileSize(), table_properties_collectors_,
      ioptions_.info_log);
}

Status RtreeTableBuilder::status() const { return status_; }

Status RtreeTableBuilder::Finish() {
  assert(!closed_);
  closed_ = true;

  properties_.data_size = FileSize();

  //  Write the following blocks
  //  1. [meta block: properties]
  //  2. [metaindex block]
  //  3. [footer]

  MetaIndexBuilder meta_index_builer;

  // Calculate bloom block size and index block size
  PropertyBlockBuilder property_block_builder;
  // -- Add basic properties
  property_block_builder.AddTableProperty(properties_);

  property_block_builder.Add(properties_.user_collected_properties);

  // -- Add user collected properties
  NotifyCollectTableCollectorsOnFinish(table_properties_collectors_,
                                       ioptions_.info_log,
                                       &property_block_builder);

  // -- Write property block
  BlockHandle property_block_handle;
  auto s = WriteBlock(
      property_block_builder.Finish(),
      file_,
      &property_block_handle
  );
  if (!s.ok()) {
    return s;
  }
  meta_index_builer.Add(kPropertiesBlock, property_block_handle);

  // -- write metaindex block
  BlockHandle metaindex_block_handle;
  s = WriteBlock(
      meta_index_builer.Finish(),
      file_,
      &metaindex_block_handle
  );
  if (!s.ok()) {
    return s;
  }

  // Write Footer
  // no need to write out new footer if we're using default checksum
  Footer footer(kRtreeTableMagicNumber, 0);
  footer.set_metaindex_handle(metaindex_block_handle);
  footer.set_index_handle(BlockHandle::NullBlockHandle());
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  s = file_->Append(footer_encoding);

  return s;
}

void RtreeTableBuilder::Abandon() {
  closed_ = true;
}

uint64_t RtreeTableBuilder::NumEntries() const {
  return properties_.num_entries;
}

uint64_t RtreeTableBuilder::FileSize() const {
  return file_->GetFileSize();
}

}  // namespace rocksdb
