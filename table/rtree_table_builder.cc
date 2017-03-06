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

const std::string RtreeTablePropertyNames::kDimensions =
    "rocksdb.rtree.table.dimensions";


RtreeLeafBuilder::RtreeLeafBuilder(uint8_t dimensions)
    : buffer_(""),
      finished_(false),
      dimensions_(dimensions) {}

void RtreeLeafBuilder::Reset() {
  buffer_.clear();
  finished_ = false;
}

// The data within the block is:
// key size | key | value size | value
void RtreeLeafBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);

  // Decode the key into its parts to calculate the enclosing bounding box
  // of it
  // TODO vmx 2017-03-03: Get the types from the table options
  std::vector<Variant::Type> types = {Variant::kDouble, Variant::kDouble};
  // Deserializing also works with internal keys as there is only additional data *after* the
  // actual key.
  std::vector<std::pair<Variant, Variant>> deserialized = RtreeUtil::DeserializeKey(types, key);

  parent_key_ = RtreeUtil::EnclosingMbb(parent_key_, deserialized);

  // We need to store the internal key as that's expected for further
  // operations within RocksDB
  PutLengthPrefixedSlice(&buffer_, key);
  PutLengthPrefixedSlice(&buffer_, value);
}

Slice RtreeLeafBuilder::Finish() {
  finished_ = true;
  return Slice(buffer_);
}

std::string RtreeLeafBuilder::ParentKey() {
  return RtreeUtil::EncodeKey(parent_key_);
}


RtreeInnerBuilder::RtreeInnerBuilder()
    : buffer_(""),
      finished_(false) {}

void RtreeInnerBuilder::Reset() {
  buffer_.clear();
  finished_ = false;
}

// The data within the block is a list of enclosing bounding boxed together
// with the handles:
// key size | internal key containing the bounding box | data offset | data size | ...
void RtreeInnerBuilder::Add(const Slice& key,
                            const BlockHandle& block_handle) {
  assert(!finished_);

  PutLengthPrefixedSlice(&buffer_, key);
  PutFixed64(&buffer_, block_handle.offset());
  PutFixed64(&buffer_, block_handle.size());
}

Slice RtreeInnerBuilder::Finish() {
  finished_ = true;
  return Slice(buffer_);
}




// kRtreeTableMagicNumber was picked by running
//    echo rocksdb.table.rtree | sha1sum
// and taking the leading 64 bits.
extern const uint64_t kRtreeTableMagicNumber = 0xf930fbe5f9f3a28full;

RtreeTableBuilder::RtreeTableBuilder(
    const ImmutableCFOptions& ioptions,
    const RtreeTableOptions& table_options,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, WritableFileWriter* file,
    const std::string& column_family_name)
    : ioptions_(ioptions),
      file_(file),
      table_options_(table_options),
      data_block_(table_options_.dimensions) {
  properties_.num_data_blocks = 0;
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

  // We don't use a `FlushBlockPolicy` here as it expects a block in the
  // BlockBulder format. Our blocks are different. Also there's currently
  // only a single implementation that is based on the size of the block,
  // we can do this without all the abstraction.
  if (data_block_.Size() >= table_options_.block_size) {
    Flush();
  }

  data_block_.Add(key, value);

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

  // The last block is probably not flushed ot the file yet
  Status status = Flush();
  if (!status.ok()) {
    return status;
  }
  data_block_.Reset();

  // The data size is the key-values without the index structure
  properties_.data_size = FileSize();

  // Store the number of dimensions of the table, so that the rtree table
  // reader can make use of it
  properties_.user_collected_properties[
      RtreeTablePropertyNames::kDimensions].assign(
          reinterpret_cast<const char*>(&table_options_.dimensions),
          sizeof(table_options_.dimensions));

  // Store the index tree after the data blocks
  BlockHandle parents_block_handle;
  status = BuildTree(parents_block_.Finish(), file_, &parents_block_handle);
  parents_block_.Reset();
  if (!status.ok()) {
    return status;
  }

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
  status = WriteBlock(
      property_block_builder.Finish(),
      file_,
      &property_block_handle
  );
  if (!status.ok()) {
    return status;
  }
  meta_index_builer.Add(kPropertiesBlock, property_block_handle);

  // -- write metaindex block
  BlockHandle metaindex_block_handle;
  status = WriteBlock(
      meta_index_builer.Finish(),
      file_,
      &metaindex_block_handle
  );
  if (!status.ok()) {
    return status;
  }

  // Write Footer
  // no need to write out new footer if we're using default checksum
  Footer footer(kRtreeTableMagicNumber, 0);
  footer.set_metaindex_handle(metaindex_block_handle);
  // We use the index handle as entry point to the data structure
  footer.set_index_handle(parents_block_handle);
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  status = file_->Append(footer_encoding);

  return status;
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

Status RtreeTableBuilder::Flush() {
  if (!data_block_.empty()) {
    BlockHandle data_block_handle;

    std::string compressed;
    Slice data = data_block_.Finish();
    Snappy_Compress(CompressionOptions(),
                    data.data(),
                    data.size(),
                    &compressed);

    Status status = WriteBlock(compressed, file_, &data_block_handle);
    if (!status.ok()) {
      return status;
    }
    parents_block_.Add(Slice(data_block_.ParentKey()), data_block_handle);
    data_block_.Reset();
    properties_.num_data_blocks++;
  }
  return Status::OK();
}

// Writes a slice to disk. If it should be compressed, then the already
// compressed data needs to be passed into this method.
//   @block_handle the block handle this particular block.
Status RtreeTableBuilder::WriteBlock(const Slice& block_contents, WritableFileWriter* file,
                                     BlockHandle* block_handle) {
  block_handle->set_offset(file->GetFileSize());
  block_handle->set_size(block_contents.size());
  Status s = file->Append(block_contents);
  return s;
}

Status RtreeTableBuilder::WriteBlockCompressed(const Slice& block_contents,
                                               WritableFileWriter* file,
                                               BlockHandle* block_handle) {
  std::string compressed;
  Snappy_Compress(CompressionOptions(),
                  block_contents.data(),
                  block_contents.size(),
                  &compressed);
  return WriteBlock(Slice(compressed), file_, block_handle);
}

// NOTE vmx 2017-01-24: Build the tree bottom up. It could be done in a
// less memory-hungy way, but it should be good for now.
Status RtreeTableBuilder::BuildTree(const Slice& block_contents,
                                    WritableFileWriter* file,
                                    BlockHandle* block_handle) {
  std::string parents;
  // The key that just got read
  std::vector<std::pair<Variant, Variant>> key;
  // Create a non const version of the slice
  Slice contents = Slice(block_contents);
  // The offset before the last compression
  const char* prev_offset = contents.data();
  Status status;
  // The current accumulated block size
  size_t data_size = 0;
  // Have a temproary string we can use as a storage for a Slice
  std::string tmp_contents;
  // The key that will be used by the parent node to point to its children
  std::vector<std::pair<Variant, Variant>> parent_key;

  // Iterate through the whole block and write it in chunks (compressed)
  // to disk. Each chunk will be a node a parent will point to.
  while(contents.size() > 0) {
    // The key is an `InternalKey`. This means that the actual key (user key)
    // is first and then some addition data appended. This means we can read
    // the user key directly.
    //key = reinterpret_cast<const double*>(contents.data());
    // TODO vmx 2017-03-03: Get the types from the table options
    std::vector<Variant::Type> types = {Variant::kDouble, Variant::kDouble};
    // TODO vmx 2017-03-06: Here the key gets deserialized. A possible
    // optimization is to not having it serialized before flushing it to disk
    Slice key_slice;
    GetLengthPrefixedSlice(&contents, &key_slice);
    key = RtreeUtil::DeserializeKey(types, key_slice);
    parent_key = RtreeUtil::EnclosingMbb(parent_key, key);

    const size_t handle_size = 2 * sizeof(uint64_t);
    contents.remove_prefix(handle_size);

    data_size = contents.data() - prev_offset;
    // Write block if a certain threshold is reached (4KB by default) or
    // when there's is some left-over
    if (data_size >= table_options_.block_size || contents.size() == 0) {
      status = WriteBlockCompressed(Slice(prev_offset, data_size),
                                    file_,
                                    block_handle);
      if (!status.ok()) {
        return status;
      }

      // There is only one parent node, i.e. we've reached the root node
      if (contents.size() == 0 && parents.empty()) {
        return Status::OK();
      }

      // Add the key and the handle to the parents' level
      std::string encoded_key = RtreeUtil::EncodeKey(parent_key);
      PutLengthPrefixedSlice(&parents, Slice(encoded_key));
      PutFixed64(&parents, block_handle->offset());
      PutFixed64(&parents, block_handle->size());

      parent_key.clear();
      prev_offset = contents.data();
    }

    // All nodes of the current level got processed, now store the parent
    // level if there is one
    if (contents.size() == 0) {
      tmp_contents = std::move(parents);
      parents.clear();
      contents = Slice(tmp_contents);
      prev_offset = contents.data();
      data_size = 0;
    }
  }

  // There was no contents
  *block_handle = BlockHandle::NullBlockHandle();

  return status;
}

}  // namespace rocksdb
