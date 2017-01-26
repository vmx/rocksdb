// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "db/dbformat.h"

#include "rocksdb/slice.h"     // for Slice
#include "rocksdb/status.h"    // for Status
#include "rocksdb/table_properties.h"

#include "table/rtree_table_reader.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"

#include "util/arena.h"

namespace rocksdb {

const uint8_t kMaxVarint32Length = 6u;

// Iterator to iterate IndexedTable
class RtreeTableIterator : public InternalIterator {
 public:
  explicit RtreeTableIterator(RtreeTableReader* table);
  ~RtreeTableIterator();

  bool Valid() const override;

  void SeekToFirst() override;

  void SeekToLast() override;

  void Seek(const Slice& target) override;

  void SeekForPrev(const Slice& target) override;

  void Next() override;

  void Prev() override;

  Slice key() const override;

  Slice value() const override;

  Status status() const override;

 private:
  RtreeTableReader* table_;
  // The offset off within the inner node (there currently is one only)
  uint64_t parent_offset_;
  // An uncompressed leaf node
  std::string leaf_;
  // The offset within a uncompressed leaf
  uint64_t offset_;
  Slice key_;
  Slice value_;
  Status status_;
  // A call to`Seek()` specifie the bounding box we want to query on, hence
  // store that value within the iterator
  // NOTE vmx 2017-01-23: It would be nicer if this could be Slice, but
  // for a reason I know, this won't work as the memory will get changed
  // somehow.
  std::string target_;

  // All the blocks (uncompressed) from the current position up to the
  // root node
  std::vector<std::pair<Slice, std::string>> blocks_to_root_;

  // Get the next leaf node
  std::string NextLeaf();

  // Read the handle of the next child block of an inner node
  BlockHandle GetNextChildHandle(Slice* inner);

  // No copying allowed
  RtreeTableIterator(const RtreeTableIterator&) = delete;
  void operator=(const Iterator&) = delete;
};

extern const uint64_t kRtreeTableMagicNumber;
RtreeTableReader::RtreeTableReader(const ImmutableCFOptions& ioptions,
                                   unique_ptr<RandomAccessFileReader>&& file,
                                   const EnvOptions& storage_options,
                                   const InternalKeyComparator& icomparator,
                                   uint64_t file_size)
    : internal_comparator_(icomparator),
      file_(std::move(file)),
      file_size_(file_size),
      table_properties_(nullptr) {
  Footer footer;
  status_ = ReadFooterFromFile(file_.get(),
                               file_size,
                               &footer,
                               kRtreeTableMagicNumber);
  if (!status_.ok()) {
    return;
  }
  root_block_handle_ = footer.index_handle();

  TableProperties* table_properties = nullptr;
  status_ = ReadTableProperties(file_.get(),
                                file_size,
                                kRtreeTableMagicNumber,
                                ioptions,
                                &table_properties);
  if (!status_.ok()) {
    return;
  }
  table_properties_.reset(table_properties);
}

RtreeTableReader::~RtreeTableReader() {
}

void RtreeTableReader::SetupForCompaction() {
}

InternalIterator* RtreeTableReader::NewIterator(const ReadOptions& options,
                                                Arena* arena,
                                                bool skip_filters) {
  if (arena == nullptr) {
    return new RtreeTableIterator(this);
  } else {
    auto mem = arena->AllocateAligned(sizeof(RtreeTableIterator));
    return new (mem) RtreeTableIterator(this);
  }
}

std::string RtreeTableReader::ReadFixedSlice(uint64_t* offset) const {
  Slice uint64_slice;
  char uint64_buf[sizeof(uint64_t)];

  uint64_t slice_size = 0;
  Status status = file_->Read(*offset, sizeof(uint64_t), &uint64_slice,
                              uint64_buf);
  GetFixed64(&uint64_slice, &slice_size);
  *offset += sizeof(uint64_t);

  Slice slice;
  std::string slice_buf;
  slice_buf.reserve(slice_size);
  status = file_->Read(*offset, slice_size, &slice,
                       const_cast<char *>(slice_buf.c_str()));
  *offset += slice_size;
  return std::string(slice.data(), slice.size());
}

std::string RtreeTableReader::ReadCompressed(BlockHandle* block_handle) const {
  return ReadCompressed(block_handle->offset(), block_handle->size());
}

std::string RtreeTableReader::ReadCompressed(size_t offset, size_t size)
    const {
  Status status;
  Slice compressed;
  std::string slice_buf;
  slice_buf.reserve(size);
  status = file_->Read(offset, size, &compressed,
                       const_cast<char *>(slice_buf.c_str()));

  size_t ulength = 0;
  static char snappy_corrupt_msg[] =
        "Corrupted Snappy compressed block contents";
  if (!Snappy_GetUncompressedLength(compressed.data(),
                                    compressed.size(),
                                    &ulength)) {
    status = Status::Corruption(snappy_corrupt_msg);
  }
  std::unique_ptr<char[]> ubuf;
  ubuf.reset(new char[ulength]);
  if (!Snappy_Uncompress(compressed.data(),
                         compressed.size(),
                         ubuf.get())) {
    status = Status::Corruption(snappy_corrupt_msg);
  }
  return std::string(ubuf.get(), ulength);
}

void RtreeTableReader::Prepare(const Slice& target) {
}

Status RtreeTableReader::Get(const ReadOptions& ro, const Slice& target,
                             GetContext* get_context, bool skip_filters) {
  InternalIterator* iter = new RtreeTableIterator(this);
  iter->Seek(target);
  if (!iter->status().ok()) {
    return iter->status();
  }

  // The key found by `Seek()` might be bigger than the target, hence check
  // for equality
  ParsedInternalKey parsed_key;
  ParseInternalKey(iter->key(), &parsed_key);
  if (parsed_key.user_key.compare(ExtractUserKey(target)) == 0) {
    get_context->SaveValue(parsed_key, iter->value());
  }
  return Status::OK();
}

uint64_t RtreeTableReader::ApproximateOffsetOf(const Slice& key) {
  return 0;
}

RtreeTableIterator::RtreeTableIterator(RtreeTableReader* table)
    : table_(table),
      parent_offset_(0),
      leaf_(""),
      offset_(0),
      target_(""),
      blocks_to_root_(std::vector<std::pair<Slice, std::string>>()) {
}

RtreeTableIterator::~RtreeTableIterator() {
}

bool RtreeTableIterator::Valid() const {
  return !leaf_.empty() && offset_ <= leaf_.size();
}

void RtreeTableIterator::SeekToFirst() {
  // TODO vmx 2017-01-20: Add a `reset()` method which resets the offsets
  // and buffers
  parent_offset_ = 0;
  offset_ = 0;
  leaf_.clear();
  blocks_to_root_.clear();

  target_ = "";

  Next();
}

void RtreeTableIterator::SeekToLast() {
  assert(false);
  status_ = Status::NotSupported("SeekToLast() is not supported in RtreeTable");
}

void RtreeTableIterator::Seek(const Slice& target) {
  // TODO vmx 2017-01-20: Add a `reset()` method which resets the offsets
  // and buffers
  parent_offset_ = 0;
  offset_ = 0;
  leaf_.clear();
  blocks_to_root_.clear();

  target_ = std::string(target.data(), target.size());
  for (Next(); status_.ok() && Valid(); Next()) {
    if (table_->internal_comparator_.Compare(key(), target) >= 0) {
      break;
    }
  }
}

void RtreeTableIterator::SeekForPrev(const Slice& target) {
  assert(false);
  status_ =
      Status::NotSupported("SeekForPrev() is not supported in RtreeTable");
}

void RtreeTableIterator::Next() {
  if (!leaf_.empty() && offset_ < leaf_.size()) {
    key_ = GetLengthPrefixedSlice(leaf_.data() + offset_);
    offset_ = key_.data() - leaf_.data() + key_.size();
    value_ = GetLengthPrefixedSlice(leaf_.data() + offset_);
    offset_ = value_.data() - leaf_.data() + value_.size();
  } else {
    // If there is no next leaf, it will return an empty string and hence
    // `Valid()` will be false
    leaf_ = NextLeaf();
    offset_ = 0;
    if (!leaf_.empty()) {
      Next();
    }
  }
}

std::string RtreeTableIterator::NextLeaf() {
  if (blocks_to_root_.empty()) {
    BlockHandle* root_handle = table_->RootBlockHandle();
    std::string block = table_->ReadCompressed(root_handle);
    // Move the `block` so the Slice points to the correct memory
    blocks_to_root_.push_back(std::make_pair(Slice(block), std::move(block)));
  }

  Slice* inner = &blocks_to_root_.back().first;
  BlockHandle child_handle = GetNextChildHandle(inner);

  // There wan't any child that could posibly contain the key, hence move
  // on with the parent node and try the sibling
  if (child_handle.IsNull()) {
    blocks_to_root_.pop_back();
    // We've gone through all children and haven't found any match
    if (blocks_to_root_.empty()) {
      return "";
    } else {
      // NOTE vmx 2017-01-25: This probably shouldn't be a recursive call, but
      // a loop
      return NextLeaf();
    }
  } else {
    std::string child = table_->ReadCompressed(&child_handle);
    // The `data_size` is also the offset where the inner nodes start
    uint64_t data_size = table_->table_properties_->data_size;

    // If the child node is an inner node, keep recursing
    if (child_handle.offset() >= data_size) {
      blocks_to_root_.push_back(std::make_pair(Slice(child),
                                               std::move(child)));
      return NextLeaf();
    } else {
      return child;
    }
  }
}

BlockHandle RtreeTableIterator::GetNextChildHandle(Slice* inner) {
  while (inner->size() > 0) {
    Slice key;
    GetLengthPrefixedSlice(inner, &key);
    // If the key we are looking for (`target_`) is bigger than the last key
    // of the block, then this block can't be a match and we try the next one.
    // If no target is given, just iterate over everything
    if (!target_.empty() && table_->internal_comparator_.Compare(key, target_) < 0) {
      // The key is followed by a handle, which is stored as a 64-bit offset
      // and 64-bit size.
      inner->remove_prefix(2 * sizeof(uint64_t));
    } else {
      uint64_t offset;
      uint64_t size;
      GetFixed64(inner, &offset);
      GetFixed64(inner, &size);
      //return table_->ReadCompressed(offset, size);
      return BlockHandle(offset, size);
    }
  }
  return BlockHandle::NullBlockHandle();
}

void RtreeTableIterator::Prev() {
  assert(false);
}

Slice RtreeTableIterator::key() const {
  assert(Valid());
  return key_;
}

Slice RtreeTableIterator::value() const {
  assert(Valid());
  return value_;
}

Status RtreeTableIterator::status() const {
  return status_;
}

}  // namespace rocksdb
