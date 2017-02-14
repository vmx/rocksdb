// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "db/dbformat.h"

#include "rocksdb/slice.h"     // for Slice
#include "rocksdb/status.h"    // for Status
#include "rocksdb/table_properties.h"

#include "table/rtree_table_reader.h"
#include "table/rtree_table_util.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"

#include "util/arena.h"

namespace rocksdb {

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
  const double* key_;
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

  // Reset the internal state of the iterator
  void Reset();

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
  auto& user_props = table_properties->user_collected_properties;

  auto dimensions = user_props.find(
      RtreeTablePropertyNames::kDimensions);
  if (dimensions == user_props.end()) {
    status_ = Status::Corruption("Number of dimensions not found");
    return;
  }
  dimensions_ = *reinterpret_cast<const uint8_t*>(dimensions->second.data());
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
  assert(false);
  return Status::NotSupported("Get() is not supported in RtreeTable");
}

uint64_t RtreeTableReader::ApproximateOffsetOf(const Slice& key) {
  return 0;
}

size_t RtreeTableReader::KeySize() const {
  return dimensions_ * 2 * sizeof(double) + kMinInternalKeySize;
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

void RtreeTableIterator::Reset() {
  parent_offset_ = 0;
  offset_ = 0;
  leaf_.clear();
  blocks_to_root_.clear();
  target_.clear();
}

bool RtreeTableIterator::Valid() const {
  return !leaf_.empty() && offset_ <= leaf_.size();
}

void RtreeTableIterator::SeekToFirst() {
  Reset();
  Next();
}

void RtreeTableIterator::SeekToLast() {
  assert(false);
  status_ = Status::NotSupported("SeekToLast() is not supported in RtreeTable");
}

void RtreeTableIterator::Seek(const Slice& target) {
  Reset();
  target_ = std::string(target.data(), target.size());
  Next();
}

void RtreeTableIterator::SeekForPrev(const Slice& target) {
  assert(false);
  status_ =
      Status::NotSupported("SeekForPrev() is not supported in RtreeTable");
}

void RtreeTableIterator::Next() {
  // We are within a leaf node
  while (!leaf_.empty() && offset_ < leaf_.size()) {
    // The key is an `InternalKey`. This means that the actual key (user key)
    // is first and then some addition data appended. This means we can read
    // the user key directly.
    key_ = reinterpret_cast<const double*>(leaf_.data() + offset_);
    const size_t key_size = table_->KeySize();
    offset_ = reinterpret_cast<const char*>(key_) - leaf_.data() + key_size;
    value_ = GetLengthPrefixedSlice(leaf_.data() + offset_);
    offset_ = value_.data() - leaf_.data() + value_.size();

    const bool intersect = RtreeUtil::IntersectMbb(key_,
                                                   target_,
                                                   table_->dimensions_);
    // We have a matching key-value pair if the bounding boxes intersect
    // each other
    if (intersect) {
      return;
    }
  }

  // The current leaf node was fully read, advance to the next one

  // If there is no next leaf, it will return an empty string and hence
  // `Valid()` will be false
  leaf_ = NextLeaf();
  offset_ = 0;
  if (!leaf_.empty()) {
    Next();
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
    // The key is an `InternalKey`. This means that the actual key (user key)
    // is first and then some addition data appended. This means we can read
    // the user key directly.
    const double* key = reinterpret_cast<const double*>(inner->data());
    // Advance the slice as we read the key
    inner->remove_prefix(table_->KeySize());
    const bool intersect = RtreeUtil::IntersectMbb(key,
                                                   target_,
                                                   table_->dimensions_);
    // If the key doesn't intersect with the search window (the bounding box
    // given by `Seek()`, try the next one.
    // If no target is given, just iterate over everything
    if (!intersect) {
      // The key is followed by a handle, which is stored as a 64-bit offset
      // and 64-bit size.
      inner->remove_prefix(2 * sizeof(uint64_t));
    } else {
      uint64_t offset = 0;
      uint64_t size = 0;
      GetFixed64(inner, &offset);
      GetFixed64(inner, &size);
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
  return Slice(reinterpret_cast<const char*>(key_), table_->KeySize());
}

Slice RtreeTableIterator::value() const {
  assert(Valid());
  return value_;
}

Status RtreeTableIterator::status() const {
  return status_;
}

}  // namespace rocksdb
