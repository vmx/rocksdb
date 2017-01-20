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

// TODO vmx 2017-01-18: Return status
std::string RtreeTableReader::NextLeaf(size_t* offset) {
  if (*offset >= root_block_handle_.size()) {
    return "";
  }

  Slice uint64_slice;
  char uint64_buf[sizeof(uint64_t)];

  uint64_t leaf_offset = 0;
  Status status = file_->Read(
      root_block_handle_.offset() + *offset,
      sizeof(uint64_t), &uint64_slice, uint64_buf);
  GetFixed64(&uint64_slice, &leaf_offset);
  *offset += sizeof(uint64_t);

  uint64_t leaf_size = 0;
  status = file_->Read(
      root_block_handle_.offset() + *offset,
      sizeof(uint64_t), &uint64_slice, uint64_buf);
  GetFixed64(&uint64_slice, &leaf_size);
  *offset += sizeof(uint64_t);

  Slice leaf_compressed;
  std::string slice_buf;
  slice_buf.reserve(leaf_size);
  status = file_->Read(leaf_offset, leaf_size, &leaf_compressed,
                       const_cast<char *>(slice_buf.c_str()));

  size_t ulength = 0;
  static char snappy_corrupt_msg[] =
        "Corrupted Snappy compressed block contents";
  if (!Snappy_GetUncompressedLength(leaf_compressed.data(),
                                    leaf_compressed.size(),
                                    &ulength)) {
    status = Status::Corruption(snappy_corrupt_msg);
  }
  std::unique_ptr<char[]> ubuf;
  ubuf.reset(new char[ulength]);
  if (!Snappy_Uncompress(leaf_compressed.data(),
                         leaf_compressed.size(),
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
      offset_(0) {
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
    leaf_ = table_->NextLeaf(&parent_offset_);
    offset_ = 0;
    if (!leaf_.empty()) {
      Next();
    }
  }
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
