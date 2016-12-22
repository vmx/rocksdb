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
  uint64_t offset_;
  std::string key_;
  std::string value_;
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
                                   uint64_t file_size,
                                   const TableProperties* table_properties)
    : internal_comparator_(icomparator),
      file_(std::move(file)),
      file_size_(file_size),
      table_properties_(table_properties) {}

RtreeTableReader::~RtreeTableReader() {
}

Status RtreeTableReader::Open(const ImmutableCFOptions& ioptions,
                              const EnvOptions& env_options,
                              const InternalKeyComparator& internal_comparator,
                              unique_ptr<RandomAccessFileReader>&& file,
                              uint64_t file_size,
                              unique_ptr<TableReader>* table_reader) {
  TableProperties* props = nullptr;
  auto s = ReadTableProperties(file.get(), file_size, kRtreeTableMagicNumber,
                               ioptions, &props);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<RtreeTableReader> new_reader(new RtreeTableReader(
      ioptions, std::move(file), env_options, internal_comparator,
      file_size, props));

  *table_reader = std::move(new_reader);
  return s;
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

std::tuple<Status, std::string, std::string> RtreeTableReader::NextKeyValue(uint64_t* offset)
    const {
  if (*offset >= table_properties_->data_size) {
    return std::make_tuple(
        Status::Corruption("There is no further key-value pair"), "", "");
  }

  std::string key = ReadFixedSlice(offset);
  std::string value = ReadFixedSlice(offset);

  return std::make_tuple(Status::OK(), key, value);
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
      // When an interator is initialized, a call to `Valid()` must return
      // false (according to the tests). Hence set the `offset_` to a value
      // bigger than the data size.
      offset_(table_->DataSize() + 1) {
}

RtreeTableIterator::~RtreeTableIterator() {
}

bool RtreeTableIterator::Valid() const {
  return offset_ <= table_->DataSize();
}

void RtreeTableIterator::SeekToFirst() {
  offset_ = 0;
  Next();
}

void RtreeTableIterator::SeekToLast() {
  assert(false);
  status_ = Status::NotSupported("SeekToLast() is not supported in RtreeTable");
}

void RtreeTableIterator::Seek(const Slice& target) {
  offset_ = 0;
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
  if (offset_ < table_->DataSize()) {
    Slice tmp_slice;

    Status status;
    std::string key;
    std::string value;
    ParsedInternalKey parsed_key;

    std::tie(status, key, value) = table_->NextKeyValue(&offset_);
    if (!status.ok()) {
      offset_ = table_->DataSize();
    }
    //ParseInternalKey(Slice(key), &parsed_key);
    //key_ = parsed_key.user_key;
    key_ = key;
    value_ = value;
  } else {
    // A key is considered invalid if offset is greater than the data size,
    // hence increase it
    offset_++;
  }
}

void RtreeTableIterator::Prev() {
  assert(false);
}

Slice RtreeTableIterator::key() const {
  assert(Valid());
  return Slice(key_);
}

Slice RtreeTableIterator::value() const {
  assert(Valid());
  return Slice(value_);
}

Status RtreeTableIterator::status() const {
  return status_;
}

}  // namespace rocksdb
