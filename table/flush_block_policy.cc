//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/options.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/slice.h"
#include "table/block_builder.h"
#include "util/coding.h"

#include <cassert>

namespace rocksdb {

// Flush block by size
class FlushBlockBySizePolicy : public FlushBlockPolicy {
 public:
  // @params block_size:           Approximate size of user data packed per
  //                               block.
  // @params block_size_deviation: This is used to close a block before it
  //                               reaches the configured
  FlushBlockBySizePolicy(const uint64_t block_size,
                         const uint64_t block_size_deviation,
                         const BlockBuilder& data_block_builder)
      : block_size_(block_size),
        block_size_deviation_limit_(
            ((block_size * (100 - block_size_deviation)) + 99) / 100),
        data_block_builder_(data_block_builder) {}

  virtual bool Update(const Slice& key,
                      const Slice& value) override {
    // it makes no sense to flush when the data block is empty
    if (data_block_builder_.empty()) {
      return false;
    }

    auto curr_size = data_block_builder_.CurrentSizeEstimate();

    // Do flush if one of the below two conditions is true:
    // 1) if the current estimated size already exceeds the block size,
    // 2) block_size_deviation is set and the estimated size after appending
    // the kv will exceed the block size and the current size is under the
    // the deviation.
    return curr_size >= block_size_ || BlockAlmostFull(key, value);
  }

 private:
  bool BlockAlmostFull(const Slice& key, const Slice& value) const {
    if (block_size_deviation_limit_ == 0) {
      return false;
    }

    const auto curr_size = data_block_builder_.CurrentSizeEstimate();
    const auto estimated_size_after =
      data_block_builder_.EstimateSizeAfterKV(key, value);

    return estimated_size_after > block_size_ &&
           curr_size > block_size_deviation_limit_;
  }

  const uint64_t block_size_;
  const uint64_t block_size_deviation_limit_;
  const BlockBuilder& data_block_builder_;
};

FlushBlockPolicy* FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
    const BlockBasedTableOptions& table_options,
    const BlockBuilder& data_block_builder) const {
  return new FlushBlockBySizePolicy(
      table_options.block_size, table_options.block_size_deviation,
      data_block_builder);
}

FlushBlockPolicy* FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
    const uint64_t size, const int deviation,
    const BlockBuilder& data_block_builder) {
  return new FlushBlockBySizePolicy(size, deviation, data_block_builder);
}

// Flush block policy for Noise. Flush for every different keypath or size,
// whatever comes first
class NoiseFlushBlockPolicy: public FlushBlockBySizePolicy {
 public:
  // @params block_size:           Approximate size of user data packed per
  //                               block.
  // @params block_size_deviation: This is used to close a block before it
  //                               reaches the configured
  NoiseFlushBlockPolicy(const uint64_t block_size,
                        const uint64_t block_size_deviation,
                        const BlockBuilder& data_block_builder)
      : FlushBlockBySizePolicy(block_size, block_size_deviation,
                               data_block_builder) {}

  virtual bool Update(const Slice& key,
                      const Slice& value) override {
    Slice key_slice = Slice(key);
    Slice keypath;
    GetLengthPrefixedSlice(&key_slice, &keypath);

    // First call, there wasn't any keypath yet
    if (prev_keypath_.empty()) {
      prev_keypath_ = std::string(keypath.data(), keypath.size());
    }

    bool size_reached = FlushBlockBySizePolicy::Update(key, value);
    if (size_reached) {
      return true;
    }
    // The size limit isn't reached yet, but perhaps the keypath changed
    else if (keypath.compare(Slice(prev_keypath_)) != 0) {
      prev_keypath_ = std::string(keypath.data(), keypath.size());
      return true;
    }

    return false;
  }

 private:
  std::string prev_keypath_;
};

FlushBlockPolicy* NoiseFlushBlockPolicyFactory::NewFlushBlockPolicy(
    const BlockBasedTableOptions& table_options,
    const BlockBuilder& data_block_builder) const {
  return new NoiseFlushBlockPolicy(
      table_options.block_size, table_options.block_size_deviation,
      data_block_builder);
};

}  // namespace rocksdb
