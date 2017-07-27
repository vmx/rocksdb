//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <assert.h>
#include <inttypes.h>

#include <list>
#include <string>
#include <unordered_map>

#include "rocksdb/comparator.h"
#include "table/block_based_table_factory.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/rtree.h"

namespace rocksdb {
// The interface for building index.
// Instruction for adding a new concrete IndexBuilder:
//  1. Create a subclass instantiated from IndexBuilder.
//  2. Add a new entry associated with that subclass in TableOptions::IndexType.
//  3. Add a create function for the new subclass in CreateIndexBuilder.
// Note: we can devise more advanced design to simplify the process for adding
// new subclass, which will, on the other hand, increase the code complexity and
// catch unwanted attention from readers. Given that we won't add/change
// indexes frequently, it makes sense to just embrace a more straightforward
// design that just works.
class IndexBuilder {
 public:
  static IndexBuilder* CreateIndexBuilder(
      BlockBasedTableOptions::IndexType index_type,
      const rocksdb::InternalKeyComparator* comparator,
      const InternalKeySliceTransform* int_key_slice_transform,
      const BlockBasedTableOptions& table_opt);

  // Index builder will construct a set of blocks which contain:
  //  1. One primary index block.
  //  2. (Optional) a set of metablocks that contains the metadata of the
  //     primary index.
  struct IndexBlocks {
    Slice index_block_contents;
    std::unordered_map<std::string, Slice> meta_blocks;
  };
  explicit IndexBuilder(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}

  virtual ~IndexBuilder() {}

  // Add a new index entry to index block.
  // To allow further optimization, we provide `last_key_in_current_block` and
  // `first_key_in_next_block`, based on which the specific implementation can
  // determine the best index key to be used for the index block.
  // @last_key_in_current_block: this parameter maybe overridden with the value
  //                             "substitute key".
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  //
  // REQUIRES: Finish() has not yet been called.
  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) = 0;

  // This method will be called whenever a key is added. The subclasses may
  // override OnKeyAdded() if they need to collect additional information.
  virtual void OnKeyAdded(const Slice& key) {}

  // Inform the index builder that all entries has been written. Block builder
  // may therefore perform any operation required for block finalization.
  //
  // REQUIRES: Finish() has not yet been called.
  inline Status Finish(IndexBlocks* index_blocks) {
    // Throw away the changes to last_partition_block_handle. It has no effect
    // on the first call to Finish anyway.
    BlockHandle last_partition_block_handle;
    return Finish(index_blocks, last_partition_block_handle);
  }

  // This override of Finish can be utilized to build the 2nd level index in
  // PartitionIndexBuilder.
  //
  // index_blocks will be filled with the resulting index data. If the return
  // value is Status::InComplete() then it means that the index is partitioned
  // and the callee should keep calling Finish until Status::OK() is returned.
  // In that case, last_partition_block_handle is pointer to the block written
  // with the result of the last call to Finish. This can be utilized to build
  // the second level index pointing to each block of partitioned indexes. The
  // last call to Finish() that returns Status::OK() populates index_blocks with
  // the 2nd level index content.
  virtual Status Finish(IndexBlocks* index_blocks,
                        const BlockHandle& last_partition_block_handle) = 0;

  // Get the estimated size for index block.
  virtual size_t EstimatedSize() const = 0;

 protected:
  const InternalKeyComparator* comparator_;
};

// This index builder builds space-efficient index block.
//
// Optimizations:
//  1. Made block's `block_restart_interval` to be 1, which will avoid linear
//     search when doing index lookup (can be disabled by setting
//     index_block_restart_interval).
//  2. Shorten the key length for index block. Other than honestly using the
//     last key in the data block as the index key, we instead find a shortest
//     substitute key that serves the same function.
class ShortenedIndexBuilder : public IndexBuilder {
 public:
  explicit ShortenedIndexBuilder(const InternalKeyComparator* comparator,
                                 int index_block_restart_interval)
      : IndexBuilder(comparator),
        index_block_builder_(index_block_restart_interval) {}

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    if (first_key_in_next_block != nullptr) {
      comparator_->FindShortestSeparator(last_key_in_current_block,
                                         *first_key_in_next_block);
    } else {
      comparator_->FindShortSuccessor(last_key_in_current_block);
    }

    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    index_block_builder_.Add(*last_key_in_current_block, handle_encoding);
  }

  using IndexBuilder::Finish;
  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override {
    index_blocks->index_block_contents = index_block_builder_.Finish();
    return Status::OK();
  }

  virtual size_t EstimatedSize() const override {
    return index_block_builder_.CurrentSizeEstimate();
  }

  friend class PartitionedIndexBuilder;

 private:
  BlockBuilder index_block_builder_;
};

// HashIndexBuilder contains a binary-searchable primary index and the
// metadata for secondary hash index construction.
// The metadata for hash index consists two parts:
//  - a metablock that compactly contains a sequence of prefixes. All prefixes
//    are stored consectively without any metadata (like, prefix sizes) being
//    stored, which is kept in the other metablock.
//  - a metablock contains the metadata of the prefixes, including prefix size,
//    restart index and number of block it spans. The format looks like:
//
// +-----------------+---------------------------+---------------------+
// <=prefix 1
// | length: 4 bytes | restart interval: 4 bytes | num-blocks: 4 bytes |
// +-----------------+---------------------------+---------------------+
// <=prefix 2
// | length: 4 bytes | restart interval: 4 bytes | num-blocks: 4 bytes |
// +-----------------+---------------------------+---------------------+
// |                                                                   |
// | ....                                                              |
// |                                                                   |
// +-----------------+---------------------------+---------------------+
// <=prefix n
// | length: 4 bytes | restart interval: 4 bytes | num-blocks: 4 bytes |
// +-----------------+---------------------------+---------------------+
//
// The reason of separating these two metablocks is to enable the efficiently
// reuse the first metablock during hash index construction without unnecessary
// data copy or small heap allocations for prefixes.
class HashIndexBuilder : public IndexBuilder {
 public:
  explicit HashIndexBuilder(const InternalKeyComparator* comparator,
                            const SliceTransform* hash_key_extractor,
                            int index_block_restart_interval)
      : IndexBuilder(comparator),
        primary_index_builder_(comparator, index_block_restart_interval),
        hash_key_extractor_(hash_key_extractor) {}

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    ++current_restart_index_;
    primary_index_builder_.AddIndexEntry(last_key_in_current_block,
                                         first_key_in_next_block, block_handle);
  }

  virtual void OnKeyAdded(const Slice& key) override {
    auto key_prefix = hash_key_extractor_->Transform(key);
    bool is_first_entry = pending_block_num_ == 0;

    // Keys may share the prefix
    if (is_first_entry || pending_entry_prefix_ != key_prefix) {
      if (!is_first_entry) {
        FlushPendingPrefix();
      }

      // need a hard copy otherwise the underlying data changes all the time.
      // TODO(kailiu) ToString() is expensive. We may speed up can avoid data
      // copy.
      pending_entry_prefix_ = key_prefix.ToString();
      pending_block_num_ = 1;
      pending_entry_index_ = static_cast<uint32_t>(current_restart_index_);
    } else {
      // entry number increments when keys share the prefix reside in
      // different data blocks.
      auto last_restart_index = pending_entry_index_ + pending_block_num_ - 1;
      assert(last_restart_index <= current_restart_index_);
      if (last_restart_index != current_restart_index_) {
        ++pending_block_num_;
      }
    }
  }

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override {
    FlushPendingPrefix();
    primary_index_builder_.Finish(index_blocks, last_partition_block_handle);
    index_blocks->meta_blocks.insert(
        {kHashIndexPrefixesBlock.c_str(), prefix_block_});
    index_blocks->meta_blocks.insert(
        {kHashIndexPrefixesMetadataBlock.c_str(), prefix_meta_block_});
    return Status::OK();
  }

  virtual size_t EstimatedSize() const override {
    return primary_index_builder_.EstimatedSize() + prefix_block_.size() +
           prefix_meta_block_.size();
  }

 private:
  void FlushPendingPrefix() {
    prefix_block_.append(pending_entry_prefix_.data(),
                         pending_entry_prefix_.size());
    PutVarint32Varint32Varint32(
        &prefix_meta_block_,
        static_cast<uint32_t>(pending_entry_prefix_.size()),
        pending_entry_index_, pending_block_num_);
  }

  ShortenedIndexBuilder primary_index_builder_;
  const SliceTransform* hash_key_extractor_;

  // stores a sequence of prefixes
  std::string prefix_block_;
  // stores the metadata of prefixes
  std::string prefix_meta_block_;

  // The following 3 variables keeps unflushed prefix and its metadata.
  // The details of block_num and entry_index can be found in
  // "block_hash_index.{h,cc}"
  uint32_t pending_block_num_ = 0;
  uint32_t pending_entry_index_ = 0;
  std::string pending_entry_prefix_;

  uint64_t current_restart_index_ = 0;
};

/**
 * IndexBuilder for two-level indexing. Internally it creates a new index for
 * each partition and Finish then in order when Finish is called on it
 * continiously until Status::OK() is returned.
 *
 * The format on the disk would be I I I I I I IP where I is block containing a
 * partition of indexes built using ShortenedIndexBuilder and IP is a block
 * containing a secondary index on the partitions, built using
 * ShortenedIndexBuilder.
 */
class PartitionedIndexBuilder : public IndexBuilder {
 public:
  static PartitionedIndexBuilder* CreateIndexBuilder(
      const rocksdb::InternalKeyComparator* comparator,
      const BlockBasedTableOptions& table_opt);

  explicit PartitionedIndexBuilder(const InternalKeyComparator* comparator,
                                   const BlockBasedTableOptions& table_opt);

  virtual ~PartitionedIndexBuilder();

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override;

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override;

  virtual size_t EstimatedSize() const override;

  inline bool ShouldCutFilterBlock() {
    // Current policy is to align the partitions of index and filters
    if (cut_filter_block) {
      cut_filter_block = false;
      return true;
    }
    return false;
  }

  std::string& GetPartitionKey() { return sub_index_last_key_; }

 private:
  void MakeNewSubIndexBuilder();

  struct Entry {
    std::string key;
    std::unique_ptr<ShortenedIndexBuilder> value;
  };
  std::list<Entry> entries_;  // list of partitioned indexes and their keys
  BlockBuilder index_block_builder_;  // top-level index builder
  // the active partition index builder
  ShortenedIndexBuilder* sub_index_builder_;
  // the last key in the active partition index builder
  std::string sub_index_last_key_;
  std::unique_ptr<FlushBlockPolicy> flush_policy_;
  // true if Finish is called once but not complete yet.
  bool finishing_indexes = false;
  const BlockBasedTableOptions& table_opt_;
  // true if it should cut the next filter partition block
  bool cut_filter_block = false;
};

// This index builder builds an R-tree. The code is currently specific to
// Noise, but could be made more generic.
class RtreeIndexBuilder : public IndexBuilder {
 public:
  explicit RtreeIndexBuilder(const InternalKeyComparator* comparator)
      : IndexBuilder(comparator),
      prev_keypath_(""),
      last_block_builder_(1) {}

  // Get the enclosing MBB from this block and use it as key
  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    Slice key = Slice(*last_key_in_current_block);
    Slice keypath;

    // The key consists of the Keypath, and several intervals. The first
    // interval is the Internal Id, all others are the other dimensions.
    GetLengthPrefixedSlice(&key, &keypath);

    // First call, there wasn't any keypath yet
    if (prev_keypath_.empty()) {
      prev_keypath_ = std::string(keypath.data(), keypath.size());
    }

    // A new keypath, hence a new index block
    if (keypath.compare(Slice(prev_keypath_)) != 0) {
      flush_rtree();
      prev_keypath_ = std::string(keypath.data(), keypath.size());
    }

    // Encode the block handle and construct leaf node
    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    LeafNode leaf_node = LeafNode{enclosing_mbb_, handle_encoding};
    leaf_nodes_.push_back(leaf_node);

    enclosing_mbb_.clear();

    block_last_key_ = std::string(*last_key_in_current_block);
  }

  // Calculate the enclosing MBB for each block
  virtual void OnKeyAdded(const Slice& const_key) override {
    Slice key = ExtractUserKey(const_key);
    Slice keypath;

    // The key consists of the Keypath, and several intervals. The first
    // interval is the Internal Id, all others are the other dimensions.
    GetLengthPrefixedSlice(&key, &keypath);

    std::vector<Interval> mbb = ReadMbb(key);
    expand_mbb(enclosing_mbb_, mbb);
  }


  using IndexBuilder::Finish;
  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override {
    // `Finish` is called for *every* index block in case your index
    // returns several ones. It only needs to return `Status::Incomplete()`
    // if there are more blocks to finish. Once the you are at the final
    // block, return `Status::OK()`.

    // There might still be points which aren't built up to a R-tree yet
    if (!leaf_nodes_.empty()) {
      flush_rtree();
      prev_keypath_ = std::string();
    }

    assert(!entries_.empty());

    // Index blocks are currently stored on disk. Store the pointers to
    // them in the last index block.
    if (finishing_indexes_ == true) {
      Entry& last_entry = entries_.front();
      std::string handle_encoding;
      last_partition_block_handle.EncodeTo(&handle_encoding);
      last_block_builder_.Add(last_entry.key, handle_encoding);
      entries_.pop_front();
    }

    // All index blocks are stored, store the last one
    if (entries_.empty()) {
      index_blocks->index_block_contents = last_block_builder_.Finish();
      return Status::OK();
    }
    // There are still blocks that need to be stored
    else {
      Entry &entry = entries_.front();
      std::string handle_encoding;
      index_blocks->index_block_contents = Slice(
          reinterpret_cast<const char *>(entry.rtree->data()),
          entry.rtree->size());
      finishing_indexes_ = true;
      return Status::Incomplete();
    }
  }

  virtual size_t EstimatedSize() const override {
    size_t total = 0;
    // The index blocks so far processed
    for (auto& entry: entries_) {
      total += entry.rtree->size();
    }
    // The current index block
    // TODO vmx 2017-06-07: Get correct size instead of guessing "48"
    total += leaf_nodes_.size() * 48;
    return total;
  }

  friend class PartitionedIndexBuilder;

 private:
  // The lowest level of the R-tree has the MBB of the block and the block
  // handle pointing to that block.
  struct LeafNode {
    std::vector<Interval> mbb;
    std::string encoded_block_handle;
  };
  // The current enclosing Mbb of the current leaf node
  std::vector<Interval> enclosing_mbb_;
  // The keypath of the previous key. If it is different, create a new R-tree
  std::string prev_keypath_;
  // The last key of a block. It's used for main filter block that points
  // to the individual R-trees
  std::string block_last_key_;
  // The MBBs that are the lowest level of the R-tree
  std::vector<LeafNode> leaf_nodes_;
  // And entry consists of the last key of the block and its R-tree
  struct Entry {
    std::string key;
    std::unique_ptr<std::string> rtree;
  };
  // List of R-trees
  std::list<Entry> entries_;
  // The last index block that contains pointers to the other blocks
  //std::string last_block_;
  BlockBuilder last_block_builder_;

  // true if Finish is called once but not complete yet.
  bool finishing_indexes_ = false;

  // Expands the the first Mbb if the second one is bigger
  void expand_mbb(std::vector<Interval>& to_expand,
                  std::vector<Interval> expander) {
    if (to_expand.empty()) {
      to_expand = expander;
    } else {
      assert(to_expand.size() == expander.size());
      for (size_t ii = 0; ii < to_expand.size(); ii++) {
        if (expander[ii].min < to_expand[ii].min) {
          to_expand[ii].min = expander[ii].min;
        }
        if (expander[ii].max > to_expand[ii].max) {
          to_expand[ii].max = expander[ii].max;
        }
      }
    }
  }

  std::string serialize_mbb(const std::vector<Interval>& mbb) {
    std::string serialized;
    for (auto& interval: mbb) {
      serialized.append(reinterpret_cast<const char*>(&interval.min),
                        sizeof(double));
      serialized.append(reinterpret_cast<const char*>(&interval.max),
                        sizeof(double));
    }
    return serialized;
  }

  std::string* serialize_leaf_nodes(const std::vector<LeafNode>& leaf_nodes) {
    // The unique pointer that wraps the leaf_nodes will free the memory
    std::string* serialized = new std::string();
    for (auto& leaf_node: leaf_nodes) {
      std::string serialized_mbb = serialize_mbb(leaf_node.mbb);
      serialized->append(serialized_mbb);
      serialized->append(leaf_node.encoded_block_handle);
    }
    return serialized;
  }

  // Add the current parents level to the R-tree and return the new
  // level of parents
  std::vector<std::vector<Interval>> serialize_parents(
      std::string* rtree, std::vector<std::vector<Interval>> mbbs) {
    // The offset the current level of nodes start rounded down to the
    // block boundary (which is determined by the inner node size)
    uint64_t level_offset =
        rtree->size() - (rtree->size() % kRtreeInnerNodeSize);
    // Parent nodes that are collecting duing the bottom-up build
    std::vector<std::vector<Interval>> parents;
    // The MBB the encloses the current children
    std::vector<Interval> enclosing_mbb;

    for (auto& mbb : mbbs) {
      std::string serialized_mbb = serialize_mbb(mbb);
      // Create a new parent for the certain node size
      // The `-1` is for the case when the current node fits in exactly,
      // then we don't want to create a new parent, but let it be done
      // after this loop.
      if ((rtree->size() - level_offset + serialized_mbb.size() - 1) /
              kRtreeInnerNodeSize >
          parents.size()) {
        parents.push_back(enclosing_mbb);
        enclosing_mbb.clear();
      }
      expand_mbb(enclosing_mbb, mbb);
      rtree->append(serialized_mbb);
    }
    // There's always at least one left-over node that needs a parent
    parents.push_back(enclosing_mbb);

    return parents;
  }

  std::string* build_rtree(const std::vector<LeafNode>& leaf_nodes) {
    // The unique pointer that wraps the leaf_nodes will free the memory
    std::string* rtree = new std::string();
    // Parent nodes that are collecting duing the bottom-up build
    std::vector<std::vector<Interval>> parents;
    // The MBB the encloses the current children
    std::vector<Interval> enclosing_mbb;
    // The offset where the current children start
    uint64_t offset = 0;
    // A collection of serialized leaf nodes with the maximum
    // inner leaf ndde size
    std::string serialized_leaf_nodes;
    serialized_leaf_nodes.reserve(kRtreeInnerNodeSize);

    for (auto& leaf_node : leaf_nodes) {
      std::string serialized_mbb = serialize_mbb(leaf_node.mbb);

      // Create a new parent if the maximum node size is reached
      const size_t next_size = serialized_leaf_nodes.size() +
                               serialized_mbb.size() +
                               leaf_node.encoded_block_handle.size();
      // Create a new parent if the node would become too large
      if (next_size > kRtreeInnerNodeSize) {
        parents.push_back(enclosing_mbb);
        enclosing_mbb.clear();
        PutVarint32(rtree, serialized_leaf_nodes.size());
        rtree->append(serialized_leaf_nodes);
        // Nodes are block alligned to the inner node size, hence fill
        // up the space to the next block
        const size_t next_block_offset = rtree->size() -
                                         (rtree->size() % kRtreeInnerNodeSize) +
                                         kRtreeInnerNodeSize;
        rtree->resize(next_block_offset);
        serialized_leaf_nodes.clear();

        offset = rtree->size();
      }
      serialized_leaf_nodes.append(serialized_mbb);
      serialized_leaf_nodes.append(leaf_node.encoded_block_handle);
      expand_mbb(enclosing_mbb, leaf_node.mbb);
    }

    // There's always nodes that weren't flushed with a parent not being
    // stored yet
    PutVarint32(rtree, serialized_leaf_nodes.size());
    rtree->append(serialized_leaf_nodes);
    offset = rtree->size();
    parents.push_back(enclosing_mbb);
    enclosing_mbb.clear();

    // Stop when it diverged to a single root node
    do {
      // Nodes are block alligned to the inner node size, hence fill
      // up the space to the next block
      const size_t next_block_offset = rtree->size() -
                                       (rtree->size() % kRtreeInnerNodeSize) +
                                       kRtreeInnerNodeSize;
      // Only add padding if it isn't already on the block boundary
      if (next_block_offset - rtree->size() < kRtreeInnerNodeSize) {
        rtree->resize(next_block_offset);
      }
      parents = serialize_parents(rtree, parents);
    } while (parents.size() > 1);

    // Add a footer to the R-tree. It's 8 bytes and contains the offset
    // where the leaf nodes end (the data, without the padding)
    PutFixed64(rtree, offset);
    return rtree;
  }

  // Build an R-tree out of the current data and put it into the list of trees
  void flush_rtree() {
    std::string* leaf_nodes = build_rtree(leaf_nodes_);
    entries_.push_back(
        {block_last_key_,
         std::unique_ptr<std::string>(leaf_nodes)});
    leaf_nodes_.clear();
  }
};
}  // namespace rocksdb
