//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <vector>

#include "util/rtree.h"

namespace rocksdb {

bool IntersectMbb(Mbb aa, Mbb bb) {
  // If a bounding box is empty, return true, as it was likely the query
  // bounding box which then corresponds to a full table scan
  if (aa.empty() || bb.empty()) {
    return true;
  }

  // If the bounding boxes don't intersect in one dimension, they won't
  // intersect at all, hence we can return early
  if (aa.iid.min > bb.iid.max || bb.iid.min > aa.iid.max) {
    return false;
  }
  if (aa.first.min > bb.first.max || bb.first.min > aa.first.max) {
    return false;
  }
  if (aa.second.min > bb.second.max || bb.second.min > aa.second.max) {
    return false;
  }
  return true;
}

void ReadMbbValues(Mbb& mbb, Slice& data) {
  double min = *reinterpret_cast<const double*>(data.data());
  double max = *reinterpret_cast<const double*>(data.data() + 8);
  mbb.set_first(min, max);
  min = *reinterpret_cast<const double*>(data.data() + 16);
  max = *reinterpret_cast<const double*>(data.data() + 24);
  mbb.set_second(min, max);
}

Mbb ReadKeyMbb(Slice data) {
  Mbb mbb;
  // In a key the first dimension is a single value only
  const uint64_t iid = *reinterpret_cast<const uint64_t*>(data.data());
  mbb.set_iid(iid, iid);
  data.remove_prefix(sizeof(uint64_t));
  ReadMbbValues(mbb, data);
  return mbb;
}

Mbb ReadQueryMbb(Slice data) {
  Mbb mbb;
  // In a key the first dimension is a single value only
  const uint64_t iid_min = *reinterpret_cast<const uint64_t*>(data.data());
  const uint64_t iid_max =
      *reinterpret_cast<const uint64_t*>(data.data() + sizeof(uint64_t));
  mbb.set_iid(iid_min, iid_max);
  data.remove_prefix(2 * sizeof(uint64_t));
  ReadMbbValues(mbb, data);
  return mbb;
}

}  // namespace rocksdb
