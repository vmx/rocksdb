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
  // intersect at all, hence we can return early. The fields hold the
  // byte-orderable encoded form, but numeric comparison on the host-order
  // uint64_ts gives the same answer as the comparisons on the original
  // (decoded) values.
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

Mbb ReadKeyMbb(Slice data) {
  Mbb mbb;
  // In a key the first dimension is a single value only
  const uint64_t iid = ReadBeU64(data.data());
  mbb.iid = {iid, iid};
  mbb.first = {ReadBeU64(data.data() + 8), ReadBeU64(data.data() + 16)};
  mbb.second = {ReadBeU64(data.data() + 24), ReadBeU64(data.data() + 32)};
  return mbb;
}

Mbb ReadQueryMbb(Slice data) {
  Mbb mbb;
  // In a query the first dimension is a [min, max] range
  mbb.iid = {ReadBeU64(data.data()), ReadBeU64(data.data() + 8)};
  mbb.first = {ReadBeU64(data.data() + 16), ReadBeU64(data.data() + 24)};
  mbb.second = {ReadBeU64(data.data() + 32), ReadBeU64(data.data() + 40)};
  return mbb;
}

}  // namespace rocksdb
