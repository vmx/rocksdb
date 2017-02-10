//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <vector>

#include "util/rtree.h"

namespace rocksdb {

bool IntersectMbb(std::vector<Interval> aa,
                  std::vector<Interval> bb) {
  // If a bounding box is empty, return true, as it was likely the query
  // bounding box which then corresponds to a full table scan
  if (aa.empty() || bb.empty()) {
    return true;
  }
  assert(aa.size() == bb.size());

  // If the bounding boxes don't intersect in one dimension, they won't
  // intersect at all, hence we can return early
  for (size_t ii = 0; ii < aa.size(); ii++) {
    if(aa[ii].min > bb[ii].max || bb[ii].min > aa[ii].max) {
      return false;
    }
  }
  return true;
}

std::vector<Interval> ReadMbb(Slice data) {
  std::vector<Interval> mbb;
  mbb.reserve(data.size() / sizeof(double));
  while (data.size() > 0) {
    const double* min = reinterpret_cast<const double*>(data.data());
    data.remove_prefix(sizeof(double));
    const double* max = reinterpret_cast<const double*>(data.data());
    data.remove_prefix(sizeof(double));
    mbb.push_back(Interval{*min, *max});
  }
  return mbb;
}

}  // namespace rocksdb
