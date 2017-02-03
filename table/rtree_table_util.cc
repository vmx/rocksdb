//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/rtree_table_util.h"

#include "db/dbformat.h"
#include "rocksdb/slice.h"

namespace rocksdb {

std::string RtreeUtil::EncodeKey(std::vector<double>& mbb) {
  Slice slice = Slice(reinterpret_cast<const char*>(mbb.data()),
                      sizeof(mbb[0]) * mbb.size());
  // NOTE vmx 2017-02-01: Use the internal key representation for consistency
  // across inner and leaf nodes
  InternalKey ikey;
  ikey.SetMaxPossibleForUserKey(slice);
  return ikey.Encode().ToString();
}

std::vector<double> RtreeUtil::EnclosingMbb(
    const double* aa,
    const double* bb,
    uint8_t dimensions) {
  std::vector<double> enclosing;
  enclosing.reserve(dimensions * 2);

  if (aa == nullptr) {
    enclosing = std::vector<double>(bb, bb + dimensions * 2);
  } else if (bb == nullptr) {
    enclosing = std::vector<double>(aa, aa + dimensions * 2);
  } else {
    // Loop through min and max in a single step
    for (size_t ii = 0; ii < dimensions * 2; ii += 2) {
      aa[ii] < bb[ii]
                 ? enclosing.push_back(aa[ii])
                 : enclosing.push_back(bb[ii]);
      aa[ii + 1] > bb[ii + 1]
                 ? enclosing.push_back(aa[ii + 1])
                 : enclosing.push_back(bb[ii + 1]);
    }
  }
  return enclosing;
}

bool RtreeUtil::IntersectMbb(
    const double* aa,
    const std::string& bb,
    uint8_t dimensions) {
  return IntersectMbb(aa,
                      bb.empty() ?
                          nullptr :
                          reinterpret_cast<const double*>(bb.data()),
                      dimensions);
}

bool RtreeUtil::IntersectMbb(
    const double* aa,
    const double* bb,
    uint8_t dimensions) {
  // Two bounding boxes are considered interseting if one of them isn't
  // defined. This way a tablescan returning all the data is easily
  // possible
  if (aa == nullptr || bb == nullptr) {
    return true;
  }
  // Loop through min and max in a single step. If the bounding boxes don't
  // intersect in one dimension, they won't intersect at all, hence we
  // can return early.
  for (size_t ii = 0; ii < dimensions * 2; ii += 2) {
    if (aa[ii] > bb[ii + 1] || bb[ii] > aa[ii + 1]) {
      return false;
    }
  }
  return true;
}

}  // namespace rocksdb
