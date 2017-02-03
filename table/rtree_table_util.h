//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <string>
#include <vector>


namespace rocksdb {

const size_t kMinInternalKeySize = 8;

class RtreeUtil {
 public:
  // Encodes a given bounding box as `InternalKey`
  static std::string EncodeKey(std::vector<double>& mbb);
  // Return the enclosing bounds of two multi-dimensional bounding boxes
  static std::vector<double> EnclosingMbb(const double* aa,
                                          const double* bb,
                                          uint8_t dimensions);
  // Return true if the two given bounding boxes intersect or one isn't defined
  // Convinience method to make the caller code easier to read
  static bool IntersectMbb(const double* aa,
                           const std::string& bb,
                           uint8_t dimensions);
  // Return true if the two given bounding boxes intersect or one isn't defined
  static bool IntersectMbb(const double* aa,
                           const double* bb,
                           uint8_t dimensions);
 private:
  // It's not allowed to create an instance of `RtreeUtil`
  RtreeUtil() {}
};

}  // namespace rocksdb
