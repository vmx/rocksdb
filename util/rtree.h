//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Utility functions needed for the R-tree

#pragma once
#include <ostream>

#include "rocksdb/options.h"

namespace rocksdb {

// There's an interval (which might be collapsed to a point) for every
// dimension
struct Interval {
  double min;
  double max;

  friend std::ostream& operator<<(std::ostream& os, const Interval& interval) {
    return os  << "[" << interval.min << "," << interval.max << "]";
  };

  friend std::ostream& operator<<(std::ostream& os, const std::vector<Interval>& intervals) {
    os << "[";
    bool first = true;
    for (auto& interval: intervals) {
      if (first) {
          first = false;
      } else {
          os << ",";
      }
      os  << interval;
    }
    return os << "]";
  };
};

struct RtreeIteratorContext: public IteratorContext {
  std::string query_mbb;
  RtreeIteratorContext(): query_mbb() {};
};


extern bool IntersectMbb(std::vector<Interval> aa, std::vector<Interval> bb);

// Reads the mbb (intervals) from the key. It modifies the key slice.
extern std::vector<Interval> ReadMbb(Slice data);

}  // namespace rocksdb
