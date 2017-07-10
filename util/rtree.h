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

// NOTE vmx 2017-07-29: It must be a multiple of the internal key size,
// which is at the moment 48 bytes
static const size_t kRtreeInnerNodeSize = 1056;

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

struct IntInterval {
  uint64_t min;
  uint64_t max;

  friend std::ostream& operator<<(std::ostream& os,
                                  const IntInterval& interval) {
    return os << "[" << interval.min << "," << interval.max << "]";
  };
};

class Mbb {
 public:
  Mbb() : isempty_(true) {}

  // Whether any valued were set or not (true no values were set yet)
  bool empty() { return isempty_; };
  // Unset the Mbb
  void clear() { isempty_ = true; };

  void set_iid(const uint64_t min, const uint64_t max) {
    iid = {min, max};
    isempty_ = false;
  }
  void set_first(const double min, const double max) {
    first = {min, max};
    isempty_ = false;
  }
  void set_second(const double min, const double max) {
    second = {min, max};
    isempty_ = false;
  }

  friend std::ostream& operator<<(std::ostream& os, const Mbb& mbb) {
    return os << "[" << mbb.iid << "," << mbb.first << "," << mbb.second << "]";
  };

  IntInterval iid;
  Interval first;
  Interval second;

 private:
  bool isempty_;
};

extern bool IntersectMbb(Mbb aa, Mbb bb);

// Reads the mbb (intervals) from a key. The first dimension is the
// Internal Id, hence a single value and not an interval.
// It modifies the key slice.
extern Mbb ReadKeyMbb(Slice data);

// Reads the mbb (intervals) from a query. The first dimension is the
// Internal Id, the other two is values.
// It modifies the key slice.
extern Mbb ReadQueryMbb(Slice data);

}  // namespace rocksdb
