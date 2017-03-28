//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/rtree_table_util.h"

#include "db/dbformat.h"
#include "rocksdb/slice.h"

namespace rocksdb {


void Variant::Init(const Variant& v, Data& d) {
  switch (v.type_) {
    case kNull:
      break;
    case kBool:
      d.b = v.data_.b;
      break;
    case kInt:
      d.i = v.data_.i;
      break;
    case kDouble:
      d.d = v.data_.d;
      break;
    case kString:
      new (d.s) std::string(*GetStringPtr(v.data_));
      break;
    default:
      assert(false);
  }
}

Variant& Variant::operator=(const Variant& v) {
  // Construct first a temp so exception from a string ctor
  // does not change this object
  Data tmp;
  Init(v, tmp);

  Type thisType = type_;
  // Boils down to copying bits so safe
  std::swap(tmp, data_);
  type_ = v.type_;

  Destroy(thisType, tmp);

  return *this;
}

Variant& Variant::operator=(Variant&& rhs) {
  Destroy(type_, data_);
  if (rhs.type_ == kString) {
    new (data_.s) std::string(std::move(*GetStringPtr(rhs.data_)));
  } else {
    data_ = rhs.data_;
  }
  type_ = rhs.type_;
  rhs.type_ = kNull;
  return *this;
}


bool Variant::operator==(const Variant& rhs) const {
  if (type_ != rhs.type_) {
    return false;
  }

  switch (type_) {
    case kNull:
      return true;
    case kBool:
      return data_.b == rhs.data_.b;
    case kInt:
      return data_.i == rhs.data_.i;
    case kDouble:
      return data_.d == rhs.data_.d;
    case kString:
      return *GetStringPtr(data_) == *GetStringPtr(rhs.data_);
    default:
      assert(false);
  }
  // it will never reach here, but otherwise the compiler complains
  return false;
}


bool Variant::operator<(const Variant& rhs) const {
  if (type_ != rhs.type_) {
    return false;
  }

  switch (type_) {
    case kNull:
      return true;
    case kBool:
      return data_.b < rhs.data_.b;
    case kInt:
      return data_.i < rhs.data_.i;
    case kDouble:
      return data_.d < rhs.data_.d;
    case kString:
      return *GetStringPtr(data_) < *GetStringPtr(rhs.data_);
    default:
      assert(false);
  }
  // it will never reach here, but otherwise the compiler complains
  return false;
}

bool Variant::operator>(const Variant& rhs) const {
  if (type_ != rhs.type_) {
    return false;
  }

  switch (type_) {
    case kNull:
      return true;
    case kBool:
      return data_.b > rhs.data_.b;
    case kInt:
      return data_.i > rhs.data_.i;
    case kDouble:
      return data_.d > rhs.data_.d;
    case kString:
      return *GetStringPtr(data_) > *GetStringPtr(rhs.data_);
    default:
      assert(false);
  }
  // it will never reach here, but otherwise the compiler complains
  return false;
}

std::string RtreeUtil::EncodeKey(std::vector<Variant>& mbb) {
  RtreeKeyBuilder serialized;
  for (auto dimension: mbb) {
    switch (dimension.type()) {
      case Variant::kDouble:
        serialized.push_double(dimension.get_double());
        break;
      case Variant::kNull:
      case Variant::kBool:
      case Variant::kInt:
      case Variant::kString:
        serialized.push_string(dimension.get_string());
        break;
      default:
        assert(false && "EncodeKey: not yet implemented");
        // TODO vmx 2017-03-03: Handle other cases
        break;
    }
  }
  Slice slice = Slice(serialized.data(), serialized.size());
  // NOTE vmx 2017-02-01: Use the internal key representation for consistency
  // across inner and leaf nodes
  InternalKey ikey;
  ikey.SetMaxPossibleForUserKey(slice);
  return ikey.Encode().ToString();
}

void RtreeUtil::ExpandMbb(std::vector<Variant>& base,
                          const Slice& expansion_const) {
  Slice expansion(expansion_const);
  bool base_is_empty = false;
  if (base.empty()) {
    base_is_empty = true;
  }
  double val_double;
  Slice val_slice;
  std::string val_string;
  for (size_t ii = 0; expansion.size() > kMinInternalKeySize; ii++) {
    RtreeDimensionType type = static_cast<RtreeDimensionType>(*expansion.data());
    expansion.remove_prefix(sizeof(uint8_t));
    switch(type) {
      case RtreeDimensionType::kDouble:
        val_double = *reinterpret_cast<const double*>(expansion.data());
        if (base_is_empty) {
          base.push_back(val_double);
        }
        else if (
            // Min
            (ii % 2 == 0 && val_double < base[ii].get_double()) ||
            // Max
            (ii % 2 == 1 && val_double > base[ii].get_double())) {
          base[ii] = Variant(val_double);
        }
        expansion.remove_prefix(sizeof(double));
        break;
      case RtreeDimensionType::kString:
        GetLengthPrefixedSlice(&expansion, &val_slice);
        val_string = std::string(val_slice.data(), val_slice.size());
        if (base_is_empty) {
          base.push_back(val_string);
        }
        else if (
            // Min
            (ii % 2 == 0 && val_string < base[ii].get_string()) ||
            // Max
            (ii % 2 == 1 && val_string > base[ii].get_string())) {
          base[ii] = Variant(val_string);
        }
        // `GetLengthPrefixedSlice` already advanced the slice
        break;
      default:
        assert(false && "not yet implemented");
        break;
    }
  }
}

bool RtreeUtil::IntersectMbb(
    const Slice& aa_orig,
    const std::string& bb_orig) {
  // If the query bounding box is empty, return true, which corresponds to a
  // full table scan
  if (bb_orig.size() == 0) {
    return true;
  }
  // Make a mutable copy of the slices
  Slice aa = Slice(aa_orig);
  Slice bb = Slice(bb_orig);

  RtreeDimensionType aa_type_min;
  RtreeDimensionType aa_type_max;
  RtreeDimensionType bb_type_min;
  RtreeDimensionType bb_type_max;

  double aa_double_min;
  double aa_double_max;
  double bb_double_min;
  double bb_double_max;

  Slice aa_slice_min;
  Slice aa_slice_max;
  Slice bb_slice_min;
  Slice bb_slice_max;

  // If the bounding boxes don't intersect in one dimension, they won't
  // intersect at all, hence we can return early
  for (size_t ii = 0; aa.size() > kMinInternalKeySize; ii++) {
    // Min
    if (ii % 2 == 0) {
      aa_type_min = static_cast<RtreeDimensionType>(*aa.data());
      aa.remove_prefix(sizeof(uint8_t));
      switch(aa_type_min) {
        case RtreeDimensionType::kDouble:
          aa_double_min = *reinterpret_cast<const double*>(aa.data());
          aa.remove_prefix(sizeof(double));
          break;
        case RtreeDimensionType::kString:
          GetLengthPrefixedSlice(&aa, &aa_slice_min);
          break;
        default:
          assert(false && "Not all types are implemented yet");
          // TODO vmx 2017-03-03: Handle other cases
          break;
      }

      bb_type_min = static_cast<RtreeDimensionType>(*bb.data());
      bb.remove_prefix(sizeof(uint8_t));
      switch(bb_type_min) {
        case RtreeDimensionType::kDouble:
          bb_double_min = *reinterpret_cast<const double*>(bb.data());
          bb.remove_prefix(sizeof(double));
          break;
        case RtreeDimensionType::kString:
          GetLengthPrefixedSlice(&bb, &bb_slice_min);
          break;
        default:
          assert(false && "Not all types are implemented yet");
          // TODO vmx 2017-03-03: Handle other cases
          break;
      }
    }
    // Max
    else {
      aa_type_max = static_cast<RtreeDimensionType>(*aa.data());
      aa.remove_prefix(sizeof(uint8_t));
      switch(aa_type_max) {
        case RtreeDimensionType::kDouble:
          aa_double_max = *reinterpret_cast<const double*>(aa.data());
          aa.remove_prefix(sizeof(double));
          break;
        case RtreeDimensionType::kString:
          GetLengthPrefixedSlice(&aa, &aa_slice_max);
          break;
        default:
          assert(false && "Not all types are implemented yet");
          // TODO vmx 2017-03-03: Handle other cases
          break;
      }

      bb_type_max = static_cast<RtreeDimensionType>(*bb.data());
      bb.remove_prefix(sizeof(uint8_t));
      switch(bb_type_max) {
        case RtreeDimensionType::kDouble:
          bb_double_max = *reinterpret_cast<const double*>(bb.data());
          bb.remove_prefix(sizeof(double));
          break;
        case RtreeDimensionType::kString:
          GetLengthPrefixedSlice(&bb, &bb_slice_max);
          break;
        default:
          assert(false && "Not all types are implemented yet");
          // TODO vmx 2017-03-03: Handle other cases
        break;
      }

      // Min and max got read in, now compare them

      if (aa_type_min == bb_type_max) {
        switch(aa_type_min) {
          case RtreeDimensionType::kDouble:
            if(aa_double_min > bb_double_max) {
              return false;
            }
            break;
          case RtreeDimensionType::kString:
            if (aa_slice_min.compare(bb_slice_max) > 0) {
              return false;
            }
            break;
          default:
            assert(false && "Not all types are implemented yet");
            // TODO vmx 2017-03-03: Handle other cases
            break;
        }
      } else if (aa_type_min > bb_type_max) {
        return false;
      }
      if (bb_type_min == aa_type_max) {
        switch(bb_type_min) {
          case RtreeDimensionType::kDouble:
            if(bb_double_min > aa_double_max) {
              return false;
            }
            break;
          case RtreeDimensionType::kString:
            if (bb_slice_min.compare(aa_slice_max) > 0) {
              return false;
            }
            break;
          default:
            assert(false && "Not all types are implemented yet");
            // TODO vmx 2017-03-03: Handle other cases
            break;
        }
      } else if (bb_type_min > aa_type_max) {
        return false;
      }
    }
  }
  return true;
}

int RtreeUtil::LowxComparatorCompare(const Slice& aa_const,
                                     const Slice& bb_const) {
  Slice aa = Slice(aa_const);
  Slice bb = Slice(bb_const);

  for (size_t ii = 0; aa.size() > 0; ii++) {
    double aa_double;
    Slice aa_slice;
    RtreeDimensionType aa_type = static_cast<RtreeDimensionType>(*aa.data());
    aa.remove_prefix(sizeof(uint8_t));
    switch(aa_type) {
      case RtreeDimensionType::kDouble:
          aa_double = *reinterpret_cast<const double*>(aa.data());
          aa.remove_prefix(sizeof(double));
        break;
      case RtreeDimensionType::kString:
        GetLengthPrefixedSlice(&aa, &aa_slice);
        break;
      default:
        assert(false && "Not all types are implemented yet");
        // TODO vmx 2017-03-03: Handle other cases
        break;
    }

    double bb_double;
    Slice bb_slice;
    RtreeDimensionType bb_type = static_cast<RtreeDimensionType>(*bb.data());
    bb.remove_prefix(sizeof(uint8_t));
    switch(bb_type) {
      case RtreeDimensionType::kDouble:
          bb_double = *reinterpret_cast<const double*>(bb.data());
          bb.remove_prefix(sizeof(double));
        break;
      case RtreeDimensionType::kString:
        GetLengthPrefixedSlice(&bb, &bb_slice);
        break;
      default:
        assert(false && "Not all types are implemented yet");
        // TODO vmx 2017-03-03: Handle other cases
        break;
    }

    // We care about the minimum values only
    if (ii % 2 == 0) {
      if (aa_type == bb_type) {
        switch(aa_type) {
          case RtreeDimensionType::kDouble:
            if(aa_double < bb_double) {
              return -1;
            } else if (aa_double > bb_double) {
              return 1;
            }
            break;
          case RtreeDimensionType::kString:
            if(aa_slice.compare(bb_slice) < 0) {
              return -1;
            } else if (aa_slice.compare(bb_slice) > 0) {
              return 1;
            }
            break;
          default:
            assert(false && "Not all types are implemented yet");
            // TODO vmx 2017-03-03: Handle other cases
            break;
        }
      } else if (aa_type < bb_type) {
        return -1;
      } else if (aa_type > bb_type) {
        return 1;
      }
    }
    // Continue looping if aa min == bb min
  }
  return 0;
}

namespace {
class LowxComparatorImpl : public rocksdb::Comparator {
 public:
  const char* Name() const {
    return RtreeUtil::LowxComparatorName();
  }

  int Compare(const rocksdb::Slice& slice_aa, const rocksdb::Slice& slice_bb) const {
    return RtreeUtil::LowxComparatorCompare(slice_aa, slice_bb);
  }

  void FindShortestSeparator(std::string* start,
      const rocksdb::Slice& limit) const {
    return;
  }

  void FindShortSuccessor(std::string* key) const  {
    return;
  }
};

}// namespace

const Comparator* LowxComparator() {
  static LowxComparatorImpl lowx;
  return &lowx;
}

}  // namespace rocksdb
