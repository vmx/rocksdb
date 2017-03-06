//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/options.h"

namespace rocksdb {

const size_t kMinInternalKeySize = 8;


// A variant type is needed. Until C++17 compilers are the norm (where
// std::variant is a thing), use a custom class for it.
// This code is based on the spatial_db.h Variant
struct Variant {
  // Don't change the values here, they are persisted on disk
  enum Type: uint8_t {
    kNull = 0x0,
    kBool = 0x1,
    kInt = 0x2,
    kDouble = 0x3,
    kString = 0x4,
  };

  Variant() : type_(kNull) {}
  /* implicit */ Variant(bool b) : type_(kBool) { data_.b = b; }
  /* implicit */ Variant(uint64_t i) : type_(kInt) { data_.i = i; }
  /* implicit */ Variant(double d) : type_(kDouble) { data_.d = d; }
  /* implicit */ Variant(const std::string& s) : type_(kString) {
    new (&data_.s) std::string(s);
  }

  Variant(const Variant& v) : type_(v.type_) { Init(v, data_); }

  Variant& operator=(const Variant& v);

  Variant(Variant&& rhs) : type_(kNull) { *this = std::move(rhs); }

  Variant& operator=(Variant&& v);

  ~Variant() { Destroy(type_, data_); }

  Type type() const { return type_; }
  bool get_bool() const { return data_.b; }
  uint64_t get_int() const { return data_.i; }
  double get_double() const { return data_.d; }
  const std::string& get_string() const { return *GetStringPtr(data_); }

  bool operator==(const Variant& other) const;
  bool operator!=(const Variant& other) const { return !(*this == other); }
  // TODO vmx 2017-03-03: Implement less than and greater than properly, currently it's just something working without much thought
  bool operator<(const Variant& other) const;
  bool operator>(const Variant& other) const;

 private:
  Type type_;

  union Data {
    bool b;
    uint64_t i;
    double d;
    // Current version of MS compiler not C++11 compliant so can not put
    // std::string
    // however, even then we still need the rest of the maintenance.
    char s[sizeof(std::string)];
  } data_;
  // Avoid type_punned aliasing problem
  static std::string* GetStringPtr(Data& d) {
    void* p = d.s;
    return reinterpret_cast<std::string*>(p);
  }

  static const std::string* GetStringPtr(const Data& d) {
    const void* p = d.s;
    return reinterpret_cast<const std::string*>(p);
  }

  static void Init(const Variant&, Data&);

  static void Destroy(Type t, Data& d) {
    if (t == kString) {
      using std::string;
      GetStringPtr(d)->~string();
    }
  }
};


class RtreeUtil {
 public:
  // Encodes a given bounding box as `InternalKey`
  static std::string EncodeKey(std::vector<std::pair<Variant, Variant>>& mbb);
  // Return the enclosing bounds of two multi-dimensional bounding boxes
  static std::vector<std::pair<Variant, Variant>> EnclosingMbb(
      const std::vector<std::pair<Variant, Variant>> aa,
      const std::vector<std::pair<Variant, Variant>> bb);
  static std::vector<double> EnclosingMbb(const double* aa,
                                          const double* bb,
                                          uint8_t dimensions);
  // Return true if the two given bounding boxes intersect or one isn't defined
  static bool IntersectMbb(
      const std::vector<std::pair<Variant, Variant>> aa,
      const std::vector<std::pair<Variant, Variant>> bb);
  // Return true if the two given bounding boxes intersect or one isn't defined
  // Convinience method to make the caller code easier to read
  static bool IntersectMbb(const double* aa,
                           const std::string& bb,
                           uint8_t dimensions);
  // Return true if the two given bounding boxes intersect or one isn't defined
  static bool IntersectMbb(const double* aa,
                           const double* bb,
                           uint8_t dimensions);

  // These comparator name and compare function are used for the C and C++
  // based comparator.
  static const char* LowxComparatorName() { return "rocksdb.LowxComparator"; };
  static int LowxComparatorCompare(const double* aa,
                                   const double* bb,
                                   uint8_t dimensions);

  // Serialize the Types of a vector of Variants
  static const std::string SerializeTypes(const std::vector<Variant>& types);
  // Deserialize the Types of a serialized vectoe of Variants
  static const std::vector<Variant::Type> DeserializeTypes(
      const std::string serialized);
  // Deserialize a key (Slice) into a vector of Variants
  static const std::vector<std::pair<Variant, Variant>> DeserializeKey(
      const std::vector<Variant::Type> types,
      const Slice& key);
private:
  // It's not allowed to create an instance of `RtreeUtil`
  RtreeUtil() {}
};

// A comparator that sorts the multi-dimensional bounding boxes by the
// lower value of the first dimension.
extern const Comparator* LowxComparator();

struct RtreeTableIteratorContext: public IteratorContext {
  std::vector<std::pair<Variant, Variant>> query_mbb;
  RtreeTableIteratorContext(): query_mbb() {}
};

}  // namespace rocksdb
