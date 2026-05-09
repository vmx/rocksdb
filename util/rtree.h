//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Utility functions needed for the R-tree

#pragma once
#include <cstdint>
#include <cstring>
#include <ostream>

#include "rocksdb/options.h"

namespace rocksdb {

// NOTE vmx 2017-07-29: It must be a multiple of the internal key size,
// which is at the moment 48 bytes
static const size_t kRtreeInnerNodeSize = 1056;

// Decode the byte-orderable IEEE-754 encoding produced by the noise side. The
// encoder flips the sign bit on positive values and flips every bit on
// negatives, then writes big-endian. The encoded uint64_t (in host byte
// order) compares the same way as the original double, so the rtree code
// stores encoded values directly and only round-trips back to double for
// debug output.
inline double DecodeByteOrderableF64(uint64_t encoded) {
  uint64_t bits;
  if ((encoded >> 63) == 1) {
    bits = encoded ^ 0x8000000000000000ull;
  } else {
    bits = ~encoded;
  }
  double out;
  std::memcpy(&out, &bits, sizeof(out));
  return out;
}

// Interval over an integer-valued dimension (the iid). Both endpoints are
// stored as a plain uint64_t — the byte-orderable form for an integer is
// just the integer itself, so no decoding is needed for compares or display.
struct IntInterval {
  uint64_t min;
  uint64_t max;

  friend std::ostream& operator<<(std::ostream& os,
                                  const IntInterval& interval) {
    return os << "[" << interval.min << "," << interval.max << "]";
  };
};

// Interval over a float-valued dimension (a bbox coordinate). Stored as the
// byte-orderable encoded representation (uint64_t in host order); compares
// with `<` / `>` give the same result as the original `double` comparisons.
// Decoding back to `double` is only needed for human-readable output.
struct Interval {
  uint64_t min;
  uint64_t max;

  friend std::ostream& operator<<(std::ostream& os, const Interval& interval) {
    return os << "[" << DecodeByteOrderableF64(interval.min) << ","
              << DecodeByteOrderableF64(interval.max) << "]";
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

// `Mbb` stores all dimensions in the byte-orderable encoded form. Numeric
// comparisons on the uint64_t fields are equivalent to numeric comparisons on
// the original values, so the rtree filter logic stays unchanged.
//
// An empty Mbb is encoded as an inverted iid range — the same sentinel for
// "default-constructed" and "after `clear()`". A real value always has
// `min <= max`, so there's no ambiguity.
struct Mbb {
  IntInterval iid{UINT64_MAX, 0};
  Interval first{0, 0};
  Interval second{0, 0};

  bool empty() const { return iid.min > iid.max; }
  void clear() { iid = {UINT64_MAX, 0}; }

  // It's 3 dimensions with 64-bit min and max values
  size_t size() const {
    return 48;
  }

  friend std::ostream& operator<<(std::ostream& os, const Mbb& mbb) {
    return os << "[" << mbb.iid << "," << mbb.first << "," << mbb.second << "]";
  };
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

// Read 8 big-endian bytes into a host-order uint64_t. Used to ingest the
// byte-orderable encoded fields from key/query bytes; for floats the result
// is the encoded representation, which compares the same way as the original
// double under uint64_t arithmetic.
inline uint64_t ReadBeU64(const char* data) {
  uint64_t v = 0;
  for (size_t i = 0; i < 8; ++i) {
    v = (v << 8) | static_cast<uint8_t>(data[i]);
  }
  return v;
}

// Append 8 big-endian bytes for the given host-order uint64_t.
inline void AppendBeU64(std::string* dest, uint64_t v) {
  char buf[8];
  for (int i = 7; i >= 0; --i) {
    buf[i] = static_cast<char>(v & 0xFF);
    v >>= 8;
  }
  dest->append(buf, 8);
}

// Encode a double into the byte-orderable form so it can be stored as a
// `Mbb` field.
inline uint64_t EncodeByteOrderableF64(double value) {
  uint64_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  if ((bits >> 63) == 0) {
    return bits ^ 0x8000000000000000ull;
  } else {
    return ~bits;
  }
}

// UTF-8-style prefix varint (Cassandra ByteComparable BIGINT, unsigned
// case). The number of leading 1-bits in byte 0 names the total encoded
// length, the rest of byte 0 plus the trailing bytes (big-endian) carry
// the value:
//   0xxxxxxx                : 1 byte;  value = b0           (0..127)
//   10xxxxxx + 1  byte      : 2 bytes; 14 value bits        (..2^14-1)
//   110xxxxx + 2  bytes     : 3 bytes; 21 value bits        (..2^21-1)
//   1110xxxx + 3  bytes     : 4 bytes; 28 value bits        (..2^28-1)
//   11110xxx + 4  bytes     : 5 bytes; 35 value bits avail. (..2^32-1)
// (uint32_t fits in at most 5 bytes; longer encodings would overflow.)
//
// memcmp on the encoded bytes preserves numeric order: longer encodings
// have more leading 1-bits in byte 0 and so always sort above shorter
// ones; within a length class the trailing bytes are big-endian.
inline void PutPrefixVarint32(std::string* dst, uint32_t value) {
  if (value < (1u << 7)) {
    dst->push_back(static_cast<char>(value));
  } else if (value < (1u << 14)) {
    dst->push_back(static_cast<char>(0x80 | (value >> 8)));
    dst->push_back(static_cast<char>(value & 0xFF));
  } else if (value < (1u << 21)) {
    dst->push_back(static_cast<char>(0xC0 | (value >> 16)));
    dst->push_back(static_cast<char>((value >> 8) & 0xFF));
    dst->push_back(static_cast<char>(value & 0xFF));
  } else if (value < (1u << 28)) {
    dst->push_back(static_cast<char>(0xE0 | (value >> 24)));
    dst->push_back(static_cast<char>((value >> 16) & 0xFF));
    dst->push_back(static_cast<char>((value >> 8) & 0xFF));
    dst->push_back(static_cast<char>(value & 0xFF));
  } else {
    // 5-byte encoding: 11110xxx with the 3 length-payload bits all 0 for
    // uint32_t — the 32 value bits live entirely in the trailing 4 bytes.
    dst->push_back(static_cast<char>(0xF0));
    dst->push_back(static_cast<char>((value >> 24) & 0xFF));
    dst->push_back(static_cast<char>((value >> 16) & 0xFF));
    dst->push_back(static_cast<char>((value >> 8) & 0xFF));
    dst->push_back(static_cast<char>(value & 0xFF));
  }
}

// Decode a `PutPrefixVarint32` value, advancing `input` past the consumed
// bytes. Returns false on truncation or if the leading byte selects an
// encoding wider than uint32_t.
inline bool GetPrefixVarint32(Slice* input, uint32_t* value) {
  if (input->size() < 1) return false;
  const uint8_t b0 = static_cast<uint8_t>(input->data()[0]);
  if ((b0 & 0x80) == 0) {  // 0xxxxxxx
    *value = b0;
    input->remove_prefix(1);
    return true;
  }
  if ((b0 & 0xC0) == 0x80) {  // 10xxxxxx
    if (input->size() < 2) return false;
    *value = (static_cast<uint32_t>(b0 & 0x3F) << 8) |
             static_cast<uint8_t>(input->data()[1]);
    input->remove_prefix(2);
    return true;
  }
  if ((b0 & 0xE0) == 0xC0) {  // 110xxxxx
    if (input->size() < 3) return false;
    *value =
        (static_cast<uint32_t>(b0 & 0x1F) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(input->data()[1])) << 8) |
        static_cast<uint8_t>(input->data()[2]);
    input->remove_prefix(3);
    return true;
  }
  if ((b0 & 0xF0) == 0xE0) {  // 1110xxxx
    if (input->size() < 4) return false;
    *value =
        (static_cast<uint32_t>(b0 & 0x0F) << 24) |
        (static_cast<uint32_t>(static_cast<uint8_t>(input->data()[1])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(input->data()[2])) << 8) |
        static_cast<uint8_t>(input->data()[3]);
    input->remove_prefix(4);
    return true;
  }
  if ((b0 & 0xF8) == 0xF0) {  // 11110xxx
    if (input->size() < 5) return false;
    // The 3 length-payload bits would carry value bits 33..35; for
    // uint32_t they must be 0. Anything else overflows.
    if ((b0 & 0x07) != 0) return false;
    *value =
        (static_cast<uint32_t>(static_cast<uint8_t>(input->data()[1])) << 24) |
        (static_cast<uint32_t>(static_cast<uint8_t>(input->data()[2])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(input->data()[3])) << 8) |
        static_cast<uint8_t>(input->data()[4]);
    input->remove_prefix(5);
    return true;
  }
  return false;  // 11111xxx — encoding too long for uint32_t
}

inline void PutPrefixLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutPrefixVarint32(dst, static_cast<uint32_t>(value.size()));
  dst->append(value.data(), value.size());
}

inline bool GetPrefixLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len = 0;
  if (!GetPrefixVarint32(input, &len)) return false;
  if (input->size() < len) return false;
  *result = Slice(input->data(), len);
  input->remove_prefix(len);
  return true;
}

}  // namespace rocksdb
