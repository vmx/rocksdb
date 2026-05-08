// rtree_querydata: parity check for the rocksdb rtree against the brute-force
// `querydata` tool. Reads the same binary data and query files (32-byte rects,
// four little-endian f64s in order x_min, x_max, y_min, y_max), indexes the
// data with the rocksdb rtree, runs each query through the rtree iterator,
// and writes the matching rectangles to a result file in the same byte
// format as querydata.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "util/coding.h"
#include "util/rtree.h"

using namespace rocksdb;

namespace {

constexpr size_t kRectBytes = 32;  // 4 little-endian f64s

struct Rect {
  double x_min;
  double x_max;
  double y_min;
  double y_max;
};

double LoadLeF64(const char* p) {
  uint64_t bits = 0;
  for (int i = 0; i < 8; ++i) {
    bits |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
  }
  double out;
  std::memcpy(&out, &bits, sizeof(out));
  return out;
}

void StoreLeF64(char* p, double v) {
  uint64_t bits;
  std::memcpy(&bits, &v, sizeof(bits));
  for (int i = 0; i < 8; ++i) {
    p[i] = static_cast<char>((bits >> (8 * i)) & 0xFF);
  }
}

std::vector<Rect> ReadRects(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    std::cerr << "failed to open " << path << "\n";
    std::exit(1);
  }
  in.seekg(0, std::ios::end);
  std::streamsize size = in.tellg();
  in.seekg(0, std::ios::beg);
  if (size % kRectBytes != 0) {
    std::cerr << path << ": size " << size << " is not a multiple of "
              << kRectBytes << "\n";
    std::exit(1);
  }
  std::vector<Rect> rects(size / kRectBytes);
  std::vector<char> buf(kRectBytes);
  for (auto& r : rects) {
    in.read(buf.data(), kRectBytes);
    r.x_min = LoadLeF64(buf.data() + 0);
    r.x_max = LoadLeF64(buf.data() + 8);
    r.y_min = LoadLeF64(buf.data() + 16);
    r.y_max = LoadLeF64(buf.data() + 24);
  }
  return rects;
}

// Key layout: varint32(keypath.size()) | keypath | iid (host uint64) |
//             x_min, x_max, y_min, y_max (host doubles).
std::string SerializeKey(const std::string& keypath, uint64_t iid,
                         const Rect& r) {
  std::string key;
  PutVarint32(&key, static_cast<uint32_t>(keypath.size()));
  key.append(keypath);
  key.append(reinterpret_cast<const char*>(&iid), sizeof(uint64_t));
  key.append(reinterpret_cast<const char*>(&r.x_min), sizeof(double));
  key.append(reinterpret_cast<const char*>(&r.x_max), sizeof(double));
  key.append(reinterpret_cast<const char*>(&r.y_min), sizeof(double));
  key.append(reinterpret_cast<const char*>(&r.y_max), sizeof(double));
  return key;
}

// Query layout: varint32(keypath.size()) | keypath | iid_min, iid_max
//               (host uint64) | x_min, x_max, y_min, y_max (host doubles).
std::string SerializeQuery(const std::string& keypath, uint64_t iid_min,
                           uint64_t iid_max, const Rect& q) {
  std::string key;
  PutVarint32(&key, static_cast<uint32_t>(keypath.size()));
  key.append(keypath);
  key.append(reinterpret_cast<const char*>(&iid_min), sizeof(uint64_t));
  key.append(reinterpret_cast<const char*>(&iid_max), sizeof(uint64_t));
  key.append(reinterpret_cast<const char*>(&q.x_min), sizeof(double));
  key.append(reinterpret_cast<const char*>(&q.x_max), sizeof(double));
  key.append(reinterpret_cast<const char*>(&q.y_min), sizeof(double));
  key.append(reinterpret_cast<const char*>(&q.y_max), sizeof(double));
  return key;
}

// Comparator: keypath first (length-prefixed), then a numeric compare on
// the iid stored as a host-order uint64_t. Each key has a unique iid, so
// the further bbox bytes never need to be examined for ordering.
class NoiseComparator : public Comparator {
 public:
  const char* Name() const override { return "rocksdb.NoiseComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    Slice sa(a), sb(b);
    Slice keypath_a, keypath_b;
    GetLengthPrefixedSlice(&sa, &keypath_a);
    GetLengthPrefixedSlice(&sb, &keypath_b);
    int kp = keypath_a.compare(keypath_b);
    if (kp != 0) return kp;
    const uint64_t* va = reinterpret_cast<const uint64_t*>(sa.data());
    const uint64_t* vb = reinterpret_cast<const uint64_t*>(sb.data());
    if (*va < *vb) return -1;
    if (*va > *vb) return 1;
    return 0;
  }

  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string*) const override {}
};

}  // namespace

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cerr << "usage: " << argv[0] << " <data> <query> <result>\n";
    return 2;
  }
  const std::string data_path = argv[1];
  const std::string query_path = argv[2];
  const std::string result_path = argv[3];

  auto data = ReadRects(data_path);
  auto queries = ReadRects(query_path);
  std::cerr << "loaded " << data.size() << " data rects, " << queries.size()
            << " queries\n";

  // Use a fresh DB directory each run — the rtree on-disk format changed
  // recently and reusing a stale dir would silently parse old bytes.
  const std::string db_path = "/tmp/rocksdb_rtree_querydata";
  std::string rm_cmd = "rm -rf '" + db_path + "'";
  if (std::system(rm_cmd.c_str()) != 0) {
    std::cerr << "warning: failed to clear " << db_path << "\n";
  }

  Options options;
  NoiseComparator cmp;
  options.comparator = &cmp;
  options.create_if_missing = true;

  BlockBasedTableOptions block_based_options;
  block_based_options.index_type = BlockBasedTableOptions::kRtreeSearch;
  block_based_options.flush_block_policy_factory.reset(
      new NoiseFlushBlockPolicyFactory());
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
  options.memtable_factory.reset(new SkipListMbbFactory);

  DB* db = nullptr;
  Status s = DB::Open(options, db_path, &db);
  if (!s.ok()) {
    std::cerr << "DB::Open failed: " << s.ToString() << "\n";
    return 1;
  }

  const std::string keypath = "rects";

  WriteOptions wo;
  for (size_t i = 0; i < data.size(); ++i) {
    std::string key = SerializeKey(keypath, static_cast<uint64_t>(i), data[i]);
    s = db->Put(wo, key, Slice());
    if (!s.ok()) {
      std::cerr << "Put failed at i=" << i << ": " << s.ToString() << "\n";
      return 1;
    }
  }
  std::cerr << "inserted " << data.size() << " keys\n";

  // Flush so the rtree SST is built and queries exercise the actual on-disk
  // index code path (not just the memtable).
  s = db->Flush(FlushOptions());
  if (!s.ok()) {
    std::cerr << "Flush failed: " << s.ToString() << "\n";
    return 1;
  }

  std::ofstream out(result_path, std::ios::binary | std::ios::trunc);
  if (!out) {
    std::cerr << "failed to open " << result_path << " for writing\n";
    return 1;
  }

  size_t total_hits = 0;
  RtreeIteratorContext iter_ctx;
  ReadOptions ro;
  ro.iterator_context = &iter_ctx;

  // Brute-force querydata writes hits in (query-index, data-index) order.
  // We mirror that: outer loop over queries, and the iterator returns hits
  // in iid order (the comparator above), which is also data-file order
  // since iid = file index.
  char rect_buf[kRectBytes];
  for (size_t qi = 0; qi < queries.size(); ++qi) {
    iter_ctx.query_mbb = SerializeQuery(
        keypath, 0, std::numeric_limits<uint64_t>::max(), queries[qi]);

    std::unique_ptr<Iterator> it(db->NewIterator(ro));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      Slice key = it->key();
      Slice keypath_slice;
      GetLengthPrefixedSlice(&key, &keypath_slice);
      // Pre-encoding `Mbb::Interval` holds the original `double`s.
      Mbb mbb = ReadKeyMbb(key);
      StoreLeF64(rect_buf + 0, mbb.first.min);
      StoreLeF64(rect_buf + 8, mbb.first.max);
      StoreLeF64(rect_buf + 16, mbb.second.min);
      StoreLeF64(rect_buf + 24, mbb.second.max);
      out.write(rect_buf, kRectBytes);
      ++total_hits;
    }
  }

  out.flush();
  if (!out) {
    std::cerr << "write to " << result_path << " failed\n";
    return 1;
  }

  std::cerr << data.size() << " data rects x " << queries.size()
            << " queries -> " << total_hits << " results\n";

  delete db;
  return 0;
}
