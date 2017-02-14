# rtree-table fork of RocksDB

## Overview

This is an experimental fork of RocksDB which implements and R-tree. This
enables you to do multi-dimensional queries. At the moment only bounding
boxes with floating point numbers (doubles) are accepted as input.


### Current state

The code isn't heavily tested or optimised yet, its release follows the
“release early, release often” paradigm. Pull requests are welcome.


### Goals

The goal is to make this fork obsolete and make the code work with a vanilla
RocksDB. Until this is achieved, I'll try to keep the code changes to the
original source as minimal as possible ([diff of all my changes][1]).


### Implementation

The current implementation consists of a custom Table format implementing
the R-tree and custom Memtable that is based on the SkipList based one.

The R-tree is ordered by the low value of the first dimension. This minimises
the changes that need to be done on RocksDB as it expects a total order of
the keys. This also leads to the nice property of having the results always
sorted the same way. The idea is based on the paper [On Support of Ordering in
Multidimensional Data Structures by Filip Křižka, Michal Krátký, Radim Bača][2].

The Memtable is just RocksDB's default SkipList where there not matching
bounding boxes are filtered out dynamically on query time.


### The API

No special API is needed, the existing RocksDB API is re-used.


#### Setup

The include files that are needed for the API section are:

    #include <iostream>

    #include "db/memtable.h"
    #include "rocksdb/db.h"
    #include "rocksdb/table.h"
    #include "table/rtree_table_util.h"

For using the R-tree you need to define the custom Table, Memtable and
comparator:

    rocksdb::Options options;
    rocksdb::RtreeTableOptions table_options;
    table_options.dimensions = 2;
    options.table_factory.reset(rocksdb::NewRtreeTableFactory(table_options));
    options.memtable_factory.reset(new rocksdb::SkipListMbbFactory);
    options.comparator = rocksdb::LowxComparator();

#### Adding data

Opening a database is just the same as with a vanilla RocksDB:

    rocksdb::DB* raw_db;
    std::unique_ptr<rocksdb::DB> db;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/rtreetable", &raw_db);
    db.reset(raw_db);

The data that is inserted are multi-dimensional bounding boxes. This means
you have a low and a high value for every dimension. Currently only doubles
are supported. Your code may look like this:

    std::vector<double> augsburg_key = {10.75, 11.11, 48.24, 48.50};
    rocksdb::Slice augsburg_slice = rocksdb::Slice(reinterpret_cast<const char*>(
        augsburg_key.data()), sizeof(augsburg_key[0]) * augsburg_key.size());
    status = raw_db->Put(rocksdb::WriteOptions(), augsburg_slice, "augsburg");
    std::vector<double> alameda_key = {-122.34, -122.22, 37.71, 37.80};
    rocksdb::Slice alameda_slice = rocksdb::Slice(reinterpret_cast<const char*>(
        alameda_key.data()), sizeof(alameda_key[0]) * alameda_key.size());
    status = db->Put(rocksdb::WriteOptions(), alameda_slice, "alameda");

#### Querying the data

Creating and iterator is the same as with a vanilla RocksDB:

    std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));

The bounding box you want to query on is specified by `Seek()`:

    std::vector<double> query = {10, 11, 48, 49};
    rocksdb::Slice query_slice = rocksdb::Slice(reinterpret_cast<const char*>(
        query.data()), sizeof(query[0]) * query.size());
    it->Seek(query_slice);

Iterating over the results is again the same as the vanilla RocksDB. This
prints out the values of the bounding boxes that intersect with the window
query as specified by `Seek()`.

    for (; it->Valid(); it->Next()) {
        std::cout << it->value().ToString() << std::endl;
    }


#### Full example

A full example can be found in
[examples/rtree_example.cc][3]. You can build it with

    make static_lib
    cd examples
    make rtree_example

The executable is located in the exxamples directory.


[1]: https://github.com/vmx/rocksdb/compare/master...vmx:rtree-table?diff=split&name=rtree-table#files_bucket
[2]: http://ceur-ws.org/Vol-639/165-krizka.pdf
[3]: examples/rtree_example.cc


# The original Readme

## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![Build Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)
[![Build status](https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/master?svg=true)](https://ci.appveyor.com/project/Facebook/rocksdb/branch/master)


RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on LevelDB by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it specially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/
