///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    test_sort.cpp                                                    //
//                                                                           //
// MODULE:  Sort                                                             //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
#include <iostream>
#include <cmath>
#include <time.h>
#include "PSTPoint.h"
#include "array_utilities.h"
#include "sort/heap_sort.h"
#include "sort/stable_01_sort.h"

using namespace std;

void unsort(PSTPoint* array, int n) {
  heap_sort(array,n);
  for(int i = 1; i < n/2; i += 2)
    PSTArray::swap(array, i, n-i);
}

int main(int argv, char** argc) {
  const int maxPointsToPrint = 20;
  time_t before, after;
  /////////////////////////////////////////////////////////////////////////////
  // Setup                                                                   //
  /////////////////////////////////////////////////////////////////////////////
  int n = 1000;
  if(argv >= 2) {
    n = atoi(argc[1]);
  }
  cout << "Creating " << n << " points..." << flush;
  before = time(0);
  PSTPoint* points = new PSTPoint[n];
  PSTPoint s;
  for(int i = 0; i < n; i++) {
    PSTPoint p(i,static_cast<coordy_t>(n-i));
    if(i == n/2)
      s = p;
    points[i] = p;
  }
  after = time(0);
  cout << "took: " << (after - before) << endl;
  /////////////////////////////////////////////////////////////////////////////
  // Unsort Points                                                           //
  /////////////////////////////////////////////////////////////////////////////
  cout << "--------------------------------------Unsorting points..." << flush;
  before = time(0);
  unsort(points,n);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  if(n <= maxPointsToPrint) {
    cout << "Before sorting: ";
    PSTArray::print(points,n);
    cout << "Using point: " << s << endl;
  }
  /////////////////////////////////////////////////////////////////////////////
  // Algorithm A                                                             //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Algorithm A..." << flush;
  before = time(0);
  algorithm_A(points,0,n-1,s);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  if(n <= maxPointsToPrint) {
    cout << "After sorting: ";
    PSTArray::print(points,n);
  }
  if(n == (pow(2,log2(n)))) {
    /////////////////////////////////////////////////////////////////////////////
    // Unsort Points                                                           //
    /////////////////////////////////////////////////////////////////////////////
    cout << "--------------------------------------Unsorting points..." << flush;
    before = time(0);
    unsort(points,n);
    after = time(0);
    cout << "took: " << (after - before) << endl;
    if(n <= maxPointsToPrint) {
      cout << "Before sorting: ";
      PSTArray::print(points,n);
    }
    /////////////////////////////////////////////////////////////////////////////
    // Algorithm B                                                             //
    /////////////////////////////////////////////////////////////////////////////
    cout << "Algorithm B..." << flush;
    before = time(0);
    algorithm_B(points,0,n-1,s);
    after = time(0);
    cout << "took: " << (after - before) << endl;
    if(n <= maxPointsToPrint) {
      cout << "After sorting: ";
      PSTArray::print(points,n);
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  // Unsort Points                                                           //
  /////////////////////////////////////////////////////////////////////////////
  cout << "--------------------------------------Unsorting points..." << flush;
  before = time(0);
  unsort(points,n);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  if(n <= maxPointsToPrint) {
    cout << "Before sorting: ";
    PSTArray::print(points,n);
  }
  /////////////////////////////////////////////////////////////////////////////
  // Simple Partition                                                        //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Simple partition..." << flush;
  before = time(0);
  simple_partition(points,0,n-1,s);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  if(n <= maxPointsToPrint) {
    cout << "After sorting: ";
    PSTArray::print(points,n);
  }
  return 0;
}
