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
#include <time.h>
#include "PSTPoint.h"
#include "array_utilities.h"
#include "sort/insertion_sort.h"
#include "sort/heap_sort.h"

using namespace std;
using namespace PrioritySearchTree;
using namespace point_sort;

int main(int argv, char** argc) {
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
  for(int i = 0; i < n; i++) {
    PSTPoint p(i, static_cast<coordy_t>(n-i));
    points[i] = p;
  }
  after = time(0);
  cout << "took: " << (after - before) << endl;
  if(n <= 10) {
    cout << "Before sorting: ";
    PSTArray::print(points,n);
  }
  /////////////////////////////////////////////////////////////////////////////
  // Heap Sort                                                               //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Heap Sort..." << flush;
  before = time(0);
  heap_sort(points,n);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  /////////////////////////////////////////////////////////////////////////////
  // Unsort Points                                                           //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Unsorting points..." << flush;
  before = time(0);
  for(int i = 0; i <= n/2; i++) { // unsort the points
    PSTArray::swap(points,i,n-i-1);
  }
  after = time(0);
  cout << "took: " << (after - before) << endl;
  /////////////////////////////////////////////////////////////////////////////
  // Insertion Sort                                                          //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Insertion Sort..." << flush;
  before = time(0);
  insertion_sort(points,n);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  /////////////////////////////////////////////////////////////////////////////
  // Tear Down                                                               //
  /////////////////////////////////////////////////////////////////////////////
  if(n <= 10) {
    cout << "After sorting: ";
    PSTArray::print(points,n);
  }
  return 0;
}
