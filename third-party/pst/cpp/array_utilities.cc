///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    array_utilities.cpp                                              //
//                                                                           //
// MODULE:  Priority Search Tree                                             //
//                                                                           //
// NOTES:   Provides useful functions for working with arrays.               //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
#include <algorithm>
#include <iostream>
#include "PSTPoint.h"
#include "array_utilities.h"

using namespace std;

namespace PSTArray {
  void print(PSTPoint* points, int nPoints) {
    print(points,0,nPoints-1);
  }
  void print(PSTPoint* points, int begin, int end) {
    cout << "{ " << points[begin];
    for(int i = begin+1; i <= end; i++)
      cout << " " << points[i];
    cout << " }" << endl;
  }
  void swap(PSTPoint* points, int a, int b) {
    //PSTPoint temp = points[a];
    //points[a] = points[b];
    //points[b] = temp;
    std::swap(points[a], points[b]);
  }
  // copies from a to b
  void copy(PSTPoint* a, PSTPoint* b, int n) {
    while(--n >= 0) b[n] = a[n];
  }
}
