//-*- mode: c++ -*-////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    array_utilities.h                                                //
//                                                                           //
// MODULE:  None.                                                            //
//                                                                           //
// PURPOSE: Provides useful functions for working with arrays.               //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
//                             Public Methods:                               //
///////////////////////////////////////////////////////////////////////////////
#ifndef ARRAY_UTILITIES_H
#define ARRAY_UTILITIES_H

#include "PSTPoint.h"

using namespace PrioritySearchTree;

namespace PSTArray {
  void copy(PSTPoint* a, PSTPoint* b, int n);
  void swap(PSTPoint* points,int a, int b);
  void print(PSTPoint* points, int nPoints);
  void print(PSTPoint* points, int begin, int end);
}

#endif
