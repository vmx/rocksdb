//-*- mode: c++ -*-////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    insertion_sort.h                                                 //
//                                                                           //
// MODULE:  Sort                                                             //
//                                                                           //
// PURPOSE: Provides an implementation of insertion sort.                    //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
//                             Public Methods:                               //
//                                                                           //
//   Type/Name:   void/insertion_sort                                        //
//   Description: Sorts a given array.                                       //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
#ifndef INSERTION_SORT_H
#define INSERTION_SORT_H

#include "../PSTPoint.h"

using namespace PrioritySearchTree;

namespace point_sort {
  void insertion_sort(PSTPoint* array, int npoints);
  void insertion_sort(PSTPoint* array, int begin, int end);
}

#endif
