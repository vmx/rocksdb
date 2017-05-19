///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    stable_01_sort.h                                                 //
//                                                                           //
// MODULE:  Sort                                                             //
//                                                                           //
// PURPOSE: Provides an implementation of Katajainen and Pasanen's           //
//          stable 0,1 sort.                                                 //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
//                             Public Methods:                               //
///////////////////////////////////////////////////////////////////////////////
#ifndef STABLE____SORT_H
#define STABLE____SORT_H

int smallestPowerOf2GreaterThanLog2Of(int x);
int simple_partition(PSTPoint* array, int begin, int end, const PSTPoint& s);
void algorithm_A(PSTPoint* array, int begin, int end, const PSTPoint& s);
void algorithm_B(PSTPoint* array, int begin, int end, const PSTPoint& s);
void stable_01_sort(PSTPoint* array, int npoints, const PSTPoint& s);
void stable_01_sort(PSTPoint* array, int begin, int end, const PSTPoint& s);

#endif
