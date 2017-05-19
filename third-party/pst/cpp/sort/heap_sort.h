//-*- mode: c++ -*-////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    heap_sort.h                                                      //
//                                                                           //
// MODULE:  Sort                                                             //
//                                                                           //
// PURPOSE: Provides an implementation of heap sort.                         //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
//                             Public Methods:                               //
///////////////////////////////////////////////////////////////////////////////
#ifndef HEAP_SORT_H
#define HEAP_SORT_H

void heap_sort(PSTPoint* array, int npoints);
void heap_sort(PSTPoint* array, int begin, int end);

#endif
