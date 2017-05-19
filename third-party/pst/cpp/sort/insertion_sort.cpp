///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:       insertion_sort.cpp                                            //
//                                                                           //
// MODULE:     Sort                                                          //
//                                                                           //
// NOTES:      Provides an implementation of insertion sort.                 //
//                                                                           //
// PROPERTIES: - in-place                                                    //
//             - stable                                                      //
//             - worst case time complexity: O(n^2)                          //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////

#include "../PSTPoint.h"
#include "insertion_sort.h"

using namespace PrioritySearchTree;

namespace point_sort {
  void insertion_sort(PSTPoint* array, int npoints) {
    insertion_sort(array,0,npoints-1);
  }
  void insertion_sort(PSTPoint* array, int begin, int end) {
    PSTPoint p;
    int j;
    bool done;
    for(int i = begin+1; i <= end; i++) {
      p = array[i];
      j = i -1;
      done = false;
      while(!done) {
	if(array[j].getX() > p.getX()) {
	  array[j+1] = array[j];
	  j--;
	  if(j < begin)
	    done = true;
	} else
	  done = true;
      }
      array[j+1] = p;
    }
  }
}
