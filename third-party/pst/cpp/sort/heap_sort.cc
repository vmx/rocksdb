///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:       heap_sort.cpp                                                 //
//                                                                           //
// MODULE:     Sort                                                          //
//                                                                           //
// NOTES:      Provides an implementation of heap sort.                      //
//                                                                           //
// PROPERTIES: - in-place                                                    //
//             - unstable                                                    //
//             - worst case time complexity: O(nlogn)                        //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////

#include "../PSTPoint.h"
#include "../array_utilities.h"
#include "heap_sort.h"

int leftChildOf(int index) {
  return 2*index+1;
}
int rightChildOf(int index) {
  return 2*index+2;
}

///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FUNCTION NAME: downHeap                                                   //
//                                                                           //
// PURPOSE:       builds a heap from top down                                //
//                                                                           //
// PARAMETERS                                                                //
//   Type/Name:   PSTPoint*/array                                            //
//   Description: The array of points                                        //
//                                                                           //
//   Type/Name:   int/v                                                      //
//   Description: The offset from begin from which to begin building         //
//                the heap.                                                  //
//                                                                           //
//   Type/Name:   int/begin                                                  //
//   Description: The first element in the array to be sorted.               //
//                                                                           //
//   Type/Name:   int/end                                                    //
//   Description: The last element in the array to be sorted.                //
//                                                                           //
// RETURN:        Void.                                                      //
//                                                                           //
// NOTES:         Note that indices are base zero, i.e. the first            //
//                element has index 0, the second element has index 1...     //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
void downHeap(PSTPoint* array, int v, int begin, int end) {
  int w = leftChildOf(v);
  // invariant: element at index v has a left child
  while(begin + w <= end) {
    // if left child has a sibling
    if(begin + w+1 <= end)
      // if right child is greater than left child
      if(array[begin + w+1] > array[begin + w])
        // use right child
        w++;
    // if larger child is less than its parent
    if(!(array[begin + w] > array[begin + v]))
      return;
    // otherwise, swap child and parent
PSTArray::swap(array,begin + w,begin + v);
    // continue with child
    v = w;
    w = leftChildOf(v);
  }
}

// builds a heap from bottom up, starting at right-most lowest level
// and traversing up the heap in reverse order (right to left, bottom to top)
void buildHeap(PSTPoint* array, int begin, int end) {
  int n = 1 + end - begin;
  for(int v = n/2-1; v >= 0; v--)
    downHeap(array,v,begin,end);
}

void heap_sort(PSTPoint* array, int npoints) {
  heap_sort(array,0,npoints-1);
}

void heap_sort(PSTPoint* array, int begin, int end) {
  // First arrange the array into a heap (root element is always higher
  // than both child elements
  buildHeap(array,begin,end);
  while(end > begin) {
    // Since the highest is first, move it to the end
    PSTArray::swap(array,begin,end);
    // Now that the highest element is last, it is sorted so don't
    // touch it again
    end--;
    // Since the smallest element is now first, rebuild the heap
    downHeap(array,0,begin,end);
  }
}
