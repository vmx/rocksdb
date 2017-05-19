///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    stable_01_sort.cpp                                               //
//                                                                           //
// MODULE:  Sort                                                             //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
#include <iostream>
#include <cmath>
#include <vector>
#include "../PSTPoint.h"
#include "../array_utilities.h"
#include "stable_01_sort.h"

using namespace std;

///////////////////////////////////////////////////////////////////////////////
// Utility Functions                                                         //
///////////////////////////////////////////////////////////////////////////////

bool isZero(const PSTPoint& p, const PSTPoint& s) {
  return p.getX() < s.getX();
}

bool isOne(const PSTPoint& p, const PSTPoint& s) {
  return !isZero(p,s);
}

void reverse(PSTPoint* array, int begin, int end) {
  while(begin < end)
    PSTArray::swap(array,begin++,end--);
}

void block_permute(PSTPoint* array, int beginA, int endA, int endB) {
  if(beginA == endB) return;
  reverse(array,beginA,endA);
  reverse(array,endA +1, endB);
  reverse(array,beginA,endB);
}

///////////////////////////////////////////////////////////////////////////////
// Simple Partition                                                          //
///////////////////////////////////////////////////////////////////////////////

// returns the position of the last zero
int simple_partition(PSTPoint* array, int begin, int end, const PSTPoint& s) {
  int n = 1 + end - begin;
  // Count the zeroes
  int countZeroes = 0;
  for(int i = begin; i <= end; i++)
    if(isZero(array[i],s))
      countZeroes++;
  if(n <= 1) return begin+countZeroes-1;
  // now do the partitioning
  int zeroEnd = end;
  int oneEnd;
  do {
    // find the preceding zero
    while(zeroEnd > begin && isOne(array[zeroEnd],s)) zeroEnd--;
    if(zeroEnd <= begin) return begin+countZeroes-1;
    // find the zero immediately following the preceding one
    oneEnd = zeroEnd-1;
    while(oneEnd > begin && isZero(array[oneEnd],s)) oneEnd--;
    if(oneEnd < begin) return begin+countZeroes-1;
    // find the one immediately following the preceding zero
    int oneStart = oneEnd;
    while(oneStart > begin && isOne(array[oneStart-1],s)) oneStart--;
    if(isZero(array[oneStart],s)) return begin+countZeroes-1;
    // finally, permute the block of zeroes with the block of ones
    block_permute(array,oneStart,oneEnd,zeroEnd);
  } while((zeroEnd-oneEnd) < countZeroes);
  return begin+countZeroes-1;
}

///////////////////////////////////////////////////////////////////////////////
// Algorithm A                                                               //
//                                                                           //
// Note: uses O(n) extra space to store rank and                             //
//       O(n) extra bits to store in position flags and                      //
//       O(1) extra space to store counters                                  //
///////////////////////////////////////////////////////////////////////////////

void algorithm_A(PSTPoint* array, int begin, int end, const PSTPoint& s) {
  int n = 1 + end - begin;
  if(n == 1) return;
  int zeroesTotal = 0;
  for(int i = begin; i <= end; i++)
    if(isZero(array[i],s))
      zeroesTotal++;
  int c0 = 0, c1 = 0;
  int* rank = new int[n];
  bool* inPlace = new bool[n];
  for(int i = 0; i < n; i++) {
    if(isZero(array[begin + i],s)) {
      rank[i] = c0;
      c0++;
    } else { // is one
      rank[i] = zeroesTotal + c1;
      c1++;
    }
    if(i == rank[i]) inPlace[i] = true;
    else inPlace[i] = false;
  }
  // permute elements to their final positions
  for(int i = 0; i < n; i++) {
    while(!inPlace[i]) {
      PSTArray::swap(array,begin + i,begin + rank[i]);
      inPlace[rank[i]] = true;
      rank[i] = rank[rank[i]];
      if(i == rank[i]) inPlace[i] = true;
    }
  }
  delete rank;
  delete inPlace;
}

///////////////////////////////////////////////////////////////////////////////
// Algorithm B                                                               //
///////////////////////////////////////////////////////////////////////////////

int smallestPowerOf2GreaterThanLog2Of(int x) {
  return pow(2,ceil(log2(ceil(log2(x)))));
}

void flip(PSTPoint* array, int index, int blockSize) {
  PSTArray::swap(array,index,index+blockSize);
}

void markZero(PSTPoint* array, int index, int blockSize, const PSTPoint& s) {
  if(isZero(array[index],s)) return; // already zero
  flip(array,index,blockSize);
}

void markOne(PSTPoint* array, int index, int blockSize, const PSTPoint& s) {
  if(isOne(array[index],s)) return; // already one
  flip(array,index,blockSize);
}

void form_buffer(PSTPoint* array, int begin, int blockSize,
		 int indexOfLastBufferZero, int indexOfLastBufferOne,
		 const PSTPoint& s) {
  // form zero buffer
  int lastZero = simple_partition(array,begin,indexOfLastBufferZero,s);
  /////////////////////////////////////////////////////////////////////////////
  // Now array looks like:                                                   //
  //                                                                         //
  // [0|0|...|0|*|*|*|...                                                    //
  // \_________|                                                             //
  //  blocksize                                                              //
  /////////////////////////////////////////////////////////////////////////////

  // form one buffer
  lastZero = simple_partition(array,lastZero+1,indexOfLastBufferOne,s);
  /////////////////////////////////////////////////////////////////////////////
  // Now array looks like:                                                   //
  //                                                                         //
  // [0|0|...|0|0|...|0|1|1|...|1|*|...                                      //
  // \_________|       |_________/                                           //
  //  blocksize         blocksize                                            //
  /////////////////////////////////////////////////////////////////////////////

  // permute block of 1's to abut blocksize-th 0
  if(lastZero >= blockSize)
    block_permute(array,blockSize,lastZero,indexOfLastBufferOne);
  /////////////////////////////////////////////////////////////////////////////
  // Now array looks like:                                                   //
  //                                                                         //
  // [0|0|...|0|1|1|...|1|*|...                                              //
  // \_________|_________/                                                   //
  //  blocksize blocksize                                                    //
  /////////////////////////////////////////////////////////////////////////////
}

void algorithm_B(PSTPoint* array, int begin, int end, const PSTPoint& s) {
  int n = 1 + end - begin;
  // ASSUME for now that n is a power of 2

  /////////////////////////////////////////////////////////////////////////////
  // STEP 1: Form internal buffer                                            //
  /////////////////////////////////////////////////////////////////////////////
  // Determine the positions of last buffer zero and last buffer one
  int blockSize = smallestPowerOf2GreaterThanLog2Of(n);
  int countZeroes = 0, indexOfLastBufferZero = 0,
    countOnes = 0, indexOfLastBufferOne = 0;
  for(int i = 0;
      begin+i <= end && (countZeroes < blockSize || countOnes < blockSize);
      i++) {
    if(isZero(array[begin+i],s) && countZeroes < blockSize) {
      indexOfLastBufferZero = begin+i;
      countZeroes++;
    } else if(isOne(array[begin+i],s) && countOnes < blockSize) {
      indexOfLastBufferOne = begin+i;
      countOnes++;
    }
  }
  // if we don't have enough ones or zeroes to form a buffer, fall back on
  // simple partitioning which works well enough in this case
  if(countZeroes < blockSize || countOnes < blockSize) {
    simple_partition(array,begin,end,s);
    return;
  }
  // otherwise, we can continue
  form_buffer(array,begin,blockSize,indexOfLastBufferZero,indexOfLastBufferOne,s);
  /////////////////////////////////////////////////////////////////////////////
  // Step 2: Sort elements within each block                                 //
  /////////////////////////////////////////////////////////////////////////////
  for(int block = 2; block*blockSize < end; block++) {
    // calculate block boundaries
    int blBegin = begin + block*blockSize;
    int blEnd   = begin + (block+1)*blockSize - 1;
    // determine and mark whether elements are in their final position
    int zeroesTotal = 0;
    for(int i = 0; blBegin + i <= blEnd; i++)
      if(isZero(array[blBegin+i],s))
	zeroesTotal++;
    int c0 = 0, c1 = 0, rank;
    for(int i = 0; blBegin + i <= blEnd; i++) {
      if(isZero(array[blBegin+i],s)) {
	rank = c0;
	c0++;
      } else {
	rank = zeroesTotal + c1;
	c1++;
      }
      if(rank == i)
	markOne(array,i,blockSize,s);
      else
	markZero(array,i,blockSize,s);
    }
    // actually sort elements
    for(int k = 0; blBegin + k <= blEnd; k++) {
      if(isZero(array[k],s)) {
	int i = k;
	do {
	  int l = 0;
	  if(isOne(array[k+blBegin],s)) l = c0;
	  int q = 0;
	  for(int j = 0; j < i-1; j++) {
	    if(j != k && isZero(array[j],s) &&
	       isOne(array[blBegin+j],s) == isOne(array[blBegin+k],s))
	      q++;
	  }
	  int ito = l;
	  bool done = false;
	  do {
	    while(isOne(array[ito],s)) ito++;
	    if(q-- > 0) ito++;
	    else done = true;
	  } while(!done);
	  PSTArray::swap(array,blBegin+ito,blBegin+k);
	  markOne(array,ito,blockSize,s);
	  i = ito;
	} while(i != k);
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  // Step 3: transform to typed blocks                                       //
  /////////////////////////////////////////////////////////////////////////////
  for(int start = 2*blockSize;
      start < end;
      start += blockSize) {
    int i = start;
    // find the blocksize'th zero     
    int nZero = 0;
    bool prevElementWasZero = true;
    while(nZero < blockSize && ++i <= end) {
      if(isZero(array[i],s)) {
	nZero++;
	prevElementWasZero = true;
      }
    }
    // if zeroes not contiguous, partition them
    if(i != start+blockSize)
      simple_partition(array,start,i,s);
  }
  
  /////////////////////////////////////////////////////////////////////////////
  // Step 4: rearrange blocks                                                //
  /////////////////////////////////////////////////////////////////////////////
  // determine and mark whether blocks are in their final position
  // int zeroesTotal = 0;
  // for(int i = 0; blBegin + i <= blEnd; i++)
  //   if(isZero(array[blBegin+i],s))
  //     zeroesTotal++;
  // int c0 = 0, c1 = 0, rank;
  // for(int i = 0; blBegin + i <= blEnd; i++) {
  //   if(isZero(array[blBegin+i],s)) {
  //     rank = c0;
  //     c0++;
  //   } else {
  //     rank = zeroesTotal + c1;
  //     c1++;
  //   }
  //   if(rank == i)
  //     markOne(array,i,blockSize,s);
  //   else
  //     markZero(array,i,blockSize,s);
  // }
  // // actually sort elements
  // for(int k = 0; blBegin + k <= blEnd; k++) {
  //   if(isZero(array[k],s)) {
  //     int i = k;
  //     do {
  // 	int l = 0;
  // 	if(isOne(array[k+blBegin],s)) l = c0;
  // 	int q = 0;
  // 	for(int j = 0; j < i-1; j++) {
  // 	  if(j != k && isZero(array[j],s) &&
  // 	     isOne(array[blBegin+j],s) == isOne(array[blBegin+k],s))
  // 	    q++;
  // 	}
  // 	int ito = l;
  // 	bool done = false;
  // 	do {
  // 	  while(isOne(array[ito],s)) ito++;
  // 	  if(q-- > 0) ito++;
  // 	  else done = true;
  // 	} while(!done);
  // 	PSTArray::swap(array,blBegin+ito,blBegin+k);
  // 	markOne(array,ito,blockSize,s);
  // 	i = ito;
  //     } while(i != k);
  //   }
  // }
  
  
  /////////////////////////////////////////////////////////////////////////////
  // Step 5: clean up any remaining zeroes in the last block                 //
  /////////////////////////////////////////////////////////////////////////////
  
}

///////////////////////////////////////////////////////////////////////////////
// Exported Interface                                                        //
///////////////////////////////////////////////////////////////////////////////

void stable_01_sort(PSTPoint* array, int npoints, const PSTPoint& s) {
  stable_01_sort(array,0,npoints-1,s);
}
void stable_01_sort(PSTPoint* array, int begin, int end, const PSTPoint& s) {
  simple_partition(array,begin,end,s);
}
