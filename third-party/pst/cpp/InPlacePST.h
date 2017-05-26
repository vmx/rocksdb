//-*- mode: c++ -*-////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    InPlacePST.h                                                     //
//                                                                           //
// MODULE:  Priority Search Tree                                             //
//                                                                           //
// PURPOSE: Implements the data structure presented in "In-place             //
//          Priority Search Tree and its applications" by De,                //
//          Maheshwari, Nandy, Smid in 2011.                                 //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
//                             Public Methods:                               //
///////////////////////////////////////////////////////////////////////////////
#ifndef INPLACEPST_H
#define INPLACEPST_H

#include <vector>
#include "PSTPoint.h"

using namespace std;

namespace PrioritySearchTree {
  class InPlacePST {
    PSTPoint* tree;
    // TODO vmx 2017-05-26: Change type from `int` to `size_t`
    int npoints;
    InPlacePST() {}
    void buildLevel(int i, int n);
    void swap(int a, int b);
    void inPlaceSort(int begin, int end);
    int numberOfChildren(int index);
    bool isLeaf(int index);
    void explore(int indexP, coordy_t ymin, vector<PSTPoint>* points);
    PSTPoint getPoint(int n); // index base 1
    void printTree(int index, int spaces);
  public:
    coordx_t POSITIVE_INFINITY;
    coordy_t NEGATIVE_INFINITY;
    InPlacePST(PSTPoint* points, int n);
    PSTPoint leftMostNE(coordx_t xmin, coordy_t ymin);
    PSTPoint highestNE(coordx_t xmin, coordy_t ymin);
    PSTPoint highest3Sided(coordx_t xmin, coordx_t xmax, coordy_t ymin);
    vector<PSTPoint>* enumerate3Sided(coordx_t xmin, coordx_t xmax, coordy_t ymin);
    void printTree();

    // The total byte size of the tree
    size_t size() {
      // `npoints` shouldn't be casted to `size_t` but have that type. This
      // would need a bigger change in the code base as it ripples up
      // throughout the whole code base
      return static_cast<size_t>(npoints) * (sizeof(coordx_t) + sizeof(coordy_t));
    };
  };
}

#endif
