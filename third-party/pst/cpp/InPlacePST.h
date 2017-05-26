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
    int npoints;
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
    InPlacePST(){}
    InPlacePST(const char *filename);
    InPlacePST(FILE *fp);
    InPlacePST(PSTPoint* points, int n);
    PSTPoint leftMostNE(coordx_t xmin, coordy_t ymin);
    PSTPoint highestNE(coordx_t xmin, coordy_t ymin);
    PSTPoint highest3Sided(coordx_t xmin, coordx_t xmax, coordy_t ymin);
    vector<PSTPoint>* enumerate3Sided(coordx_t xmin, coordx_t xmax, coordy_t ymin);
    void printTree();

    vector< unsigned char > serialize();
    int load(FILE* fp);
    void load(const char *filename);

    void printArray() {
      cout << "RESULT: " << endl;
      for(int i=0;i<npoints;i++)
        cout << tree[i] << " ";
      cout << endl << "OVER" << endl;
    };

    // The total byte size of the tree
    size_t size() {
      return npoints * (sizeof(coordx_t) + sizeof(coordy_t));
    };
  };
}

#endif
