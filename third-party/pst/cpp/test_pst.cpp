///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    test_pst.cpp                                                     //
//                                                                           //
// MODULE:  Priority Search Tree                                             //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
#include <iostream>
#include <string>
#include <time.h>
#include <assert.h>
#include "PSTPoint.h"
#include "InPlacePST.h"
#include "array_utilities.h"
#include "control_utilities.h"

using namespace std;
using namespace PrioritySearchTree;

int main(int argv, char** argc) {
  const int MAX_POINTS_DISPLAY = 16;
  time_t before, after;
  int n, qi, ei;
  /////////////////////////////////////////////////////////////////////////////
  // Seed the PRNG                                                           //
  /////////////////////////////////////////////////////////////////////////////
  srand( time(0) );
  /////////////////////////////////////////////////////////////////////////////
  // Ensure the user has entered required parameters, otherwise print        //
  // a helpful message and bail out.                                         //
  /////////////////////////////////////////////////////////////////////////////
  if(argv < 3) {
    cout << "Usage: test_pst [number of points] [query iterations] "
	 << "[enumerate iterations]" << endl;
    return 1;
  }
  // parse number of points
  n = atoi(argc[1]);
  // parse query iterations
  qi = atoi(argc[2]);
  // parse enumerate iterations
  ei = atoi(argc[3]);
  /////////////////////////////////////////////////////////////////////////////
  // Create Points                                                           //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Creating " << n << " points..." << flush;
  before = time(0);
  PSTPoint *points = new PSTPoint[n]; // allocate on the heap
  for(int i = 1; i < n; i++) {
    PSTPoint p(i,i); // allocate on the stack
    points[i] = p;
  }
  after = time(0);
  cout << "took: " << (after - before) << endl;
  if(n <= MAX_POINTS_DISPLAY) {
    cout << "Points: ";
    PSTArray::print(points,n);
  }
  /////////////////////////////////////////////////////////////////////////////
  // Build tree                                                              //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Building tree..." << flush;
  before = time(0);
  InPlacePST ippst(points,n);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  delete points;
  /////////////////////////////////////////////////////////////////////////////
  // Calculate memory usage                                                  //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Memory usage: " << (n * sizeof(int)) << " bytes." << endl;
  /////////////////////////////////////////////////////////////////////////////
  // Print the structure if the number of points is small                    //
  /////////////////////////////////////////////////////////////////////////////
  if(n <= MAX_POINTS_DISPLAY) {
    cout << "Tree: " << endl;
    ippst.printTree();
  }
  /////////////////////////////////////////////////////////////////////////////
  // leftMostNE                                                              //
  /////////////////////////////////////////////////////////////////////////////
  PSTPoint result;
  cout << qi << " iterations of ";
  cout << "leftMostNE..." << flush;
  before = time(0);
  for(int i = 0; i < qi; i++)
    result = ippst.leftMostNE(rand() % n, rand() % n);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  /////////////////////////////////////////////////////////////////////////////
  // highestNE                                                               //
  /////////////////////////////////////////////////////////////////////////////
  cout << qi << " iterations of ";
  cout << "highestNE..." << flush;
  before = time(0);
  for(int i = 0; i < qi; i++)
    result = ippst.highestNE(rand() % n, rand() % n);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  /////////////////////////////////////////////////////////////////////////////
  // highest3Sided                                                           //
  /////////////////////////////////////////////////////////////////////////////
  cout << qi << " iterations of ";
  cout << "highest3Sided..." << flush;
  int xmin = 0, xmax = 0, ymin = 0;
  before = time(0);
  for(int i = 0; i < qi; i++) {
    xmin = rand() % n;
    xmax = xmin + (rand() % (n - xmin));
    ymin = rand() % n;
    result = ippst.highest3Sided(xmin,xmax,ymin);
  }
  after = time(0);
  cout << "took: " << (after - before) << endl;
  /////////////////////////////////////////////////////////////////////////////
  // enumerate3Sided                                                         //
  /////////////////////////////////////////////////////////////////////////////
  vector<PSTPoint>* results;
  cout << ei << " iterations of ";
  cout << "enumerate3Sided..." << flush;
  before = time(0);
  for(int i = 0; i < ei; i++) {
    xmin = rand() % n;
    xmax = xmin + (rand() % (n - xmin));
    results = ippst.enumerate3Sided(xmin,xmax,rand() % n);
    delete results;
  }
  after = time(0);
  cout << "took: " << (after - before) << endl;
  return 0;
}
