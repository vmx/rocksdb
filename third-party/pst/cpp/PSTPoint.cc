///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    PSTPoint.cpp                                                     //
//                                                                           //
// MODULE:  Priority Search Tree                                             //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
#include "PSTPoint.h"
#include "base_util.h"
#include <algorithm>
#include <iostream>
using namespace std;

namespace PrioritySearchTree {
  void PSTPoint::setX(coordx_t new_x) {
    x = new_x;
  }

  void PSTPoint::setY(coordy_t new_y) {
    y = new_y;
  }

  bool PSTPoint::yGreaterThan(const PSTPoint& p) {
    return y > p.getY();
  }

  bool PSTPoint::yLessThan(const PSTPoint& p) {
    return y < p.getY();
  }

  bool PSTPoint::operator<(const PSTPoint& p) {
    return x < p.getX();
  }

  bool PSTPoint::operator>(const PSTPoint& p) {
    return x > p.getX();
  }

  vector<unsigned char> PSTPoint::serialize() {
    // NOTE vmx 2017-05-19: Add proper conversion for doubles
    vector<unsigned char> result = longlongToBytes(static_cast<long long>(x));
    vector<unsigned char> temp = intToBytes(y);
    result.insert(result.end(), temp.begin(), temp.end());
    return result;
  }

  void PSTPoint::load(char *filename) {
    FILE *fp = fopen(filename, "r");
    load(fp);
  }

  int PSTPoint::load(FILE *fp) {
    x = bytesToInt(fp);
    y = bytesToInt(fp);
    return 8;
  }
}
