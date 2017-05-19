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
#include <algorithm>

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
}
