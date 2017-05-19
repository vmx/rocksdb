//-*- mode: c++ -*-////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    PSTPoint.h                                                       //
//                                                                           //
// MODULE:  Priority Search Tree                                             //
//                                                                           //
// PURPOSE: Defines the PST Point class.                                     //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
//                             Public Methods:                               //
//                                                                           //
//   Type/Name:   double/getX                                                //
//   Description: Returns the x value of the point.                          //
//                                                                           //
//   Type/Name:   double/getY                                                //
//   Description: Returns the y value of the point.                          //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
#ifndef PSTPOINT_H
#define PSTPOINT_H

#include <ostream>
#include <algorithm>
using std::basic_ostream;

namespace PrioritySearchTree {
  typedef int coord_t;
  
  class PSTPoint {
    coord_t x, y;
    void setX(coord_t new_x);
    void setY(coord_t new_y);
  public:
    PSTPoint(coord_t x = 0, coord_t y = 0)
      : x(x), y(y)
    { }
    PSTPoint(const PSTPoint& copy)
      : x(copy.getX()), y(copy.getY())
    { }
    coord_t getX() const { return x; }
    coord_t getY() const { return y; }
  
    /////////////////////////////////////////////////////////////////////////////
    // Implemented in PSTPoint.cpp                                             //
    /////////////////////////////////////////////////////////////////////////////
    bool operator>(const PSTPoint& p); // compares x coordinate
    bool operator<(const PSTPoint& p); // compares x coordinate
    bool yGreaterThan(const PSTPoint& p);
    bool yLessThan(const PSTPoint& p);
  };

  /////////////////////////////////////////////////////////////////////////////
  // Boilerplate needed to send PSTPoint to cout                             //
  /////////////////////////////////////////////////////////////////////////////
  template <typename CharT, typename Traits>
  basic_ostream<CharT, Traits>& operator<<(basic_ostream<CharT, Traits>& out,
					   const PSTPoint& p) {
    return out<< "(" << p.getX() << "," << p.getY() << ")";
  }
}

#endif
