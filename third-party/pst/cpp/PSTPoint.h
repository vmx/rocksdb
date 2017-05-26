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

#include <cstdint>
#include <ostream>
#include <algorithm>
using std::basic_ostream;

namespace PrioritySearchTree {
  typedef double coordx_t;
  typedef uint64_t coordy_t;

  class PSTPoint {
    coordx_t x;
    coordy_t y;
    void setX(coordx_t new_x);
    void setY(coordy_t new_y);
  public:
    PSTPoint(coordx_t xx = 0, coordy_t yy = 0)
      : x(xx), y(yy)
    { }
    PSTPoint(const PSTPoint& copy)
      : x(copy.getX()), y(copy.getY())
    { }
    coordx_t getX() const { return x; }
    coordy_t getY() const { return y; }

    /////////////////////////////////////////////////////////////////////////////
    // Implemented in PSTPoint.cpp                                             //
    /////////////////////////////////////////////////////////////////////////////
    bool operator>(const PSTPoint& p); // compares x coordinate
    bool operator<(const PSTPoint& p); // compares x coordinate
    bool yGreaterThan(const PSTPoint& p);
    bool yLessThan(const PSTPoint& p);

    // Return the size of a single point in bytes
    static size_t size() {
      return sizeof(coordx_t) + sizeof(coordy_t);
    }
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
