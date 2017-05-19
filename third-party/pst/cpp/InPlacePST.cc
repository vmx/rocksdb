///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    InPlacePST.cpp                                                   //
//                                                                           //
// MODULE:  Priority Search Tree                                             //
//                                                                           //
// NOTES:   Implements the data structure presented in "In-place             //
//          Priority Search Tree and its applications" by De,                //
//          Maheshwari, Nandy, Smid in 2011.                                 //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
#include <iostream>
#include <vector>
#include <cmath>
#include <limits>
#include <time.h>
#include <cstdio>

#include "PSTPoint.h"
#include "array_utilities.h"
#include "sort/heap_sort.h"
#include "InPlacePST.h"

using namespace std;

namespace PrioritySearchTree {
  ///////////////////////////////////////////////////////////////////////////////
  // General functions                                                         //
  ///////////////////////////////////////////////////////////////////////////////
  int indexOfParent(int index) { // base 1
    return static_cast<int>(floor(index/2));
  }
  int indexOfLeftChild(int index) { // base 1
    return (2*index);
  }
  int indexOfRightChild(int index) { // base 1
    return (2*index)+1;
  }
  int powerOf2(int x) {
    return (int)pow((double)2,(double)x);
  }
  int level(int index) {
    return (int)log2(index);
  }
  bool isLeftChild(int index) {
    return (index % 2) == false;
  }

  ///////////////////////////////////////////////////////////////////////////////
  // Methods                                                                   //
  ///////////////////////////////////////////////////////////////////////////////
        
        InPlacePST::InPlacePST(const char *filename) {
                load(filename);
        }

        InPlacePST::InPlacePST(FILE *fp) {
                load(fp);
        }

  InPlacePST::InPlacePST(PSTPoint* points, int n) {
    time_t before, after;
    if(numeric_limits<coordx_t>::has_infinity) {
      POSITIVE_INFINITY = numeric_limits<coordx_t>::infinity();
    } else {
      POSITIVE_INFINITY = numeric_limits<coordx_t>::max();
    }
    if(numeric_limits<coordy_t>::has_infinity) {
      NEGATIVE_INFINITY = -numeric_limits<coordy_t>::infinity();
    } else {
      NEGATIVE_INFINITY = numeric_limits<coordy_t>::min();
    }
    npoints = n;
    cout << endl << "Initial copy..." << flush;
    before = time(0);
    tree = new PSTPoint[n];
    PSTArray::copy(points,tree,n);
    after = time(0);
    cout << "took: " << (after - before) << endl
         << "Initial sort..." << flush;
    before = time(0);
    heap_sort(tree,n);
    after = time(0);
    cout << "took: " << (after - before) << endl;
    int h = (int)log2(n);
    cout << "Building levels..." << flush;
    before = time(0);
    for(int i = 0; i <= h-1; i++)
      buildLevel(i,n);
    after = time(0);
    cout << "took: " << (after - before) << endl;
    cout << "Total time ";
  }

  void InPlacePST::load(const char *filename) {
    FILE *fp = fopen(filename, "r");
    load(fp);
  }

  int InPlacePST::load(FILE *fp) {
    int size = 0;
    npoints = bytesToInt(fp);
                cout << "# of points: " << npoints << endl << flush;
    size += 4;
    tree = new PSTPoint[npoints];
    for(int i = 0;i < npoints; ++i) {
      int shift = tree[i].load(fp);
      size += shift;
    }
    return size;
  }

  vector<unsigned char> InPlacePST::serialize() {
    vector<unsigned char> result = intToBytes(npoints);
    for(int i = 0;i < npoints; ++i) {
      vector<unsigned char> temp = tree[i].serialize();
      result.insert(result.end(), temp.begin(), temp.end());
    }
    return result;
  }

  void InPlacePST::printTree() {
    printTree(1,0);
  }

  void InPlacePST::printTree(int index, int spaces) {
    int SPACES_PER_LEVEL = 5;
    if(index >= npoints) {
      while(spaces--) cout << " ";
      cout << "NIL\n";
      return;
    } else {
      printTree(indexOfRightChild(index),spaces+SPACES_PER_LEVEL);
      for(int i = 0; i < spaces; i++) cout << " ";
      cout << getPoint(index) << "\n";
      printTree(indexOfLeftChild(index),spaces+SPACES_PER_LEVEL);
    }
  }

  PSTPoint InPlacePST::getPoint(int n) { // index base 1
    return tree[n-1];
  }

  void InPlacePST::inPlaceSort(int begin, int end) {
    heap_sort(tree,begin-1,end-1);
  }

  void InPlacePST::swap(int a, int b) {
    PSTArray::swap(tree,a-1,b-1);
  }

  void InPlacePST::buildLevel(int i, int n) {
    /////////////////////////////////////////////////////////////////////////////
    // Find all the variables                                                  //
    /////////////////////////////////////////////////////////////////////////////
    // height of tree
    int h = (int)log2(n);
    // number of nodes filled in the last level
    int A = n - (powerOf2(h) - 1);
    // the first k nodes are roots of subtrees of size k1
    int k = (int)(A/powerOf2(h-i));
    // the first k nodes are roots of subtrees of size k1
    int k1 = powerOf2(h+1-i) - 1;
    // the (k+1)-st node is the root of subtree of size k2
    int k2 = powerOf2(h-i) - 1 + A - k*powerOf2(h-i);
    // the remaining nodes are roots of subtrees of size k3
    int k3 = powerOf2(h-i) - 1;

    /////////////////////////////////////////////////////////////////////////////
    // Order nodes at given level                                              //
    /////////////////////////////////////////////////////////////////////////////
    int indexOfMaxY;

    // build the subtrees of size k1
    for(int j = 1; j <= k; j++) {
      // Find point with maximum Y in range
      indexOfMaxY = powerOf2(i)+(j-1)*k1;
      for(int index = indexOfMaxY; index <= powerOf2(i)+j*k1-1; index++)
        if(getPoint(index).yGreaterThan(getPoint(indexOfMaxY)))
          indexOfMaxY = index;
      swap(indexOfMaxY,powerOf2(i)+j-1);
    }

    if(k < powerOf2(i)) {
      // build the one subtree of size k2
      indexOfMaxY = powerOf2(i)+k*k1;
      for(int index = indexOfMaxY; index <= powerOf2(i)+k*k1+k2-1; index++) {
        if(getPoint(index).yGreaterThan(getPoint(indexOfMaxY)))
          indexOfMaxY = index;
      }
      swap(indexOfMaxY,powerOf2(i)+k);
        
      // build the subtrees of size k3
      int m = powerOf2(i)+k*k1+k2;
      for(int j = 1; j <= powerOf2(i)-k-1; j++) {
        indexOfMaxY = m+(j-1)*k3;
        for(int index = indexOfMaxY; index <= m+j*k3-1; index++)
          if(getPoint(index).yGreaterThan(getPoint(indexOfMaxY)))
            indexOfMaxY = index;
        swap(indexOfMaxY,powerOf2(i)+k+j);
      }
    }
    // Finally, sort all points past the current level
    inPlaceSort(powerOf2(i+1),n);
  }
  int InPlacePST::numberOfChildren(int index) { // base 1
    if(indexOfLeftChild(index) > npoints) return 0;
    if(indexOfRightChild(index) > npoints) return 1;
    return 2;
  }

  bool InPlacePST::isLeaf(int index) {
    return 0 == numberOfChildren(index);
  }

  PSTPoint InPlacePST::leftMostNE(coordx_t xmin, coordy_t ymin) {
    PSTPoint best(POSITIVE_INFINITY,NEGATIVE_INFINITY);
    int indexP = 1; // start at root
    int indexQ = 1;
    while(!isLeaf(indexP)) {
      // UpdateLeftMost(p)
      PSTPoint p = getPoint(indexP);
      if(xmin <= p.getX() && p.getY() >= ymin && p.getX() < best.getX())
        best = p;
      // UpdateLeftMost(q)
      PSTPoint q = getPoint(indexQ);
      if(xmin <= q.getX() && q.getY() >= ymin && q.getX() < best.getX())
        best = q; // assume q has the lower x coordinate
      // 
      if(indexP == indexQ) {
        if(numberOfChildren(indexP) == 1) {
          indexQ = indexOfLeftChild(indexP);
          indexP = indexOfLeftChild(indexP);
        } else {
          indexQ = indexOfRightChild(indexP);
          indexP = indexOfLeftChild(indexP);
        }
      } else {
        if(isLeaf(indexQ))
          indexQ = indexP;
        else if(numberOfChildren(indexQ) == 1) {
          PSTPoint ql = getPoint(indexOfLeftChild(indexQ));
          PSTPoint pr = getPoint(indexOfRightChild(indexP));
          if(ql.getY() < ymin) {
            indexQ = indexOfRightChild(indexP);
            indexP = indexOfLeftChild(indexP);
          } else if(pr.getY() < ymin) {
            indexP = indexOfLeftChild(indexP);
            indexQ = indexOfLeftChild(indexQ);
          } else if(ql.getX() < xmin) {
            indexP = indexOfLeftChild(indexQ);
            indexQ = indexOfLeftChild(indexQ);
          } else if(pr.getX() < xmin) {
            indexP = indexOfRightChild(indexP);
            indexQ = indexOfLeftChild(indexQ);
          } else {
            indexQ = indexOfRightChild(indexP);
            indexP = indexOfLeftChild(indexP);
          }
        } else { // q has 2 children
          PSTPoint ql = getPoint(indexOfLeftChild(indexQ));
          PSTPoint pr = getPoint(indexOfRightChild(indexP));
          PSTPoint pl = getPoint(indexOfLeftChild(indexP));
          if(pr.getX() >= xmin && pr.getY() >= ymin) {
            indexQ = indexOfRightChild(indexP);
            indexP = indexOfLeftChild(indexP);
          } else if(pr.getX() < xmin) {
            if(ql.getX() < xmin) {
              indexP = indexOfLeftChild(indexQ);
              indexQ = indexOfRightChild(indexQ);
            } else if(ql.getY() < ymin) {
              indexP = indexOfRightChild(indexP);
              indexQ = indexOfRightChild(indexQ);
            } else {
              indexP = indexOfRightChild(indexP);
              indexQ = indexOfLeftChild(indexQ);
            }
          } else { // pr.getX() >= xmin AND pr.getY() < ymin
            if(pl.getY() < ymin) {
              indexP = indexOfLeftChild(indexQ);
              indexQ = indexOfRightChild(indexQ);
            } else {
              indexP = indexOfLeftChild(indexP);
              if(ql.getY() >= ymin)
                indexQ = indexOfLeftChild(indexQ);
              else
                indexQ = indexOfRightChild(indexQ);
            }
          }
        }
      }
    }
    // UpdateLeftMost(p)
    PSTPoint p = getPoint(indexP);
    if(xmin <= p.getX() && p.getY() >= ymin && p.getX() < best.getX())
      best = p;
    // UpdateLeftMost(q)
    PSTPoint q = getPoint(indexQ);
    if(xmin <= q.getX() && q.getY() >= ymin && q.getX() < best.getX())
      best = q; // assume q has the lower x coordinate
    return best;  
  }
  PSTPoint InPlacePST::highestNE(coordx_t xmin, coordy_t ymin) {
    PSTPoint best(POSITIVE_INFINITY,NEGATIVE_INFINITY);
    int indexP = 1; // root
    while(!isLeaf(indexP)) {
      PSTPoint p = getPoint(indexP);
      if(p.getX() >= xmin && p.getY() >= ymin) {
        // Updatehighest(p)
        if(p.getY() > best.getY())
          best = p;
        indexP = indexOfLeftChild(indexP);
      } else if(p.getY() < ymin) {
        indexP = indexOfLeftChild(indexP);
      } else if(numberOfChildren(indexP) == 1) {
        indexP = indexOfLeftChild(indexP);
      } else { // must have 2 children
        PSTPoint pl = getPoint(indexOfLeftChild(indexP));
        PSTPoint pr = getPoint(indexOfRightChild(indexP));
        if(pr.getX() <= xmin) {
          indexP = indexOfRightChild(indexP);
        } else if(pl.getX() >= xmin) {
          if(pl.getY() > pr.getY()) indexP = indexOfLeftChild(indexP);
          else indexP = indexOfRightChild(indexP);
        } else if(pr.getY() < ymin) {
          indexP = indexOfLeftChild(indexP);
        } else {
          // Updatehighest(pr)
          if(pr.getX() >= xmin &&
             pr.getY() >= ymin &&
             pr.getY() > best.getY())
            best = p;
          indexP = indexOfLeftChild(indexP);
        }
      }
    }
    PSTPoint p = getPoint(indexP);
    // Updatehighest(p)
    if(p.getX() >= xmin && p.getY() >= ymin && p.getY() > best.getY())
      best = p;
    return best;
  }
  PSTPoint InPlacePST::highest3Sided(coordx_t xmin, coordx_t xmax, coordy_t ymin) {
    PSTPoint best(POSITIVE_INFINITY,NEGATIVE_INFINITY);
    /////////////////////////////////////////////////////////////////////////////
    // Initialization                                                          //
    /////////////////////////////////////////////////////////////////////////////
    bool L = false, R = false;
    PSTPoint root = getPoint(1);
    int indexP = 1, indexQ = 1; // start with the root
    if(xmin <= root.getX() && root.getX() <= xmax) {
      if(root.getY() >= ymin)
        best = root;
    } else {
      if(root.getX() < xmin) {
        L = true;
      } else {
        R = true;
      }
    }
    /////////////////////////////////////////////////////////////////////////////
    // Traversal
    /////////////////////////////////////////////////////////////////////////////
    while(L || R) {
      // Need to check from the left side of the query region
      if(L && (!R || level(indexP) < level(indexQ))) {
        // CheckLeft(p)
        int indexPL = indexOfLeftChild(indexP);
        int indexPR = indexOfRightChild(indexP);
        if(isLeaf(indexP))
          L = false;
        else if(numberOfChildren(indexP) == 1) {
          PSTPoint pl = getPoint(indexPL);
          if(xmin <= pl.getX() && pl.getX() <= xmax) {
            // within query region
            // Updatehighest(Pl)
            if(pl.getY() >= ymin && pl.getY() > best.getY()) {
              best = pl;
            }
            // end
            L = false;
          } else if(pl.getX() < xmin) {
            // left of query region
            indexP = indexPL;
          } else {
            // must be right of query region
            indexQ = indexPL;
            R = true;
            L = false;
          }
        } else { // 2 children
          PSTPoint pl = getPoint(indexPL);
          PSTPoint pr = getPoint(indexPR);
          if(pl.getX() < xmin) { // pl is left of query region
            if(pr.getX() < xmin) {
              // Since both subtrees are to the left of
              // the query region, all points within the query region
              // must be in the right subtree
              indexP = indexPR;
            } else if(pr.getX() <= xmax) {
              // Updatehighest(Pr)
              if(pr.getY() >= ymin && pr.getY() > best.getY()) {
                best = pr;
              }
              // end
              // since we've taken the best from the right subtree
              // any remaining points within the query region must
              // be in the left subtree
              indexP = indexPL; 
            } else {
              // pl is to the left of the query region and
              // pr is to the right of the query region
              // therefore, we must search left and right
              indexQ = indexPR;
              indexP = indexPL;
              R = true;
            } 
          } else if(pl.getX() <= xmax) { // pl is within the query region
            // UpdateHighest(Pl)
            if(pl.getY() >= ymin && pl.getY() > best.getY()) {
              best = pl;
            }
            // end
            L = false;
            if(pr.getX() > xmax) { // pr is beyond the query region
              indexQ = indexPR;
              R = true;
            } else { // pr must also be within the query region
              // UpdateHighest(Pr)
              if(pr.getY() >= ymin && pr.getY() > best.getY()) {
                best = pr;
              }
              // end
            }
          } else { // pl is right of query region
            // we begin searching from the right
            indexQ = indexPL;
            L = false;
            R = true;
          }
        }
        // end CheckLeft(p)
      }
      // need to check from the right of the query region
      else {
        // CheckRight(q)
        int indexQL = indexOfLeftChild(indexQ);
        int indexQR = indexOfRightChild(indexQ);
        // CASE 1: Q is a leaf
        if(isLeaf(indexQ)) {
          R = false; // stop searching from the right
        }
        // CASE 2: Q has one child
        else if(numberOfChildren(indexQ) == 1) {
          PSTPoint ql = getPoint(indexQL);
          // CASE 2A: ql is within query region
          if(xmin <= ql.getX() && ql.getX() <= xmax) {
            // UpdateHighest(Ql)
            if(ql.getY() >= ymin && ql.getY() > best.getY()) {
              best = ql;
            }
            // end
            // no need to check any subtrees since they will
            // have lower y-values by design
            R = false;
          }
          // CASE 2B: ql is right of query region
          else if(ql.getX() > xmax) {
            indexQ = indexQL; // keep checking from the right
          }
          // CASE 2C: ql must be left of query region
          else {
            indexP = indexQL; // start checking from the left
            L = true;
            R = false;
          }
        }
        // CASE 3: Q has 2 children
        else { 
          PSTPoint ql = getPoint(indexQL);
          PSTPoint qr = getPoint(indexQR);
          // CASE 3A: qr is right of query region
          if(qr.getX() > xmax) {
            // CASE 3A(I): both children are right of query region
            if(ql.getX() > xmax) {
              // since the median x-coordinate that bisects the
              // subtrees must be >= ql.getX(), only left subtree
              // may contain points within the query region
              indexQ = indexQL; 
            }
            // CASE 3A(II): ql is within query region
            else if(ql.getX() >= xmin) {
              // Updatehighest(Ql)
              if(ql.getY() >= ymin && ql.getY() > best.getY()) {
                best = ql;
              }
              // end
              // since we've taken the best from the left subtree
              // any remaining points within the query region must
              // be in the right subtree
              indexQ = indexQR; 
            }
            // CASE 3A(III): ql is left of query region
            else {
              // we must search left and right
              indexP = indexQL;
              indexQ = indexQR;
              L = true; // check left
            } 
          }
          // CASE 3B: qr is within query region
          else if(qr.getX() >= xmin) { 
            // Updatehighest(Qr)
            if(qr.getY() >= ymin && qr.getY() > best.getY()) {
              best = qr;
            }
            // end
            R = false;
            // CASE 3B(I): ql is left of query region
            if(ql.getX() < xmin) { 
              indexP = indexQR;
              L = true; // search from the left
            }
            // CASE 3B(II): ql must be within query region
            else { 
              // Updatehighest(Ql)
              if(ql.getY() >= ymin && ql.getY() > best.getY()) {
                best = ql;
              }
              // end
            }
          }
          // CASE 3C: qr is left of query region
          else { 
            // begin searching from the left
            indexP = indexQR;
            L = true;
            R = false;
          }
        }
        // end CheckRight(q)
      }
    }
    return best;
  }
  void InPlacePST::explore(int indexP, coordy_t ymin, vector<PSTPoint>* points) {
    PSTPoint p = getPoint(indexP);
    // p is within query region
    if(p.getY() >= ymin) {
      int indexC = indexP;
      PSTPoint current;
      int state = 0;
      while((indexC != indexP) || (state != 2)) {
        current = getPoint(indexC);
        if(state == 0) {
          // report current
          points->push_back(current);
          // if current has a left child and y(left child of current) >= ymin
          int indexCl = indexOfLeftChild(indexC);
          if(numberOfChildren(indexC) > 0 &&
             getPoint(indexCl).getY() >= ymin) {
            indexC = indexCl;
          } else {
            state = 1;
          }
        } else {
          if(state == 1) {
            // if current has a right child and y(right child of current) >= ymin
            int indexCr = indexOfRightChild(indexC);
            if(numberOfChildren(indexC) == 2 &&
               getPoint(indexCr).getY() >= ymin) {
              indexC = indexCr;
              state = 0;
            } else {
              state = 2;
            }
          } else { 
            // state == 2 && current != p
            if(isLeftChild(indexC)) {
              state = 1;
            }
            indexC = indexOfParent(indexC);
          }
        }
      }
    }
  }
  vector<PSTPoint>* InPlacePST::enumerate3Sided(coordx_t xmin,
                                                coordx_t xmax,
                                                coordy_t ymin) {
    /////////////////////////////////////////////////////////////////////////////
    // Initialization                                                          //
    /////////////////////////////////////////////////////////////////////////////
    vector<PSTPoint>* points = new vector<PSTPoint>;
    int indexP = 1, indexPp = 1, indexQ = 1, indexQp = 1;
    PSTPoint root = getPoint(1);
    bool
      // flags that a point p has been found which is left of the
      // query region but whose children may be within
      L = false,
      // flags that a point p' has been found which is between xmin and
      // xmax, but may be below the query region
      Lp = false,
      // flags that a point q has been found with is right of the query
      // region, but whose children may be within
      R = false,
      // flags that a point q' has been found which is between xmin and
      // xmax, but may be below the query region
      Rp = false;
    // root is left of query region
    if(root.getX() < xmin) {
      L = true;
    }
    // root is between xmin and xmax (may be below ymin)
    else if(root.getX() < xmax) {
      Lp = true;
    }
    // root must be right of query region
    else {
      R = true;
    }
    /////////////////////////////////////////////////////////////////////////////
    // Traversal                                                               //
    /////////////////////////////////////////////////////////////////////////////
    while(L || Lp || R || Rp) {
      // if several points should be searched, we will begin with the
      // point at the lowest level in the tree
      int levelP  = level(indexP);
      int levelPp = level(indexPp);
      int levelQ  = level(indexQ);
      int levelQp = level(indexQp);
      int minLevel = -1;
      if(L && ((minLevel == -1) || (levelP < minLevel)))
        levelP = minLevel;
      else if(Lp && ((minLevel == -1) || (levelPp < minLevel)))
        levelPp = minLevel;
      else if(R && ((minLevel == -1) || (levelQ < minLevel)))
        levelQ = minLevel;
      else if(Rp && ((minLevel == -1) || (levelQp < minLevel)))
        levelQp = minLevel;
            
      // Search from the left
      if(L && levelP == minLevel) {
        // EnumerateLeft(p)
        int indexPl = indexOfLeftChild(indexP);
        int indexPr = indexOfRightChild(indexP);
        // CASE 1: p is a leaf
        if(isLeaf(indexP)) {
          L = false;
        }
        // CASE 2: p has one child (must be left)
        else if(numberOfChildren(indexP) == 1) {
          PSTPoint pl = getPoint(indexPl);
          // CASE 2A: child is in query region
          if(xmin <= pl.getX() && pl.getX() <= xmax) {
            // If there are points p' and q' s.t.:
            //   xmin <= x(p') <= xmax
            // AND
            //   xmin <= x(q') <= xmax
            // AND
            //   x(p') <= x(q')
            if(Lp && Rp) {
              // Explore(p')
              explore(indexPp,ymin,points);
            }
            // If there is a point p' s.t.:
            //   xmin <= x(p') <= xmax
            else if(Lp) {
              indexQp = indexPp;
              Rp = true;
            }
            indexPp = indexPl;
            Lp = true;
            L = false;
          }
          // CASE 2B: child is left of query region
          else if(pl.getX() < xmin) {
            indexP = indexPl;
          }
          // CASE 2C: child must be right of query region
          else {
            indexQ = indexPl;
            R = true;
            L = false;
          }
        }
        // CASE 3: p has two children
        else {
          PSTPoint pl = getPoint(indexPl);
          PSTPoint pr = getPoint(indexPr);
          // CASE 3A: left child is left of query region
          if(pl.getX() < xmin) {
            // CASE 3A(i): right child is left of query region
            if(pr.getX() < xmin) {
              indexP = indexPr;
            }
            // CASE 3A(ii): right child is within query region
            else if(pr.getX() <= xmax) {
              // If there are points p' and q' s.t.:
              //   xmin <= x(p') <= xmax
              // AND
              //   xmin <= x(q') <= xmax
              // AND
              //   x(p') <= x(q')
              if(Lp && Rp) {
                // Explore(p')
                explore(indexPp,ymin,points);
              }
              // If there is a point p' s.t.:
              //   xmin <= x(p') <= xmax
              else if(Lp) {
                indexQp = indexPp;
                Rp = true;
              }
              indexPp = indexPr;
              indexP = indexPl;
              Lp = true;
            }
            // CASE 3A(iii): right child must be right of query region
            else {
              indexQ = indexPr;
              indexP = indexPl;
              R = true;
            }
          }
          // CASE 3B: left child is within query region
          else if(pl.getX() <= xmax) {
            // CASE 3B(i): right child is right of query region
            if(pr.getX() > xmax) {
              indexQ = indexPr;
              indexPp = indexPl;
              L = false;
              Lp = true;
              R = true;
            }
            // CASE 3B(ii): right child must be within query region
            else {
              // If there are points p' and q' s.t.:
              //   xmin <= x(p') <= xmax
              // AND
              //   xmin <= x(q') <= xmax
              // AND
              //   x(p') <= x(q')
              if(Rp && Lp) {
                // Explore(p')
                explore(indexPp,ymin,points);
                // Explore(pr)
                explore(indexPr,ymin,points);
              }
              // If there is a point p' s.t.:
              //   xmin <= x(p') <= xmax
              else if(Lp) {
                // Explore(pr)
                explore(indexPr,ymin,points);
                indexQp = indexPp;
                Rp = true;
              }
              // If there is a point q' s.t.:
              //   xmin <= x(q') <= xmax
              else if(Rp) {
                // Explore(pr)
                explore(indexPr,ymin,points);
                Lp = true;
              }
              // Otherwise, there are no such points p' or q'
              else {
                indexQp = indexPr;
                Lp = true;
                Rp = true;
              }
              indexPp = indexPl;
              L = false;
            }
          }
          // CASE 3C: left child must be right of query region
          else {
            indexQ = indexPl;
            L = false;
            R = true;
          }
        }
        // end EnumerateLeft(p)
      }
      // search from the left within the query region
      else if(Lp && levelPp == minLevel) {
        // EnumerateLeftIn(p')
        int indexPpl = indexOfLeftChild(indexPp);
        int indexPpr = indexOfRightChild(indexPp);
        PSTPoint pp = getPoint(indexPp);
        // If y is within query region, report it
        if(pp.getY() >= ymin) {
          points->push_back(pp);
        }
        // CASE 1: p' has no children
        if(isLeaf(indexPp)) {
          Lp = false;
        }
        // CASE 2: p' has one child (left)
        else if(numberOfChildren(indexPp) == 1) {
          PSTPoint ppl = getPoint(indexPpl);
          // CASE 2A: child is within query region
          if(xmin <= ppl.getX() && ppl.getX() <= xmax) {
            indexPp = indexPpl;
          }
          // CASE 2B: child is left of query region
          else if(ppl.getX() < xmin) {
            indexP = indexPpl;
            Lp = false;
            L = true;
          }
          // CASE 2C: child must be right of query region
          else {
            indexQ = indexPpl;
            R = true;
            Lp = false;
          }
        }
        // CASE 3: p' has two children
        else {
          PSTPoint ppl = getPoint(indexPpl);
          PSTPoint ppr = getPoint(indexPpr);
          // CASE 3A: left child is left of query region
          if(ppl.getX() < xmin) {
            // CASE 3A(i): right child is left of query region
            if(ppr.getX() < xmin) {
              indexP = indexPpr;
              L = true;
              Lp = false;
            }
            // CASE 3B(ii): right child is within query region
            else if(ppr.getX() <= xmax) {
              indexP = indexPpl;
              indexPp = indexPpr;
              L = true;
            }
            // CASE 3C(iii): right child must be right of query region
            else {
              indexQ = indexPpr;
              indexP = indexPpl;
              R = true;
              L = true;
              Lp = false;
            }
          }
          // CASE 3B: left child is within query region
          else if(ppl.getX() <= xmax) {
            // CASE 3B(i): right child is right of query region
            if(ppr.getX() > xmax) {
              indexQ = indexPpr;
              indexPp = indexPpl;
              R = true;
            }
            // CASE 3B(ii): right child must be within query region
            else {
              // If there is a point q' s.t.:
              //   xmin <= x(q') <= xmax
              if(Rp) {
                // Explore(p'r)
                explore(indexPpr,ymin,points);
                indexPp = indexPpl;
              }
              // Otherwise, there is no such point q'
              else {
                indexQp = indexPpr;
                indexPp = indexPpl;
                Rp = true;
              }
            }
          }
          // CASE 3C: left child must be right of query region
          else {
            indexQ = indexPpl;
            Lp = false;
            R = true;
          }
        }
        // end EnumerateLeftIn(p')
      }
      // search from the right
      else if(R && levelQ == minLevel) {
        // EnumerateRight(q)
        int indexQl = indexOfLeftChild(indexQ);
        int indexQr = indexOfRightChild(indexQ);
        // CASE 1: q is a leaf
        if(isLeaf(indexQ)) {
          R = false; // stop enumerating from the right
        }
        // CASE 2: q has one child (must be left)
        else if(numberOfChildren(indexQ) == 1) {
          PSTPoint ql = getPoint(indexQl);
          // CASE 2A: child is in query region
          if(xmin <= ql.getX() && ql.getX() <= xmax) {
            // If there are points p' and q' s.t.:
            //   xmin <= x(p') <= xmax
            // AND
            //   xmin <= x(q') <= xmax
            // AND
            //   x(p') <= x(q')
            if(Lp && Rp) {
              // Explore(q')
            }
            // If there is a point q' s.t.:
            //   xmin <= x(q') <= xmax
            else if(Rp) {
              indexQp = indexPp;
              Lp = true;
            }
            indexQp = indexQl;
            Rp = true;
            R = false;
          }
          // CASE 2B: child is left of query region
          else if(ql.getX() < xmin) {
            indexP = indexQl;
            R = false;
            L = true; // search from left
          }
          // CASE 2C: child must be right of query region
          else {
            indexQ = indexQl;
          }
        }
        // CASE 3: p has two children
        else {
          PSTPoint ql = getPoint(indexQl);
          PSTPoint qr = getPoint(indexQr);
          // CASE 3A: right child is right of query region
          if(qr.getX() > xmax) {
            // CASE 3A(i): left child is right of query region
            if(ql.getX() > xmax) {
              indexQ = indexQl;
            }
            // CASE 3A(ii): left child is within query region
            else if(ql.getX() >= xmin) {
              // If there are points p' and q' s.t.:
              //   xmin <= x(p') <= xmax
              // AND
              //   xmin <= x(q') <= xmax
              // AND
              //   x(p') <= x(q')
              if(Lp && Rp) {
                // Explore(q')
                explore(indexQp,ymin,points);
              }
              // If there is a point q' s.t.:
              //   xmin <= x(q') <= xmax
              else if(Rp) {
                indexPp = indexQp;
                Lp = true;
              }
              indexQp = indexQl;
              indexQ = indexQr;
              Rp = true;
            }
            // CASE 3A(iii): left child must be left of query region
            else {
              indexP = indexQl;
              indexQ = indexQr;
              L = true;         // search from left as well
            }
          }
          // CASE 3B: right child is within query region
          else if(qr.getX() >= xmin) {
            // CASE 3B(i): left child is left of query region
            if(ql.getX() < xmin) {
              indexQp = indexQr;
              indexP = indexQl;
              R = false;
              Rp = true;
              L = true;
            }
            // CASE 3B(ii): left child must be within query region
            else {
              // If there are points p' and q' s.t.:
              //   xmin <= x(p') <= xmax
              // AND
              //   xmin <= x(q') <= xmax
              // AND
              //   x(p') <= x(q')
              if(Rp && Lp) {
                // Explore(q')
                explore(indexQp,ymin,points);
                // Explore(ql)
                explore(indexQl,ymin,points);
              }
              // If there is a point q' s.t.:
              //   xmin <= x(q') <= xmax
              else if(Rp) {
                // Explore(ql)
                explore(indexQl,ymin,points);
                indexPp = indexQp;
                Lp = true;
              }
              // If there is a point p' s.t.:
              //   xmin <= x(p') <= xmax
              else if(Lp) {
                // Explore(ql)
                explore(indexQl,ymin,points);
                Rp = true;
              }
              // Otherwise, there are no such points p' or q'
              else {
                indexPp = indexQl;
                Lp = true;
                Rp = true;
              }
              indexQp = indexQr;
              R = false;
            }
          }
          // CASE 3C: right child must be left of query region
          else {
            indexP = indexQl;
            L = true;          // search from left
            R = false;
          }
        }
        // end EnumerateRight(q)
      }
      // search from the right within the query region
      else {
        // EnumerateRightIn(q')
        int indexQpl = indexOfLeftChild(indexQp);
        int indexQpr = indexOfRightChild(indexQp);
        PSTPoint qp = getPoint(indexQp);
        // If q' is within query region, report it
        if(qp.getY() >= ymin) {
          points->push_back(qp);
        }
        // CASE 1: q' has no children
        if(isLeaf(indexQp)) {
          Rp = false;
        }
        // CASE 2: q' has one child (left)
        else if(numberOfChildren(indexQp) == 1) {
          PSTPoint qpl = getPoint(indexQpl);
          // CASE 2A: child is within query region
          if(xmin <= qpl.getX() && qpl.getX() <= xmax) {
            indexQp = indexQpl;
          }
          // CASE 2B: child is left of query region
          else if(qpl.getX() < xmin) {
            indexP = indexQpl;
            Rp = false;
            L = true;
          }
          // CASE 2C: child must be right of query region
          else {
            indexQ = indexQpl;
            R = true;
            Rp = false;
          }
        }
        // CASE 3: q' has two children
        else {
          PSTPoint qpl = getPoint(indexQpl);
          PSTPoint qpr = getPoint(indexQpr);
          // CASE 3A: right child is right of query region
          if(qpr.getX() > xmax) {
            // CASE 3A(i): left child is right of query region
            if(qpl.getX() > xmax) {
              indexQ = indexQpr;
              R = true;
              Rp = false;
            }
            // CASE 3B(ii): left child is within query region
            else if(qpl.getX() >= xmin) {
              indexQ = indexQpr;
              indexQp = indexQpl;
              R = true;
            }
            // CASE 3C(iii): left child must be left of query region
            else {
              indexQ = indexQpr;
              indexP = indexQpl;
              R = true;
              L = true;
              Rp = false;
            }
          }
          // CASE 3B: right child is within query region
          else if(qpr.getX() >= xmin) {
            // CASE 3B(i): left child is left of query region
            if(qpl.getX() < xmin) {
              indexP = indexQpl;
              indexQp = indexQpr;
              L = true;
            }
            // CASE 3B(ii): left child must be within query region
            else {
              // If there is a point p' s.t.:
              //   xmin <= x(p') <= xmax
              if(Lp) {
                // Explore(q'l)
                explore(indexQpl,ymin,points);
                indexQp = indexQpl;
              }
              // Otherwise, there is no such point q'
              else {
                indexQp = indexQpr;
                indexPp = indexQpl;
                Lp = true;
              }
            }
          }
          // CASE 3C: right child must be left of query region
          else {
            indexP = indexQpr;
            Rp = false;
            L = true;
          }
        }
        // end EnumerateRightIn(q')
      }
    }
    return points;
  }
}
