/******************************************************************************
*                       Copyright (c) 2011 - 2012 by                          *
*                               Simon Pratt                                   *
*                         (All rights reserved)                               *
*******************************************************************************
*                                                                             *
* FILE:    InPlacePST.java                                                    *
*                                                                             *
* MODULE:  Priority Search Tree                                               *
*                                                                             *
* NOTES:   Data structure presented in "In-place Priority Search Tree         *
*          and its applications" by De, Maheshwari, Nandy, Smid in 2011.      *
*                                                                             *
******************************************************************************/

import java.util.*;

public class InPlacePST implements PrioritySearchTree {
    PSTPoint[] tree;

    public InPlacePST(PSTPoint[] points) {
	tree = new PSTPoint[points.length];
	for(int i = 0; i < points.length; i++)
	    tree[i] = points[i];
	Sort.heapSort(tree,0,tree.length-1);
	int h = (int)Math.floor(log2(tree.length));
	for(int i = 0; i <= h-1; i++)
	    buildLevel(i);
    }

    private void buildLevel(int i) {
/******************************************************************************
* Find all the variables                                                      *
******************************************************************************/
	// number of nodes
	int n = tree.length;
	// height of tree
	int h = (int)Math.floor(log2(n));
	// number of nodes filled in the last level
	int A = n - (powerOf2(h) - 1);
	// the first k nodes are roots of subtrees of size k1
	int k = (int)Math.floor(A/powerOf2(h-i));
	// s is the median value
	PSTPoint s = getPoint(powerOf2(i+1));
	// the first k nodes are roots of subtrees of size k1
	int k1 = powerOf2(h+1-i) - 1;
	// the (k+1)-st node is the root of subtree of size k2
	int k2 = powerOf2(h-i) - 1 + A - k*powerOf2(h-i);
	// the remaining nodes are roots of subtrees of size k3
	int k3 = powerOf2(h-i) - 1;

/******************************************************************************
* Order nodes at given level                                                  *
******************************************************************************/

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
	inPlaceSort(powerOf2(i+1),n,s);
    }
/******************************************************************************
* Sorting                                                                     *
******************************************************************************/
    // Note: takes array indices of base 1
    private void inPlaceSort(int beginIndex, int endIndex, PSTPoint s) {
	Sort.heapSort(tree,baseZeroIndex(beginIndex),baseZeroIndex(endIndex));
    }
    

/******************************************************************************
* Query                                                                       *
******************************************************************************/
/******************************************************************************
*                                                                             *
* FUNCTION NAME: leftMostNE                                                   *
*                                                                             *
* PURPOSE:       Determine the point with minimum x-coordinate among          *
*                all points {p ∈ P | xmin ≤ p.x ∧ ymin ≤ p.y}                 *
*                                                                             *
* PARAMETERS                                                                  *
*   Type/Name:   double/xmin                                                  *
*   Description: The minimum x coordinate to consider                         *
*                                                                             *
*   Type/Name:   double/ymin                                                  *
*   Description: The minimum y coordinate to consider                         *
*                                                                             *
* RETURN:        The PSTPoint with minimum x-coordinate within given          *
*                boundaries.                                                  *
*                                                                             *
******************************************************************************/
    public PSTPoint leftMostNE(double xmin, double ymin) {
	PSTPoint best = new PSTPoint(Double.POSITIVE_INFINITY,
				     Double.POSITIVE_INFINITY);
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

/******************************************************************************
*                                                                             *
* FUNCTION NAME: highestNE                                                    *
*                                                                             *
* PURPOSE:       Determine the point with maximum y-coordinate among          *
*                all points {p ∈ P | xmin ≤ p.x ∧ ymin ≤ p.y}                 *
*                                                                             *
* PARAMETERS                                                                  *
*   Type/Name:   double/xmin                                                  *
*   Description: The minimum x coordinate to consider                         *
*                                                                             *
*   Type/Name:   double/ymin                                                  *
*   Description: The minimum y coordinate to consider                         *
*                                                                             *
* RETURN:        The PSTPoint with maximum y-coordinate within given          *
*                boundaries.                                                  *
*                                                                             *
* NOTES:         None.                                                        *
*                                                                             *
******************************************************************************/
    public PSTPoint highestNE(double xmin, double ymin) {
	PSTPoint best = new PSTPoint(Double.POSITIVE_INFINITY,
				     Double.NEGATIVE_INFINITY);
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

/******************************************************************************
*                                                                             *
* FUNCTION NAME: highest3Sided                                                *
*                                                                             *
* PURPOSE:       Determine the point with maximum y-coordinate among          *
*                all points {p ∈ P | xmin ≤ p.x ≤ xmax ∧ ymin ≤ p.y}          *
*                                                                             *
* PARAMETERS                                                                  *
*   Type/Name:   double/xmin                                                  *
*   Description: The minimum x coordinate to consider                         *
*                                                                             *
*   Type/Name:   double/xmax                                                  *
*   Description: The maximum x coordinate to consider                         *
*                                                                             *
*   Type/Name:   double/ymin                                                  *
*   Description: The minimum y coordinate to consider                         *
*                                                                             *
* RETURN:        The PSTPoint with maximum y-coordinate within given          *
*                boundaries.                                                  *
*                                                                             *
* NOTES:         None.                                                        *
*                                                                             *
******************************************************************************/
    public PSTPoint highest3Sided(double xmin, double xmax, double ymin) {
/******************************************************************************
* Initialization                                                              *
******************************************************************************/
	PSTPoint best = new PSTPoint(Double.POSITIVE_INFINITY,
				     Double.NEGATIVE_INFINITY);
	boolean L = false, R = false;
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
/******************************************************************************
* Traversal                                                                   *
******************************************************************************/
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
/******************************************************************************
*                                                                             *
* FUNCTION NAME: Enumerate3Sided                                              *
*                                                                             *
* PURPOSE:       Returns a list of points within a query region               *
*                bounded on 3 sides (a minimum and maximum x, and a           *
*                minimum y), but no maximum y value.                          *
*                                                                             *
* PARAMETERS                                                                  *
*   Type/Name:   double/xmin                                                  *
*   Description: Minimum x value.                                             *
*                                                                             *
*   Type/Name:   double/xmax                                                  *
*   Description: Maximum x value.                                             *
*                                                                             *
*   Type/Name:   double/ymin                                                  *
*   Description: Minimum y value.                                             *
*                                                                             *
* RETURN:        List<PSTPoint> of all points within boundaries.              *
*                                                                             *
******************************************************************************/
    public List<PSTPoint> enumerate3Sided(double xmin, double xmax, double ymin) {
/******************************************************************************
* Initialization                                                              *
******************************************************************************/
	ArrayList<PSTPoint> points = new ArrayList<PSTPoint>();
	int indexP = 1, indexPp = 1, indexQ = 1, indexQp = 1;
	PSTPoint root = getPoint(1);
	boolean
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
/******************************************************************************
* Traversal                                                                   *
******************************************************************************/
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
		    points.add(pp);
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
		    points.add(qp);
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
    private void explore(int indexP, double ymin, ArrayList<PSTPoint> points) {
	PSTPoint p = getPoint(indexP);
	// p is within query region
	if(p.getY() >= ymin) {
	    int indexC = indexP;
	    PSTPoint current;
	    int state = 0;
	    while((indexC != indexP) || (state != 2)) {
		current = getPoint(indexC);
		if(state == 0) {
		    points.add(current);
		    int indexCl = indexOfLeftChild(indexC);
		    if(numberOfChildren(indexC) > 0 &&
		       getPoint(indexCl).getY() >= ymin) {
			indexC = indexCl;
		    } else {
			state = 1;
		    }
		} else if(state == 1) {
		    int indexCr = indexOfRightChild(indexC);
		    if(numberOfChildren(indexC) == 2 &&
		       getPoint(indexCr).getY() >= ymin) {
			indexC = indexCr;
			state = 0;
		    } else {
			state = 2;
		    }
		} else { // state == 2 && current != p
		    if(isLeftChild(indexC)) {
			state = 1;
		    }
		    indexC = indexOfParent(indexC);
		}
	    }
	}
    }
/******************************************************************************
* Utility                                                                     *
******************************************************************************/
    private static int powerOf2(int x) {
	return (int)Math.pow(2,x);
    }
    private PSTPoint getPoint(int index) { // base 1
	return tree[baseZeroIndex(index)];
    }
    private void setPoint(int index,PSTPoint p) { // base 1
	tree[baseZeroIndex(index)] = p;
    }
    private boolean isLeaf(int index) { // base 1
	return numberOfChildren(index) == 0;
    }
    private int numberOfChildren(int index) { // base 1
	if(indexOfLeftChild(index) > tree.length) return 0;
	if(indexOfRightChild(index) > tree.length) return 1;
	return 2;
    }
    private static int indexOfParent(int index) { // base 1
	return index/2;
    }
    private static boolean isLeftChild(int index) {
	return isEven(index);
    }
    private static boolean isRightChild(int index) {
	return !isLeftChild(index);
    }
    private static boolean isOdd(int n) {
	return !isEven(n);
    }
    private static boolean isEven(int n) {
	return (n % 2) == 0;
    }
    private static int indexOfLeftChild(int index) { // base 1
	return (2*index);
    }
    private static int indexOfRightChild(int index) { // base 1
	return (2*index)+1;
    }
    private void swap(int i, int j) { // base 1
	PSTPoint temp;
	temp = getPoint(i);
	setPoint(i,getPoint(j));
	setPoint(j,temp);
    }
    public void printArray() {
	printArray(tree);
    }
    public static void printArray(PSTPoint[] points) {
	for(int i = 0; i < points.length; i++) System.out.print(points[i] + " ");
	System.out.println();
    }
    // Gives the base-2 logarithm of a double
    private static double log2(int x) {
	return Math.log(x) / Math.log(2);
    }
    private static int level(int index) {
	return (int)log2(index);
    }

/******************************************************************************
*                                                                             *
* FUNCTION NAME: baseZeroIndex                                                *
*                                                                             *
* PURPOSE:       Converts the index of a base one array into the index        *
*                of a base zero array.                                        *
*                                                                             *
* PARAMETERS                                                                  *
*   Type/Name:   int/baseOneIndex                                             *
*   Description: The index of an array with base one.                         *
*                                                                             *
* RETURN:        The integer equivalent index in an array with base zero.     *
*                                                                             *
******************************************************************************/
    private static int baseZeroIndex(int baseOneIndex) {
	return baseOneIndex - 1;
    }

/******************************************************************************
* Testing                                                                     *
******************************************************************************/
    public static void main(String[] args) {
	PSTPoint[] testPoints;
	StopWatch sw;
	long time;
	if(args.length < 1) {
	    testPoints = new PSTPoint[7];
	    testPoints[0]  = new PSTPoint(0,8);
	    testPoints[1]  = new PSTPoint(1,7);
	    testPoints[2]  = new PSTPoint(2,6);
	    testPoints[3]  = new PSTPoint(3,5);
	    testPoints[4]  = new PSTPoint(4,4);
	    testPoints[5]  = new PSTPoint(5,3);
	    testPoints[6]  = new PSTPoint(6,2);
	    System.out.print("Points: "); printArray(testPoints);
	    InPlacePST ippst = new InPlacePST(testPoints);
	    System.out.print("PST: "); ippst.printArray();
	} else {
	    System.out.println("Creating points...");
	    int n = Integer.parseInt(args[0]);
	    testPoints = new PSTPoint[n];
	    int count = 0;
	    for(int i = 0; i < n ; i++) {
		testPoints[count++] = new PSTPoint(i,n-i);
	    }
	    System.out.println("Building PST with " + n + " nodes...");
	    if(n < 20) {
		System.out.print("Points: "); printArray(testPoints);
	    }
	    sw = new StopWatch();
	    InPlacePST ippst = new InPlacePST(testPoints);
	    time = sw.stop(); // ms
	    System.out.println("Took: " + (time/1000));
	    if(n < 20) {
		System.out.print("PST: "); ippst.printArray();
	    }
	    // Test queries
	    System.out.println("leftMostNE(x=-10,y=-10):                "
			       + ippst.leftMostNE(-10,-10));
	    System.out.println("highestNE(x=1,y=-10):                   "
			       + ippst.highestNE(1,-10));
	    System.out.println("highest3Sided(xmin=4,xmax=5,ymin=0):    "
			       + ippst.highest3Sided(4,5,0));
	    System.out.print(  "enumerate3Sided(xmin=1,xmax=7,ymin=-8): ");
	    printArray(ippst.enumerate3Sided(1,7,-8).toArray(new PSTPoint[0]));
	}
    }

/******************************************************************************
* Stubs (for now)                                                             *
******************************************************************************/
    
    public double minYinRange(double minX, double maxX, double maxY)
	throws NoPointsInRangeException, NotImplementedException {
	throw new NotImplementedException();
    }
    public double minXinRange(double minX, double maxX, double maxY)
	throws NoPointsInRangeException, NotImplementedException {
	throw new NotImplementedException();
    }
    public double maxXinRange(double minX, double maxX, double maxY)
	throws NoPointsInRangeException, NotImplementedException {
	throw new NotImplementedException();
    }
    public double maxYinRange(double minX, double maxX, double maxY)
	throws NoPointsInRangeException, NotImplementedException {
	double highestY = (highest3Sided(minX,maxX,maxY)).getY();
	if(highestY == Double.NEGATIVE_INFINITY) throw new NoPointsInRangeException();
	return highestY;
    }
}
