/******************************************************************************
*                       Copyright (c) 2011 - 2012 by                          *
*                               Simon Pratt                                   *
*                         (All rights reserved)                               *
*******************************************************************************
*                                                                             *
* FILE:    ArrayPST.java                                                      *
*                                                                             *
* MODULE:  Priority Search Tree                                               *
*                                                                             *
* NOTES:   A priority search tree is a tree data structure which stores       *
*          a set of coordinates in sorted order.  The root element of         *
*          the tree is the point with highest Y value.  The rest of           *
*          the points are divided into two sets, one set having               *
*          smaller x values than the median, the other having higher.         *
*                                                                             *
*          See README for more information.                                   *
*                                                                             *
*          See LICENSE for license information.                               *
*                                                                             *
******************************************************************************/

import java.awt.geom.*;
import java.util.*;

public class ArrayPST implements PrioritySearchTree {
    ArrayPSTNode[] heap;

/******************************************************************************
* The worst case for space is when there are 2^m nodes, for some m.           *
* In which case, O(2^(logn) - 1) extra space is allocated.                    *
******************************************************************************/
    public ArrayPST(ArrayList<PSTPoint> points) {
	if(points == null) return;
	Collections.sort(points); // Sort by y-coordinate in decreasing order
	this.heap = new ArrayPSTNode[heapSize(treeHeight(points.size()))];
	buildTree(0,points);
    }
/******************************************************************************
* Given a root index and a list of valid points P ordered by                  *
* y-coordinate in decreasing order, determines a median which bisects         *
* the remaining points, then builds:                                          *
*                                                                             *
*   root: point with lowest y-value                                           *
*   left child:  {p ∈ (P - root) | p.x <= medianX}                            *
*   right child: {p ∈ (P - root) | p.x >  medianX}                            *
*                                                                             *
* Note: points are also assumed to have distinct coordinates, i.e. no         *
*       two points have the same x coordinate and no two points have          *
*       the same y coordinate.                                                *
*                                                                             *
*       While this may seem unrealistic, we can convert any indistinct        *
*       coordinates by replacing all real coordinates with distinct           *
*       coordinates from the composite-number space without any loss          *
*       of generality.  See: Computational Geometry: Applications and         *
*       Algorithms, de Berg et al.  Section 5.5.                              *
*                                                                             *
******************************************************************************/
    private void buildTree(int rootIndex, ArrayList<PSTPoint> points) {
	if(points == null || points.size() < 1) return;
	// Since points are ordered by y decreasing, largest is first
	PSTPoint rootPoint = points.remove(0);
	// Find median X value
	//  - uses average X value of non-root points
	double sumX = 0.0d;
	for(PSTPoint p : points)
	    sumX += p.getX();
	double medianX = sumX/points.size();
	// Set the root node
	heap[rootIndex] = new ArrayPSTNode(rootPoint);
	// Bisect the non-root points into two arrays above and below the median
	ArrayList<PSTPoint> upperPoints = new ArrayList<PSTPoint>();
	ArrayList<PSTPoint> lowerPoints = new ArrayList<PSTPoint>();
	for(PSTPoint p : points) {
	    // note: if p.x is equal to median, it will be added to left child
	    if(p.getX() <= medianX) lowerPoints.add(p);
	    else upperPoints.add(p);
	}
	if(lowerPoints.size() > 0)
	    buildTree(indexOfLeftChild(rootIndex),lowerPoints);
	if(upperPoints.size() > 0)
	    buildTree(indexOfRightChild(rootIndex),upperPoints);
    }
/******************************************************************************
*                                                                             *
* Find all points within the region bounded by (minX,minY) and (maxX,minY)    *
*                                                                             *
*             /|\      /|\                                                    *
*              |        |                                                     *
*              |        |                                                     *
*              |        |                                                     *
*  (minX,minY) +--------+ (maxX,minY)                                         *
*                                                                             *
* Assumes maxX > minX and minY                                                *
* Choose minX,maxX,minY appropriately.                                        *
*                                                                             *
******************************************************************************/
    public List<PSTPoint> enumerate3Sided(double minX, 
					  double maxX, double minY)
	throws EmptyTreeException {
	return enumerate3Sided(minX,maxX,minY,
			       new ArrayList<PSTPoint>(),0);
    }
    // Note that as minY and maxX approach positive infinity and
    // minX approaches negative infinity, this search visits more nodes.
    // In the worst case, all nodes are visited.
    private List<PSTPoint> enumerate3Sided(double minX,
					   double maxX, double minY,
					   ArrayList<PSTPoint> list,
					   int index)
	throws EmptyTreeException {
	ArrayPSTNode node = heap[index];
	if(node == null) return list;
	if(node.getY() >= minY) {
	    double nodeX = node.getX();
	    if(nodeX >= minX && nodeX <= maxX) { 
		list.add(node.getPoint());
	    }
	    if(isValidNode(indexOfLeftChild(index))) {
		double nodeR = maxX(index);
		// nodeR >= points in left tree >= minX
		if(nodeR >= minX)
		    enumerate3Sided(minX,maxX,minY,list,
					indexOfLeftChild(index));
		// nodeR < points in right tree <= maxX
		if(nodeR < maxX) 
		    enumerate3Sided(minX,maxX,minY,list,
					indexOfRightChild(index));
	    }
	}
	return list;
    }
/******************************************************************************
* Other query functions                                                       *
******************************************************************************/
    public double maxYinRange(double minX, double maxX, double minY)
	throws NoPointsInRangeException {
	double max = maxYinRange(minX,maxX,minY,0);
	if(max > Double.NEGATIVE_INFINITY) return max;
	throw new NoPointsInRangeException();
    }
    private double maxYinRange(double minX, double maxX,double minY, int index) {
	ArrayPSTNode node = heap[index];
	if(node == null || node.getY() < minY) return Double.NEGATIVE_INFINITY;
	double nodeX = node.getX();
	if(nodeX >= minX && nodeX <= maxX) return node.getY();
	if(isValidNode(indexOfLeftChild(index))) {
	    double nodeR = maxX(indexOfLeftChild(index));
	    // nodeR >= points in left tree >= minX
	    if(nodeR >= minX && 
	       nodeR < maxX && isValidNode(indexOfRightChild(index))) {
		double maxLeft = maxYinRange(minX,maxX,minY,indexOfLeftChild(index));
		double maxRight = maxYinRange(minX,maxX,minY,indexOfRightChild(index));
		return (maxLeft < maxRight ? maxLeft : maxRight);
	    } else if(nodeR >= minX) {
		return maxYinRange(minX,maxX,minY,indexOfLeftChild(index));
	    } else if(nodeR < maxX && isValidNode(indexOfRightChild(index))) {
		return maxYinRange(minX,maxX,minY,indexOfRightChild(index));
	    }
	}
	return Double.NEGATIVE_INFINITY;
    }
    public double minXinRange(double minX, double maxX, double minY)
	throws NoPointsInRangeException {
	double min = minXinRange(minX,maxX,minY,0);
	if(min < Double.POSITIVE_INFINITY) return min;
	throw new NoPointsInRangeException();
    }
    private double minXinRange(double minX, double maxX, double minY, int index) {
	ArrayPSTNode node = heap[index];
	if(node == null || node.getY() < minY)
	    return Double.POSITIVE_INFINITY;
	double min = Double.POSITIVE_INFINITY;
	double nodeX = node.getX();
	if(minX <= nodeX && nodeX <= maxX)
	    min = nodeX;
	if(isValidNode(indexOfLeftChild(index))) {
	    double nodeR = maxX(indexOfLeftChild(index));
	    if(nodeR >= minX) {
		double minLeft = minXinRange(minX,maxX,minY,indexOfLeftChild(index));
		if(minLeft < min) min = minLeft;
	    }
	    if(nodeR < maxX && isValidNode(indexOfRightChild(index))) {
		double minRight = minXinRange(minX,maxX,minY,indexOfRightChild(index));
		if(minRight < min) min = minRight;
	    }
	}
	return min;
    }
    public double maxXinRange(double minX, double maxX, double minY)
	throws NoPointsInRangeException {
	double max = maxXinRange(minX,maxX,minY,0);
	if(max > Double.NEGATIVE_INFINITY) return max;
	throw new NoPointsInRangeException();
    }
    private double maxXinRange(double minX, double maxX, double minY, int index) {
	ArrayPSTNode node = heap[index];
	if(node == null || node.getY() < minY)
	    return Double.NEGATIVE_INFINITY;
	double max = Double.NEGATIVE_INFINITY;
	double nodeX = node.getX();
	if(minX <= nodeX && nodeX <= maxX)
	    max = nodeX;
	if(isValidNode(indexOfLeftChild(index))) {
	    double nodeR = maxX(indexOfLeftChild(index));
	    if(nodeR >= minX) {
		double maxLeft = maxXinRange(minX,maxX,minY,indexOfLeftChild(index));
		if(maxLeft > max) max = maxLeft;
	    }
	    if(nodeR < maxX && isValidNode(indexOfRightChild(index))) {
		double maxRight = maxXinRange(minX,maxX,minY,indexOfRightChild(index));
		if(maxRight > max) max = maxRight;
	    }
	}
	return max;
    }
    public double minYinRange(double minX, double maxX, double minY)
	throws NoPointsInRangeException {
	double min = minYinRange(minX,maxX,minY,0);
	if(min < Double.POSITIVE_INFINITY) return min;
	throw new NoPointsInRangeException();
    }
    private double minYinRange(double minX, double maxX, double minY, int index) {
	ArrayPSTNode node = heap[index];
	if(node == null || node.getY() < minY)
	    return Double.POSITIVE_INFINITY;
	double min = Double.POSITIVE_INFINITY;
	double nodeX = node.getX();
	if(minX <= nodeX && nodeX <= maxX)
	    min = node.getY();
	if(isValidNode(indexOfLeftChild(index))) {
	    double nodeR = maxX(indexOfLeftChild(index));
	    if(nodeR >= minX) {
		double minLeft = minYinRange(minX,maxX,minY,indexOfLeftChild(index));
		if(minLeft < min) min = minLeft;
	    }
	    if(nodeR < maxX && isValidNode(indexOfRightChild(index))) {
		double minRight = minYinRange(minX,maxX,minY,indexOfRightChild(index));
		if(minRight < min) min = minRight;
	    }
	}
	return min;
    }
	
/******************************************************************************
* Whole-tree query functions                                                  *
******************************************************************************/
    public double minX() throws EmptyTreeException {
	int index = 0;
	if(heap[index] == null) throw new EmptyTreeException();
	double min = heap[index].getX();
	while(isValidNode(indexOfLeftChild(index))) {
	    index = indexOfLeftChild(index);
	    if(heap[index].getX() < min)
		min = heap[index].getX();
	}
	return min;
    }
    public double maxX() {
	return maxX(0);
    }
    private double maxX(int index) {
	double max = heap[index].getX();
	while(isValidNode(indexOfRightChild(index))) {
	    index = indexOfRightChild(index);
	    if(heap[index].getX() > max)
		max = heap[index].getX();
	}
	// Since a leaf without a sibling is always left
	// we have to check the last left child just in case
	if(isValidNode(indexOfLeftChild(index)) &&
	   heap[indexOfLeftChild(index)].getX() > max)
	    max = heap[indexOfLeftChild(index)].getX();
	return max;
    }
    public double maxY() throws EmptyTreeException {
	if(heap[0] == null) throw new EmptyTreeException();
	return heap[0].getY();
    }
    public double minY() throws EmptyTreeException {
	if(heap[0] == null) throw new EmptyTreeException();
	return minY(0);
    }
    private double minY(int index) {
	double min = heap[index].getY();
	if(isValidNode(indexOfRightChild(index)) &&
	   isValidNode(indexOfLeftChild(index))) {
		double minLeft = minY(indexOfLeftChild(index));
		double minRight = minY(indexOfRightChild(index));
		if(minLeft < minRight) min = minLeft;
		else min = minRight;
	} else if(isValidNode(indexOfRightChild(index))) {
		min = minY(indexOfRightChild(index));
	} else if(isValidNode(indexOfLeftChild(index))) {
		min = minY(indexOfLeftChild(index));
	}
	return min;
    }
/******************************************************************************
* Utility Functions                                                           *
******************************************************************************/
    private boolean isValidNode(int index) {
	return index < heap.length && heap[index] != null;
    }
    // height of a balanced tree with n elements
    private static int treeHeight(int n) {
	return doubleToInt(Math.ceil(Math.log(n+1)/Math.log(2)));
    }
    // max number of heap nodes in a tree of given height
    private static int heapSize(int height) {
	return doubleToInt(Math.pow(2, height)-1);
    }
    private static int indexOfLeftChild(int rootIndex) {
	return (2*rootIndex)+1;
    }
    private static int indexOfRightChild(int rootIndex) {
	return (2*rootIndex)+2;
    }
    private static int doubleToInt(double d) {
	return (new Double(d)).intValue();
    }
}