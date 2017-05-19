/******************************************************************************
*                       Copyright (c) 2011 - 2012 by                          *
*                               Simon Pratt                                   *
*                         (All rights reserved)                               *
*******************************************************************************
*                                                                             *
* FILE:    PointerPSTNode.java                                                *
*                                                                             *
* MODULE:  Priority Search Tree                                               *
*                                                                             *
* NOTES:   See README for more information.                                   *
*                                                                             *
*          See LICENSE for license information.                               *
*                                                                             *
******************************************************************************/

public class PointerPSTNode {
    private PSTPoint p;
    private PointerPSTNode leftChild, rightChild;

    public PointerPSTNode(double x, double y) {
	this(new PSTPoint(x,y));
    }

    public PointerPSTNode(PSTPoint p) {
	this.p = p;
    }

    public double getX() { return p.getX(); }
    public double getY() { return p.getY(); }
    public PSTPoint getPoint() { return p; }
    public PointerPSTNode getLeftChild() { return leftChild; }
    public PointerPSTNode getRightChild() { return rightChild; }
    public void setLeftChild(PointerPSTNode p) {
	this.leftChild = p;
    }
    public void setRightChild(PointerPSTNode p) {
	this.rightChild = p;
    }
}