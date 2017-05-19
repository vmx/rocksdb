/******************************************************************************
*                       Copyright (c) 2011 - 2012 by                          *
*                               Simon Pratt                                   *
*                         (All rights reserved)                               *
*******************************************************************************
*                                                                             *
* FILE:    ArrayPSTNode.java                                                  *
*                                                                             *
* MODULE:  Priority Search Tree                                               *
*                                                                             *
* NOTES:   See README for more information.                                   *
*                                                                             *
*          See LICENSE for license information.                               *
*                                                                             *
******************************************************************************/

import java.awt.geom.*;

public class ArrayPSTNode {
    private PSTPoint p;

    public ArrayPSTNode(double x, double y) {
	this(new PSTPoint(x,y));
    }

    public ArrayPSTNode(PSTPoint p) {
	this.p = p;
    }

    public double getX() { return p.getX(); }
    public double getY() { return p.getY(); }
    public PSTPoint getPoint() { return p; }
}