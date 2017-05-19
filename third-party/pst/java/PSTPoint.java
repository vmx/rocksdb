/******************************************************************************
*                       Copyright (c) 2011 - 2012 by                          *
*                               Simon Pratt                                   *
*                         (All rights reserved)                               *
*******************************************************************************
*                                                                             *
* FILE:    PSTPoint.java                                                      *
*                                                                             *
* MODULE:  Priority Search Tree                                               *
*                                                                             *
* NOTES:   See README for more information.                                   *
*                                                                             *
*          See LICENSE for license information.                               *
*                                                                             *
******************************************************************************/

import java.awt.geom.*;

public class PSTPoint implements Comparable<PSTPoint> {
    private Point2D.Double p;

    public PSTPoint(double x, double y) {
	this.p = new Point2D.Double(x,y);
    }

    public int compareTo(PSTPoint p) {
	if(yLessThan(p)) return 1;
	else if(yGreaterThan(p)) return -1;
	return 0;
    }
    public boolean yLessThan(PSTPoint p) {
	return this.getY() < p.getY();
    }
    public boolean yGreaterThan(PSTPoint p) {
	return this.getY() > p.getY();
    }
    public boolean xLessThan(PSTPoint p) {
	return this.getX() < p.getX();
    }
    public boolean xGreaterThan(PSTPoint p) {
	return this.getX() > p.getX();
    }
    
    public double getX() { return p.getX(); }
    public double getY() { return p.getY(); }
    public String toString() { return "(" + p.getX() + "," + p.getY() + ")"; }
}