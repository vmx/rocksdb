public interface PrioritySearchTree {
    public java.util.List<PSTPoint> enumerate3Sided(double minX,
						   double maxX,
						   double maxY)
	throws EmptyTreeException;
    public double minYinRange(double minX, double maxX, double maxY)
	throws NoPointsInRangeException, NotImplementedException;
    public double minXinRange(double minX, double maxX, double maxY)
	throws NoPointsInRangeException, NotImplementedException;
    public double maxXinRange(double minX, double maxX, double maxY)
	throws NoPointsInRangeException, NotImplementedException;
    public double maxYinRange(double minX, double maxX, double maxY)
	throws NoPointsInRangeException, NotImplementedException;
}