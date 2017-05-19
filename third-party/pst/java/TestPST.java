import java.util.*;
public class TestPST {
    public static void main(String[] args) 
	throws EmptyTreeException, NoPointsInRangeException, NotImplementedException {
	StopWatch sw;
	long time;
	PrioritySearchTree arrayPST, pointerPST, inPlacePST;
	System.out.println("Creating points...");
	ArrayList<PSTPoint> testPoints = new ArrayList<PSTPoint>();
	int n = Integer.parseInt(args[0]);
	int count = 0;
	for(int i = -n; i < n ; i++) {
	    if((i%2) == 0)
		testPoints.add(new PSTPoint(i,i+n));
	    else
		testPoints.add(new PSTPoint(-i,i+n));
	}
	ArrayList<PSTPoint> copyPoints = new ArrayList<PSTPoint>(testPoints);
	PSTPoint[] pointArray = testPoints.toArray(new PSTPoint[2*n]);
	System.out.println("Building PSTs with " + doubleToInt(2*n) + " nodes...");
	sw = new StopWatch();
	pointerPST = new PointerPST(testPoints);
	time = sw.stop();
	System.out.println("Pointer implementation took: " + time);
	// sw = new StopWatch();
	// arrayPST = new ArrayPST(copyPoints);
	// time = sw.stop();
	// System.out.println("Array implementation took: " + time);
	sw = new StopWatch();
	inPlacePST = new InPlacePST(pointArray);
	time = sw.stop();
	System.out.println("In-place implementation took: " + time);
	System.out.println();

	System.out.println("Testing pointer impementation...");
	testTime(pointerPST,n);
	System.out.println();

	// System.out.println("Testing array impementation...");
	// testTime(arrayPST,n);
	// System.out.println();

	System.out.println("Testing in-place impementation...");
	testTime(inPlacePST,n);
    } 
    private static void testTime(PrioritySearchTree pst, int n)
	throws EmptyTreeException, NoPointsInRangeException, NotImplementedException {
	// Find all points in range
	System.out.println("Finding all points in range...");
	StopWatch sw = new StopWatch();
	List<PSTPoint> testPoints = pst.enumerate3Sided(-n,n,n);
	long time = sw.stop();
	System.out.println("Took: " + time);

	// Find max/min x/y in range
	System.out.println("Finding max/min x/y in range...");
	double result;
	sw = new StopWatch();
	// result = pst.minYinRange(-n,n,n);
	// result = pst.minXinRange(-n,n,n);
	// result = pst.maxXinRange(-n,n,n);
	// should all be on same order, only need to test one
	result = pst.maxYinRange(-n,n,n); 
	time = sw.stop();
	System.out.println(result);
	System.out.println("Took: " + time);
    }
    private static int doubleToInt(double d) {
	return (new Double(d)).intValue();
    }
    // height of a balanced tree with n elements
    private static int treeHeight(int n) {
	return doubleToInt(Math.ceil(Math.log(n+1)/Math.log(2)));
    }
    // max number of heap nodes in a tree of given height
    private static int heapSize(int height) {
	return doubleToInt(Math.pow(2, height)-1);
    }
    // width of a tree at a given depth
    private static int width(int depth) {
	return doubleToInt(Math.pow(2,depth-1));
    }
    // amount of unused space allocated for a given number of nodes
    private static int waste(int n) {
	int height = treeHeight(n);
	return (width(height) - (n - heapSize(height-1)));
    }
    private static void printList(ArrayList<PSTPoint> points) {
	for(PSTPoint p : points) System.out.print(p + " ");
	System.out.println();
    }
    private static void report(int n) {
	System.out.println("Nodes: " + n);
	int height = treeHeight(n);
	System.out.println("Tree depth: " + height);
	int heapSize = heapSize(height);
	System.out.println("Heap size: " + heapSize);
	System.out.println("Width at max depth: " + width(height));
	System.out.println("Unused nodes: " + (heapSize - n));
    }
}