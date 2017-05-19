import java.util.*;

public class Partition {
/******************************************************************************
* As described in the paper "Stable Minimum Space Partitioning in             *
* Linear Time" by Jyrki Katajainen and Tomi Pasanen.                          *
******************************************************************************/
    private static void stableInPlace01Sort(PSTPoint[] array, int beginIndex,
					    int endIndex, PSTPoint s) {
	
    }
    // time: O(n)
    // extra space:
    //    - n bits
    //    - n + O(1) counters, each requiring:
    //       - ceil(log(n+1)) bits
    private static void algorithmA(PSTPoint[] array, PSTPoint s) {
	algorithmA(array,0,array.length-1,s);
    }
    private static void algorithmA(PSTPoint[] array, int beginIndex, int endIndex,
				   PSTPoint s) {
	int n = 1 + endIndex - beginIndex;
	if(n == 1) return;
	int zeroesTotal = 0;
	for(int i = beginIndex; i <= endIndex; i++)
	    if(isZero(array[i],s))
		zeroesTotal++;
	int c0 = 0, c1 = 0;
	int[] rank = new int[n];
	boolean[] inPlace = new boolean[n];
	for(int i = 0; i < n; i++) {
	    if(isZero(array[beginIndex + i],s)) {
		rank[i] = c0;
		c0++;
	    } else { // is one
		rank[i] = zeroesTotal + c1;
		c1++;
	    }
	    if(i == rank[i]) inPlace[i] = true;
	    else inPlace[i] = false;
	}
	// permute elements to their final positions
	for(int i = 0; i < n; i++) {
	    while(!inPlace[i]) {
		swap(array,beginIndex + i,beginIndex + rank[i]);
		inPlace[rank[i]] = true;
		rank[i] = rank[rank[i]];
		if(i == rank[i]) inPlace[i] = true;
	    }
	}
    }
    // finds a k such that 2^k <= n <= 2^k+1 
    private static int findK(int start, int end) {
	return (int)log2(1+ end - start);
    }
/******************************************************************************
* Stable 0-1 partitioning - super simple algorithm                            *
******************************************************************************/
    private static void stableInPlace01Partition(PSTPoint[] array, PSTPoint s) {
	stableInPlace01Partition(array,0,array.length-1,s);
    }
    // Partitions the array from beginIndex to endIndex (inclusive)
    // using s as a pivot, uses base 0
    private static void stableInPlace01Partition(PSTPoint[] array, int beginIndex,
						 int endIndex, PSTPoint s) {
	int n = 1 + endIndex - beginIndex;
	if(n == 1) return;
	int countZeroes = 0;
	for(int i = beginIndex; i <= endIndex; i++)
	    if(isZero(array[i],s))
		countZeroes++;
	// Step 1: form internal buffer
	int zeroEnd = endIndex;
	int oneEnd;
	do {
	    // find the preceding zero
	    while(zeroEnd > beginIndex && isOne(array[zeroEnd],s)) zeroEnd--;
	    if(zeroEnd <= beginIndex) return;
	    // find the zero immediately following the preceding one
	    oneEnd = zeroEnd-1;
	    while(oneEnd > beginIndex && isZero(array[oneEnd],s)) oneEnd--;
	    if(oneEnd < beginIndex) return;
	    // find the one immediately following the preceding zero
	    int oneStart = oneEnd;
	    while(oneStart > beginIndex && isOne(array[oneStart-1],s)) oneStart--;
	    if(isZero(array[oneStart],s)) return;
	    // finally, permute the block of zeroes with the block of ones
	    blockPermute(array,oneStart,oneEnd,zeroEnd);
	} while((zeroEnd-oneEnd) < countZeroes);
    }

/******************************************************************************
* Testing                                                                     *
******************************************************************************/
    public static void main(String[] args) {
	int n = 1000;
	if(args.length > 0) n = Integer.parseInt(args[0]);
	System.out.println("Creating " + (2*n) + " points...");
	ArrayList<PSTPoint> testPoints = new ArrayList<PSTPoint>();
	PSTPoint pivot = new PSTPoint(0,n);
	for(int i = -n; i < n ; i++) {
	    if(i == 0)
		continue;
	    else if((i%2) == 0)
		testPoints.add(new PSTPoint(i,i+n));
	    else
		testPoints.add(new PSTPoint(-i,-(i+n)));
	}
	PSTPoint[] copyPoints, oldPoints;
	StopWatch sw;
	long time;
	boolean same;
	// algorithmA
	System.out.println("ALGORITHM A");
	copyPoints = testPoints.toArray(new PSTPoint[2*n-1]);
	if((n*2) < 7) {
	    System.out.print("Before: "); printArray(copyPoints);
	}
	sw = new StopWatch();
	algorithmA(copyPoints,pivot);
	time = sw.stop();
	System.out.println("Took: " + time);
	if((n*2) < 7) {
	    System.out.print("After: "); printArray(copyPoints);
	}
    }
/******************************************************************************
* Utility                                                                     *
******************************************************************************/
    public static void printArray(PSTPoint[] points) {
	for(int i = 0; i < points.length; i++) System.out.print(points[i] + " ");
	System.out.println();
    }
    // Gives the base-2 logarithm of a double
    private static double log2(int x) {
	return Math.log(x) / Math.log(2);
    } 
    // moves all elements between beginA and endA (inclusive) past
    // all elements between endA+1 and endB, and vice versa
    private static void blockPermute(PSTPoint[] array, int beginA, int endA, int endB) {
	if(beginA == endB) return;
	// reverse A
	reverse(array,beginA,endA);
	// reverse B
	reverse(array,endA +1, endB);
	// reverse A+B
	reverse(array,beginA,endB);
    }
    // reverses the order of elements between begin and end (inclusive)
    private static void reverse(PSTPoint[] array, int begin, int end) {
	while(begin < end)
	    swap(array,begin++,end--);
    }
    private static void swap(PSTPoint[] array, int indexA, int indexB) {
	PSTPoint temp = array[indexA];
	array[indexA] = array[indexB];
	array[indexB] = temp;
    }
    // returns true if point at index is less than s (conceptually a zero)
    private static boolean isZero(PSTPoint p, PSTPoint s) {
	return p.getX() < s.getX();
    }
    private static boolean isOne(PSTPoint p, PSTPoint s) {
	return !isZero(p,s);
    }
}