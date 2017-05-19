import java.util.*;

public class Sort {
/******************************************************************************
* Insertion Sort                                                              *
******************************************************************************/
    public static void insertionSort(PSTPoint[] array) {
	insertionSort(array,0,array.length-1);
    }
    public static void insertionSort(PSTPoint[] array, int beginIndex, int endIndex) {
	PSTPoint p;
	int j;
	boolean done;
	for(int i = beginIndex +1; i <= endIndex; i++) {
	    p = array[i];
	    j = i -1;
	    done = false;
	    while(!done) {
		if(array[j].xGreaterThan(p)) {
		    array[j+1] = array[j];
		    j--;
		    if(j < beginIndex)
			done = true;
		} else {
		    done = true;
		}
	    }
	    array[j+1] = p;
	}
    }
/******************************************************************************
* Heap Sort                                                                   *
*                                                                             *
* Very helpful:                                                               *
*  http://www.iti.fh-flensburg.de/lang/algorithmen/sortieren/heap/heapen.htm  *
******************************************************************************/
    public static void heapSort(PSTPoint[] array) {
	heapSort(array,0,array.length-1);
    }
    // Sorts the elements of array between beginIndex and endIndex
    // (ignoring all other elements)
    public static void heapSort(PSTPoint[] array, int beginIndex, int endIndex) {
	// First arrange the array into a heap (root element is always higher
	// than both child elements
	buildHeap(array,beginIndex,endIndex);
	while(endIndex > beginIndex) {
	    // Since the highest is first, move it to the end
	    swap(array,beginIndex,endIndex);
	    // Now that the highest element is last, it is sorted so don't
	    // touch it again
	    endIndex--;
	    // Since the smallest element is now first, rebuild the heap
	    downHeap(array,0,beginIndex,endIndex);
	}
    }
    // builds a heap from bottom up, starting at right-most lowest level
    // and traversing up the heap in reverse order (right to left, bottom to top)
    private static void buildHeap(PSTPoint[] array, int beginIndex, int endIndex) {
	int n = 1+endIndex - beginIndex;
	for(int v = n/2-1; v >= 0; v--)
	    downHeap(array,v,beginIndex,endIndex);
    }
/******************************************************************************
*                                                                             *
* FUNCTION NAME: downHeap                                                     *
*                                                                             *
* PURPOSE:       builds a heap from top down                                  *
*                                                                             *
* PARAMETERS                                                                  *
*   Type/Name:   PSTPoint[]/array                                             *
*   Description: The array of points                                          *
*                                                                             *
*   Type/Name:   int/v                                                        *
*   Description: The offset from beginIndex from which to begin               *
*                building the heap.                                           *
*                                                                             *
*   Type/Name:   int/beginIndex                                               *
*   Description: We don't assume that the heap begins at element 0,           *
*                instead an index of the first element in the heap            *
*                must be provided.                                            *
*                                                                             *
*   Type/Name:   int/endIndex                                                 *
*   Description: We don't assume that the heap continues to array.length-1,   *
*                instead an index of the last element in the heap must        *
*                be provided.                                                 *
*                                                                             *
* RETURN:        Void.                                                        *
*                                                                             *
* NOTES:         Note that indices are base zero, i.e. the first              *
*                element has index 0, the second element has index 1...       *
*                                                                             *
******************************************************************************/
    
    private static void downHeap(PSTPoint[] array, int v,
				 int beginIndex, int endIndex) {
	int w = leftChildOf(v);
	// invariant: element at index v has a left child
	while(beginIndex + w <= endIndex) {
	    // if left child has a sibling
	    if(beginIndex + w+1 <= endIndex)
		// if right child is greater than left child
		if(array[beginIndex + w+1].xGreaterThan(array[beginIndex + w]))
		    // use right child
		    w++;
	    // if larger child is less than its parent
	    if(!(array[beginIndex + w].xGreaterThan(array[beginIndex + v])))
		return;
	    // otherwise, swap child and parent
	    swap(array,beginIndex + w,beginIndex + v);
	    // continue with child
	    v = w;
	    w = leftChildOf(v);
	}
    }
    private static int leftChildOf(int index) {
	return 2*index+1;
    }
    private static int rightChildOf(int index) {
	return 2*index+2;
    }
/******************************************************************************
* Testing                                                                     *
******************************************************************************/
    public static void main(String[] args) {
	int n = 1000;
	if(args.length > 0) n = Integer.parseInt(args[0]);
	System.out.println("Creating " + (2*n) + " points...");
	ArrayList<PSTPoint> testPoints = new ArrayList<PSTPoint>();
	for(int i = -n; i < n ; i++) {
	    if((i%2) == 0)
		testPoints.add(new PSTPoint(i,i+n));
	    else
		testPoints.add(new PSTPoint(-i,-(i+n)));
	}
	PSTPoint[] copyPoints, oldPoints;
	StopWatch sw;
	long time;
	// heap sort
	System.out.println("HEAP SORT ON FULL ARRAY");
	copyPoints = testPoints.toArray(new PSTPoint[2*n]);
	if((n*2) < 7) {
	    System.out.print("Before: "); printArray(copyPoints);
	}
	sw = new StopWatch();
	heapSort(copyPoints);
	time = sw.stop();
	System.out.println("Took: " + time);
	if((n*2) < 7) {
	    System.out.print("After: "); printArray(copyPoints);
	}
	System.out.println("HEAP SORT ON ARRAY FROM " + n + " TO " + (2*n-1));
	copyPoints = testPoints.toArray(new PSTPoint[2*n]);
	if((n*2) < 7) {
	    System.out.print("Before: "); printArray(copyPoints);
	}
	sw = new StopWatch();
	heapSort(copyPoints,n,2*n-1);
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
    private static void swap(PSTPoint[] array,int a, int b) {
	PSTPoint temp = array[a];
	array[a] = array[b];
	array[b] = temp;
    }
}