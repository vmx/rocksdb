# compile with debug information
ifeq ($(mode),release)
	CXXFLAGS=-O2 -Wall -Wextra -Wshadow -Wconversion
else
	mode = debug
	CXXFLAGS=-O0 -g -Wall -Wextra -Wshadow -Wconversion
endif

all: test_sort test_partition test_pst test_leda

# executables
test_leda:
	g++ $(CXXFLAGS) -L$(LEDAROOT) -I$(LEDAROOT)/incl test_leda.cc -lleda -lX11 -lm -o test_leda

test_pst: PSTPoint.o sort/heap_sort.o InPlacePST.o control_utilities.o array_utilities.o test_pst.cc

test_sort: PSTPoint.o array_utilities.o sort/insertion_sort.o sort/heap_sort.o test_sort.cc

test_partition: PSTPoint.o array_utilities.o sort/heap_sort.o sort/stable_01_sort.o test_partition.cc

# object files
InPlacePST.o: PSTPoint.o sort/heap_sort.o InPlacePST.h

sort/insertion_sort.o: PSTPoint.o array_utilities.o sort/insertion_sort.h

sort/heap_sort.o: PSTPoint.o array_utilities.o sort/heap_sort.h

sort/stable_01_sort.o: PSTPoint.o array_utilities.o sort/stable_01_sort.h

array_utilities.o: PSTPoint.o array_utilities.h

PSTPoint.o: PSTPoint.h

clean:
	@if [ -f test_leda ]; then rm test_leda; fi
	@if [ -f test_pst ]; then rm test_pst; fi
	@if [ -f test_sort ]; then rm test_sort; fi
	@if [ -f test_partition ]; then rm test_partition; fi
	@rm -f *.o # remove all object files and be quiet about it
