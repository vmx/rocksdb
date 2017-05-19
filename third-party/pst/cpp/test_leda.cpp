///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    test_leda.cpp                                                    //
//                                                                           //
// MODULE:  Priority Search Tree                                             //
//                                                                           //
// NOTES:   None.                                                            // 
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
#include <iostream>
#include <string>
#include <LEDA/geo/ps_tree.h>

using namespace std;
using namespace leda;

int main(int argv, char** argc) {
  const int MAX_POINTS_TO_DISPLAY = 12;
  time_t before, after;
  pair_item pit;
  /////////////////////////////////////////////////////////////////////////////
  // Setup                                                                   //
  /////////////////////////////////////////////////////////////////////////////
  ps_tree* pst = new ps_tree();
  /////////////////////////////////////////////////////////////////////////////
  // Insert points                                                           //
  /////////////////////////////////////////////////////////////////////////////
  int n = 1000000;
  if(argv >= 2) {
    n = atoi(argc[1]);
  }
  cout << "Inserting " << n << " points..." << flush;
  before = time(0);
  for(int i = 1; i < n; i++) {
    pst->insert(i,i);
  }
  after = time(0);
  cout << "took: " << (after - before) << endl;
  if(n <= 10) {
    cout << "Tree: " << endl;
    pst->pr_ps_tree();
  }
  /////////////////////////////////////////////////////////////////////////////
  // Wait for user input                                                     //
  /////////////////////////////////////////////////////////////////////////////
  std::string input;
  cout << "Press any key to continue..." << endl;
  getline(cin,input);
  /////////////////////////////////////////////////////////////////////////////
  // max x in rect                                                           //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Querying max x in rect..." << flush;
  before = time(0);
  pit = pst->max_x_in_rect(4,5,n);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  if(pit->valid)
    cout << "Found: (" << pit->x_pkt << "," << pit->y_pkt << ")" << endl;
  /////////////////////////////////////////////////////////////////////////////
  // enumerate                                                               //
  /////////////////////////////////////////////////////////////////////////////
  cout << "Enumerating 3 sided..." << flush;
  before = time(0);
  pst->enumerate(1,5,n);
  after = time(0);
  cout << "took: " << (after - before) << endl;
  /////////////////////////////////////////////////////////////////////////////
  // Print
  /////////////////////////////////////////////////////////////////////////////
  if(n <= MAX_POINTS_TO_DISPLAY)
    pst->pr_ps_tree();
  return 0;
}
