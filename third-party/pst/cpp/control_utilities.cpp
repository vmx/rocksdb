///////////////////////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    control_utilities.cpp                                            //
//                                                                           //
// MODULE:  Control Utilities                                                //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////

#include <iostream>
#include <unistd.h>
#include <termios.h>
#include <stdio.h>
#include "control_utilities.h"

using namespace std;

namespace control_utilities {
  char waitForAnyKey() {
    cout << "Press any key to continue..." << flush;
    int ch;
    struct termios oldt;
    struct termios newt;
    tcgetattr(STDIN_FILENO, &oldt); // store old settings
    newt = oldt; // copy old settings to new settings
    newt.c_lflag &= ~(ICANON | ECHO); // make one change to old settings in new settings
    tcsetattr(STDIN_FILENO, TCSANOW, &newt); // apply the new settings immediatly
    ch = getchar(); // standard getchar call
    tcsetattr(STDIN_FILENO, TCSANOW, &oldt); // reapply the old settings
    cout << endl;
    return ch; // return received char
  }
}
