//-*- mode: c++ -*-////////////////////////////////////////////////////////////
//                       Copyright (c) 2011 - 2012 by                        //
//                                Simon Pratt                                //
//                           (All rights reserved)                           //
///////////////////////////////////////////////////////////////////////////////
//                                                                           //
// FILE:    control_utilities.h                                              //
//                                                                           //
// MODULE:  Control Utilities                                                //
//                                                                           //
// PURPOSE: Provides a simple set of control functions                       //
//                                                                           //
// NOTES:   None.                                                            //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////
// Public Variable:                     Description:                         //
// ----------------                     ------------                         //
///////////////////////////////////////////////////////////////////////////////
//                             Public Methods:                               //
// char waitForEnter()                                                       //
///////////////////////////////////////////////////////////////////////////////
#ifndef CONTROL_UTILITIES_H
#define CONTROL_UTILITIES_H

namespace control_utilities {
  /////////////////////////////////////////////////////////////////////////////
  //                                                                         //
  // FUNCTION NAME: waitForEnter                                             //
  //                                                                         //
  // PURPOSE:       Prints the message "Press any key to continue...",       //
  //                then blocks until the user presses a key.                //
  //                                                                         //
  // PARAMETERS                                                              //
  //   Type/Name:   Void.                                                    //
  //   Description: None.                                                    //
  //                                                                         //
  // RETURN:        The character pressed.                                   //
  //                                                                         //
  // NOTES:         None.                                                    //
  //                                                                         //
  /////////////////////////////////////////////////////////////////////////////
  char waitForAnyKey();
}

#endif
