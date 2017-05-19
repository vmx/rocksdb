/******************************************************************************
*                       Copyright (c) 2011 - 2012 by                          *
*                               Simon Pratt                                   *
*                         (All rights reserved)                               *
*******************************************************************************
*                                                                             *
* FILE:    StopWatch.java                                                     *
*                                                                             *
* NOTES:   Used to take measurements of time.                                 *
*                                                                             *
******************************************************************************/

import java.util.*;

public class StopWatch {
    private Date start;
    public StopWatch() {
	start = new Date();
    }
    public long stop() {
	return (new Date()).getTime() - start.getTime();
    }
}