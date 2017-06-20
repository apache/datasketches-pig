/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * @author Lee Rhodes
 */
public class PigTestingUtil {
  public static final String LS = System.getProperty("line.separator");
  
  
  /**
   * Returns a tuple constructed from the given array of objects.
   * 
   * @param in Array of objects.
   * @throws ExecException this is thrown by Pig
   * @return tuple
   */
  public static Tuple createTupleFromArray(Object[] in) throws ExecException {
    int len = in.length;
    Tuple tuple = TupleFactory.getInstance().newTuple(len);
    for (int i = 0; i < in.length; i++ ) {
      tuple.set(i, in[i]);
    }
    return tuple;
  }
  
  /**
   * Returns a Pig DataByteArray constructed from a QuickSelectSketch.
   * 
   * @param nomSize of the Sketch. Note, minimum size is 16. 
   * Cache size will autoscale from a minimum of 16.
   * @param start start value
   * @param numValues number of values in the range
   * @return DataByteArray
   */
  public static DataByteArray createDbaFromQssRange(int nomSize, int start, int numValues) {
    UpdateSketch skA = UpdateSketch.builder().setNominalEntries(nomSize).build();
    for (int i = start; i < (start + numValues); i++ ) {
      skA.update(i);
    }
    byte[] byteArr = skA.compact(true, null).toByteArray();
    return new DataByteArray(byteArr);
  }
  
  /**
   * Returns a Pig DataByteArray constructed from a AlphaSketch.
   * 
   * @param nomSize of the Sketch. Note, minimum nominal size is 512.  
   * Cache size will autoscale from a minimum of 512.
   * @param start start value
   * @param numValues number of values in the range
   * @return DataByteArray
   */
  public static DataByteArray createDbaFromAlphaRange(int nomSize, int start, int numValues) {
    UpdateSketch skA = UpdateSketch.builder().setFamily(Family.ALPHA)
            .setNominalEntries(nomSize).build();
    for (int i = start; i < (start + numValues); i++ ) {
      skA.update(i);
    }
    byte[] byteArr = skA.compact(true, null).toByteArray();
    return new DataByteArray(byteArr);
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s);
  }
  
}
