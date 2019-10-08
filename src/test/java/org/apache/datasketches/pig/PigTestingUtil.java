/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.pig;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.datasketches.Family;
import org.apache.datasketches.theta.UpdateSketch;

@SuppressWarnings("javadoc")
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
