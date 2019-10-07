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

package org.apache.datasketches.pig.theta;

import static org.apache.datasketches.pig.theta.PigUtil.compactOrderedSketchToTuple;
import static org.apache.datasketches.pig.theta.PigUtil.extractTypeAtIndex;
import static org.testng.Assert.assertNull;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;

@SuppressWarnings("javadoc")
public class PigUtilTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkCompOrdSketchToTuple() {
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(16).build();
    for (int i=0; i<16; i++) {
      usk.update(i);
    }
    CompactSketch csk = usk.compact(false, null);
    compactOrderedSketchToTuple(csk);
  }

  @Test
  public void checkExtractTypeAtIndex() {
    Tuple tuple = TupleFactory.getInstance().newTuple(0);
    assertNull(extractTypeAtIndex(tuple, 0));
  }

  @Test
  public void printlnTest() {
    println(this.getClass().getSimpleName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
