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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class EstimateTest {

  @Test
  public void testNullEmpty() throws IOException {
    EvalFunc<Double> func = new Estimate();
    Tuple inputTuple = null;
    Double returnValue = func.exec(inputTuple);
    assertNull(returnValue);

    inputTuple = TupleFactory.getInstance().newTuple(0);
    returnValue = func.exec(inputTuple);
    assertNull(returnValue);
  }

  @Test
  public void testExact() throws IOException {
    EvalFunc<Double> func = new Estimate();

    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromQssRange(64, 0, 64));

    Double result = func.exec(dataTuple);
    assertNotNull(result);
    assertEquals(result, 64.0, 0.0);
  }

  @Test
  public void testExactWithSeed() throws IOException {
    EvalFunc<Double> func = new Estimate(Long.toString(DEFAULT_UPDATE_SEED));

    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromQssRange(64, 0, 64));

    Double result = func.exec(dataTuple);
    assertNotNull(result);
    assertEquals(result, 64.0, 0.0);
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
