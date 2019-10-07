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

package org.apache.datasketches.pig.kll;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;

import org.apache.datasketches.kll.KllFloatsSketch;

import org.testng.Assert;

@SuppressWarnings("javadoc")
public class GetRankTest {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  @Test
  public void emptySketch() throws Exception {
    final EvalFunc<Double> func = new GetRank();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    final Double result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0f)));
    Assert.assertEquals(result, Double.NaN);
  }

  @Test
  public void normalCase() throws Exception {
    final EvalFunc<Double> func = new GetRank();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    final Double result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 3f)));
    Assert.assertEquals(result, 0.5);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongNumberOfInputs() throws Exception {
    final EvalFunc<Double> func = new GetRank();
    func.exec(TUPLE_FACTORY.newTuple(1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    final EvalFunc<Double> func = new GetRank();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0, 1.0)));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForValue() throws Exception {
    final EvalFunc<Double> func = new GetRank();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 1)));
  }

}
