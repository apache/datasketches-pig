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

package org.apache.datasketches.pig.quantiles;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;
import org.testng.Assert;

@SuppressWarnings("javadoc")
public class GetQuantileFromDoublesSketchTest {
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void emptySketch() throws Exception {
    EvalFunc<Double> func = new GetQuantileFromDoublesSketch();
    DoublesSketch sketch = DoublesSketch.builder().build();
    Double result = func.exec(tupleFactory.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0.0)));
    Assert.assertEquals(result, Double.NaN);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Double> func = new GetQuantileFromDoublesSketch();
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(1.0);
    Double result = func.exec(tupleFactory.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0.5)));
    Assert.assertEquals(result, 1.0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongNumberOfInputs() throws Exception {
    EvalFunc<Double> func = new GetQuantileFromDoublesSketch();
    func.exec(tupleFactory.newTuple(1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    EvalFunc<Double> func = new GetQuantileFromDoublesSketch();
    func.exec(tupleFactory.newTuple(Arrays.asList(1.0, 1.0)));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForFraction() throws Exception {
    EvalFunc<Double> func = new GetQuantileFromDoublesSketch();
    DoublesSketch sketch = DoublesSketch.builder().build();
    func.exec(tupleFactory.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 1)));
  }
}
