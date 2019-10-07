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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;

import org.apache.datasketches.kll.KllFloatsSketch;

import org.testng.Assert;

@SuppressWarnings("javadoc")
public class GetQuantilesTest {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  @Test
  public void emptySketch() throws Exception {
    final EvalFunc<Tuple> func = new GetQuantiles();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0.5)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    Assert.assertEquals(((float) resultTuple.get(0)), Float.NaN);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooFewInputs() throws Exception {
    final EvalFunc<Tuple> func = new GetQuantiles();
    func.exec(TUPLE_FACTORY.newTuple(1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    final EvalFunc<Tuple> func = new GetQuantiles();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0, 1.0)));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForFractionOrNumberOfIntervals() throws Exception {
    final EvalFunc<Tuple> func = new GetQuantiles();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), "")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeAmongFractions() throws Exception {
    final EvalFunc<Tuple> func = new GetQuantiles();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0.0, 1)));
  }

  @Test
  public void oneFraction() throws Exception {
    final EvalFunc<Tuple> func = new GetQuantiles();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    for (int i = 1; i <= 10; i++) {
      sketch.update(i);
    }
    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0.5)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    Assert.assertEquals(((float) resultTuple.get(0)), 6f);
  }

  @Test
  public void severalFractions() throws Exception {
    final EvalFunc<Tuple> func = new GetQuantiles();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    for (int i = 1; i <= 10; i++) {
      sketch.update(i);
    }
    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0.0, 0.5, 1.0)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 3);
    Assert.assertEquals(((float) resultTuple.get(0)), 1f);
    Assert.assertEquals(((float) resultTuple.get(1)), 6f);
    Assert.assertEquals(((float) resultTuple.get(2)), 10f);
  }

  @Test
  public void numberOfEvenlySpacedIntervals() throws Exception {
    final EvalFunc<Tuple> func = new GetQuantiles();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    for (int i = 1; i <= 10; i++) {
      sketch.update(i);
    }
    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 3)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 3);
    Assert.assertEquals(((float) resultTuple.get(0)), 1f);
    Assert.assertEquals(((float) resultTuple.get(1)), 6f);
    Assert.assertEquals(((float) resultTuple.get(2)), 10f);
  }

}
