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

package org.apache.datasketches.pig.tuple;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToEstimateAndErrorBoundsTest {

  static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void nullInput() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToEstimateAndErrorBounds();
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToEstimateAndErrorBounds();
    Tuple resultTuple = func.exec(tupleFactory.newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void emptyInputSketch() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToEstimateAndErrorBounds();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Tuple resultTuple = func.exec(tupleFactory.newTuple(new DataByteArray(sketch.compact().toByteArray())));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 3);
    Assert.assertEquals(resultTuple.get(0), 0.0);
    Assert.assertEquals(resultTuple.get(1), 0.0);
    Assert.assertEquals(resultTuple.get(2), 0.0);
  }

  @Test
  public void nonEmptyInputSketchExactMode() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToEstimateAndErrorBounds();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {0});
    Tuple resultTuple = func.exec(tupleFactory.newTuple(new DataByteArray(sketch.compact().toByteArray())));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 3);
    Assert.assertEquals(resultTuple.get(0), 1.0);
    Assert.assertEquals(resultTuple.get(1), 1.0);
    Assert.assertEquals(resultTuple.get(2), 1.0);
  }

  @Test
  public void nonEmptyInputSketchEstimationMode() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToEstimateAndErrorBounds();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    int numKeys = 10000; // to saturate the sketch with default number of nominal entries (4K)
    for (int i = 0; i < numKeys; i++ ) {
      sketch.update(i, new double[] {0});
    }
    Tuple resultTuple = func.exec(tupleFactory.newTuple(new DataByteArray(sketch.compact().toByteArray())));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 3);
    double estimate = (double) resultTuple.get(0);
    double lowerBound = (double) resultTuple.get(1);
    double upperBound = (double) resultTuple.get(2);
    Assert.assertEquals(estimate, numKeys, numKeys * 0.04);
    Assert.assertEquals(lowerBound, numKeys, numKeys * 0.04);
    Assert.assertEquals(upperBound, numKeys, numKeys * 0.04);
    Assert.assertTrue(lowerBound < estimate);
    Assert.assertTrue(upperBound > estimate);
  }

}
