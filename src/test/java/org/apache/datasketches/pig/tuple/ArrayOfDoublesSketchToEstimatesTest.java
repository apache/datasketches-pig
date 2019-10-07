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

import org.testng.annotations.Test;
import org.testng.Assert;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToEstimatesTest {

  @Test
  public void nullInput() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToEstimates();
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void emptyInput() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToEstimates();
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void emptySketch() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToEstimates();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals(resultTuple.get(0), 0.0);
    Assert.assertEquals(resultTuple.get(1), 0.0);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToEstimates();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    int iterations = 100000;
    for (int i = 0; i < iterations; i++) {
      sketch.update(i, new double[] {1});
    }
    for (int i = 0; i < iterations; i++) {
      sketch.update(i, new double[] {1});
    }
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals((double) resultTuple.get(0), iterations, iterations * 0.03);
    Assert.assertEquals((double) resultTuple.get(1), 2 * iterations, 2 * iterations * 0.03);
  }
}
