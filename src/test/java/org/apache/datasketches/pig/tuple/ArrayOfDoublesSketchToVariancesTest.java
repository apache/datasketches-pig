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

import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToVariancesTest {

  @Test
  public void nullInput() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToVariances();
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToVariances();
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void emptyInputSketch() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToVariances();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void oneEntryInputSketch() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToVariances();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {1});
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    Assert.assertEquals(resultTuple.get(0), 0.0);
  }

  @Test
  public void manyEntriesTwoValuesInputSketch() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToVariances();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    Random rand = new Random(0);
    int numKeys = 10000; // to saturate the sketch with default number of nominal entries (4K)
    for (int i = 0; i < numKeys; i++ ) {
      // two random values normally distributed with standard deviations of 1 and 10
      sketch.update(i, new double[] {rand.nextGaussian(), rand.nextGaussian() * 10.0});
    }
    Assert.assertTrue(sketch.getRetainedEntries() >= 4096);
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals((double) resultTuple.get(0), 1.0, 0.04);
    Assert.assertEquals((double) resultTuple.get(1), 100.0, 100.0 * 0.04); // squared standard deviation within 4%
  }

}
