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

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToQuantilesSketchTest {

  static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void nullInput() throws Exception {
    EvalFunc<DataByteArray> func = new ArrayOfDoublesSketchToQuantilesSketch();
    DataByteArray result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    EvalFunc<DataByteArray> func = new ArrayOfDoublesSketchToQuantilesSketch();
    DataByteArray result = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputSketch() throws Exception {
    EvalFunc<DataByteArray> func = new ArrayOfDoublesSketchToQuantilesSketch();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    DataByteArray result = func.exec(tupleFactory.newTuple(new DataByteArray(sketch.compact().toByteArray())));
    Assert.assertNotNull(result);
    DoublesSketch quantilesSketch = DoublesSketch.wrap(Memory.wrap(result.get()));
    Assert.assertTrue(quantilesSketch.isEmpty());
  }

  @Test
  public void nonEmptyInputSketchWithTwoColumnsExplicitK() throws Exception {
    int k = 256;
    EvalFunc<DataByteArray> func = new ArrayOfDoublesSketchToQuantilesSketch(Integer.toString(k));
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch.update(1, new double[] {1.0, 2.0});
    sketch.update(2, new double[] {10.0, 20.0});
    DataByteArray result = func.exec(tupleFactory.newTuple(Arrays.asList(
        new DataByteArray(sketch.compact().toByteArray()),
        2
    )));
    Assert.assertNotNull(result);
    DoublesSketch quantilesSketch = DoublesSketch.wrap(Memory.wrap(result.get()));
    Assert.assertFalse(quantilesSketch.isEmpty());
    Assert.assertEquals(quantilesSketch.getK(), k);
    Assert.assertEquals(quantilesSketch.getMinValue(), 2.0);
    Assert.assertEquals(quantilesSketch.getMaxValue(), 20.0);
  }

}
