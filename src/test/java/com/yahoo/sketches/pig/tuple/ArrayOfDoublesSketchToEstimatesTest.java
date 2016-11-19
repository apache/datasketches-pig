/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

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
    for (int i = 0; i < iterations; i++) sketch.update(i, new double[] {1});
    for (int i = 0; i < iterations; i++) sketch.update(i, new double[] {1});
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals((double) resultTuple.get(0), iterations, iterations * 0.03);
    Assert.assertEquals((double) resultTuple.get(1), 2 * iterations, 2 * iterations * 0.03);
  }
}
