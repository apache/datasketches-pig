/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

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
