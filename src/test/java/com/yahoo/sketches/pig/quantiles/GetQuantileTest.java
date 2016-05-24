/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.quantiles;

import com.yahoo.sketches.quantiles.QuantilesSketch;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;
import org.testng.Assert;

public class GetQuantileTest {
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void emptySketch() throws Exception {
    EvalFunc<Double> func = new GetQuantile();
    QuantilesSketch sketch = QuantilesSketch.builder().build();
    Double result = func.exec(tupleFactory.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0.0)));
    Assert.assertEquals(result, Double.POSITIVE_INFINITY);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Double> func = new GetQuantile();
    QuantilesSketch sketch = QuantilesSketch.builder().build();
    sketch.update(1.0);
    Double result = func.exec(tupleFactory.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0.5)));
    Assert.assertEquals(result, 1.0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongNumberOfInputs() throws Exception {
    EvalFunc<Double> func = new GetQuantile();
    func.exec(tupleFactory.newTuple(1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    EvalFunc<Double> func = new GetQuantile();
    func.exec(tupleFactory.newTuple(Arrays.asList(1.0, 1.0)));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForFraction() throws Exception {
    EvalFunc<Double> func = new GetQuantile();
    QuantilesSketch sketch = QuantilesSketch.builder().build();
    func.exec(tupleFactory.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 1)));
  }
}
