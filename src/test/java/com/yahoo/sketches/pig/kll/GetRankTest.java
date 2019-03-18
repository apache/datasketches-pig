/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.kll;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;

import com.yahoo.sketches.kll.KllFloatsSketch;

import org.testng.Assert;

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
