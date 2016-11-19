/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import org.testng.annotations.Test;
import org.testng.Assert;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

public class DoubleSummarySketchToPercentileTest {
  @Test
  public void emptySketch() throws Exception {
    EvalFunc<Double> func = new DoubleSummarySketchToPercentile();
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    Tuple inputTuple = TupleFactory.getInstance().newTuple(Arrays.asList(new DataByteArray(sketch.compact().toByteArray()), 0.0));
    double result = func.exec(inputTuple);
    Assert.assertEquals(result, Double.POSITIVE_INFINITY);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Double> func = new DoubleSummarySketchToPercentile();
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    int iterations = 100000;
    for (int i = 0; i < iterations; i++) sketch.update(i, (double) i);
    for (int i = 0; i < iterations; i++) sketch.update(i, (double) i);
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()), 50.0);
    double result = func.exec(inputTuple);
    Assert.assertEquals(result, iterations, iterations * 0.02);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongNumberOfInputs() throws Exception {
    EvalFunc<Double> func = new DoubleSummarySketchToPercentile();
    func.exec(PigUtil.objectsToTuple(1.0));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void percentileOutOfRange() throws Exception {
    EvalFunc<Double> func = new DoubleSummarySketchToPercentile();
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    func.exec(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()), 200.0));
  }
}
