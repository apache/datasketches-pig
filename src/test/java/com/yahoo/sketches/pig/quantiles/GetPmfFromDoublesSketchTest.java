/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.quantiles;

import com.yahoo.sketches.quantiles.DoublesSketch;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;
import org.testng.Assert;

public class GetPmfFromDoublesSketchTest {
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void emptySketch() throws Exception {
    EvalFunc<Tuple> func = new GetPmfFromDoublesSketch();
    DoublesSketch sketch = DoublesSketch.builder().build();
    Tuple resultTuple = func.exec(tupleFactory.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0.5)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals(((double) resultTuple.get(0)), Double.NaN);
    Assert.assertEquals(((double) resultTuple.get(1)), Double.NaN);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Tuple> func = new GetPmfFromDoublesSketch();
    DoublesSketch sketch = DoublesSketch.builder().build();
    for (int i = 1; i <= 10; i++) sketch.update(i);
    Tuple resultTuple = func.exec(tupleFactory.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 2.0, 7.0)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 3);
    Assert.assertEquals(((double) resultTuple.get(0)), 0.1);
    Assert.assertEquals(((double) resultTuple.get(1)), 0.5);
    Assert.assertEquals(((double) resultTuple.get(2)), 0.4);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongNumberOfInputs() throws Exception {
    EvalFunc<Tuple> func = new GetPmfFromDoublesSketch();
    func.exec(tupleFactory.newTuple(1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    EvalFunc<Tuple> func = new GetPmfFromDoublesSketch();
    func.exec(tupleFactory.newTuple(Arrays.asList(1.0, 1.0)));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForFraction() throws Exception {
    EvalFunc<Tuple> func = new GetPmfFromDoublesSketch();
    DoublesSketch sketch = DoublesSketch.builder().build();
    func.exec(tupleFactory.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 1)));
  }
}
