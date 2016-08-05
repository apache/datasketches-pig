/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.quantiles;

import com.yahoo.sketches.quantiles.DoublesSketch;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;
import org.testng.Assert;

public class GetKFromDoublesSketchTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  @Test
  public void defalutK() throws Exception {
    EvalFunc<Integer> func = new GetKFromDoublesSketch();
    DoublesSketch sketch = DoublesSketch.builder().build();
    Integer result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()))));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Integer.valueOf(128));
  }

  @Test
  public void customK() throws Exception {
    EvalFunc<Integer> func = new GetKFromDoublesSketch();
    DoublesSketch sketch = DoublesSketch.builder().setK(1024).build();
    Integer result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()))));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Integer.valueOf(1024));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooFewInputs() throws Exception {
    EvalFunc<Integer> func = new GetKFromDoublesSketch();
    func.exec(TUPLE_FACTORY.newTuple());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooManyInputs() throws Exception {
    EvalFunc<Integer> func = new GetKFromDoublesSketch();
    func.exec(TUPLE_FACTORY.newTuple(2));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    EvalFunc<Integer> func = new GetKFromDoublesSketch();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0)));
  }

}
