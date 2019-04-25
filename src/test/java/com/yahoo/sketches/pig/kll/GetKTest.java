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

public class GetKTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  @Test
  public void defalutK() throws Exception {
    final EvalFunc<Integer> func = new GetK();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    final Integer result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()))));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Integer.valueOf(KllFloatsSketch.DEFAULT_K));
  }

  @Test
  public void customK() throws Exception {
    final EvalFunc<Integer> func = new GetK();
    final KllFloatsSketch sketch = new KllFloatsSketch(400);
    final Integer result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()))));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Integer.valueOf(400));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void noInputs() throws Exception {
    final EvalFunc<Integer> func = new GetK();
    func.exec(TUPLE_FACTORY.newTuple());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooManyInputs() throws Exception {
    final EvalFunc<Integer> func = new GetK();
    func.exec(TUPLE_FACTORY.newTuple(2));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    final EvalFunc<Integer> func = new GetK();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0)));
  }

}
