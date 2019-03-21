/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.cpc;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

import com.yahoo.sketches.cpc.CpcSketch;

import junit.framework.Assert;

public class GetEstimateTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  @Test
  public void nullInputTuple() throws Exception {
    final EvalFunc<Double> func = new GetEstimate();
    final Double result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    final EvalFunc<Double> func = new GetEstimate();
    final Double result = func.exec(TUPLE_FACTORY.newTuple());
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() throws Exception {
    final EvalFunc<Double> func = new GetEstimate();
    final CpcSketch sketch = new CpcSketch();
    sketch.update(1);
    sketch.update(2);
    Double result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, 2.0, 0.01);
  }

  @Test
  public void normalCaseCustomSeed() throws Exception {
    final EvalFunc<Double> func = new GetEstimate("123");
    final CpcSketch sketch = new CpcSketch(12, 123);
    sketch.update(1);
    sketch.update(2);
    Double result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, 2.0, 0.01);
  }

}
