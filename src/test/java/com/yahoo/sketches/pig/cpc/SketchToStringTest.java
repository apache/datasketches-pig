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

public class SketchToStringTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  @Test
  public void nullInputTuple() throws Exception {
    final EvalFunc<String> func = new SketchToString();
    final String result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    final EvalFunc<String> func = new SketchToString();
    final String result = func.exec(TUPLE_FACTORY.newTuple());
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() throws Exception {
    final EvalFunc<String> func = new SketchToString();
    final CpcSketch sketch = new CpcSketch();
    final String result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

  @Test
  public void normalCaseWithDetail() throws Exception {
    final EvalFunc<String> func = new SketchToString("true");
    final CpcSketch sketch = new CpcSketch();
    final String result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

  @Test
  public void normalCaseWithDetailAndSeed() throws Exception {
    final EvalFunc<String> func = new SketchToString("true", "123");
    final CpcSketch sketch = new CpcSketch(12, 123);
    final String result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

}
