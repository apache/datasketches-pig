/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.kll;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;

import com.yahoo.sketches.kll.KllFloatsSketch;

import org.testng.Assert;

public class GetPmfTest {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  @Test
  public void emptySketch() throws Exception {
    final EvalFunc<Tuple> func = new GetPmf();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 0f)));
    Assert.assertNull(resultTuple);
  }

  @Test
  public void normalCase() throws Exception {
    final EvalFunc<Tuple> func = new GetPmf();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    for (int i = 1; i <= 10; i++) sketch.update(i);
    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 2f, 7f)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 3);
    Assert.assertEquals(((double) resultTuple.get(0)), 0.1);
    Assert.assertEquals(((double) resultTuple.get(1)), 0.5);
    Assert.assertEquals(((double) resultTuple.get(2)), 0.4);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongNumberOfInputs() throws Exception {
    final EvalFunc<Tuple> func = new GetPmf();
    func.exec(TUPLE_FACTORY.newTuple(1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    final EvalFunc<Tuple> func = new GetPmf();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0, 1.0)));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeOfSplitPoint() throws Exception {
    final EvalFunc<Tuple> func = new GetPmf();
    final KllFloatsSketch sketch = new KllFloatsSketch();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray()), 1)));
  }

}
