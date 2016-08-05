/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.quantiles;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;
import org.testng.Assert;

public class GetKFromStringsSketchTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  @Test
  public void defalutK() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    Integer result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)))));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Integer.valueOf(128));
  }

  @Test
  public void customK() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(1024, COMPARATOR);
    Integer result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)))));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Integer.valueOf(1024));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooFewInputs() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooManyInputs() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple(2));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0)));
  }

}
