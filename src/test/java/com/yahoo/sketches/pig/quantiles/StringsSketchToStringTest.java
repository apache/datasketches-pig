/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.quantiles;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

import org.testng.Assert;

public class StringsSketchToStringTest {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  @Test
  public void nullInputTuple() throws Exception {
    final EvalFunc<String> func = new StringsSketchToString();
    final String result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void emptyInputTuple() throws Exception {
    final EvalFunc<String> func = new StringsSketchToString();
    final String result = func.exec(TUPLE_FACTORY.newTuple());
    Assert.assertNull(result);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooManyInputs() throws Exception {
    final EvalFunc<String> func = new StringsSketchToString();
    func.exec(TUPLE_FACTORY.newTuple(2));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    final EvalFunc<String> func = new StringsSketchToString();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0)));
  }

  @Test
  public void normalCase() throws Exception {
    final EvalFunc<String> func = new DoublesSketchToString();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    final String result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray(SER_DE))));
    Assert.assertNotNull(result);
  }

}
