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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;
import org.testng.Assert;

public class GetQuantilesFromStringsSketchTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  @Test
  public void emptySketch() throws Exception {
    EvalFunc<Tuple> func = new GetQuantilesFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)), 0.5)));
    Assert.assertNull(resultTuple);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooFewInputs() throws Exception {
    EvalFunc<Tuple> func = new GetQuantilesFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple(1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    EvalFunc<Tuple> func = new GetQuantilesFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0, 1.0)));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForFractionOrNumberOfIntervals() throws Exception {
    EvalFunc<Tuple> func = new GetQuantilesFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    sketch.update("a");
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)), "")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeAmongFractions() throws Exception {
    EvalFunc<Tuple> func = new GetQuantilesFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    sketch.update("a");
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)), 0.0, 1)));
  }

  @Test
  public void oneFraction() throws Exception {
    EvalFunc<Tuple> func = new GetQuantilesFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    for (int i = 1; i <= 10; i++) sketch.update(String.format("%02d", i));
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)), 0.5)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    Assert.assertEquals(((String) resultTuple.get(0)), "06");
  }

  @Test
  public void severalFractions() throws Exception {
    EvalFunc<Tuple> func = new GetQuantilesFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    for (int i = 1; i <= 10; i++) sketch.update(String.format("%02d", i));
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)), 0.0, 0.5, 1.0)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 3);
    Assert.assertEquals(((String) resultTuple.get(0)), "01");
    Assert.assertEquals(((String) resultTuple.get(1)), "06");
    Assert.assertEquals(((String) resultTuple.get(2)), "10");
  }

  @Test
  public void numberOfEvenlySpacedIntervals() throws Exception {
    EvalFunc<Tuple> func = new GetQuantilesFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    for (int i = 1; i <= 10; i++) sketch.update(String.format("%02d", i));
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)), 3)));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 3);
    Assert.assertEquals(((String) resultTuple.get(0)), "01");
    Assert.assertEquals(((String) resultTuple.get(1)), "06");
    Assert.assertEquals(((String) resultTuple.get(2)), "10");
  }

}
