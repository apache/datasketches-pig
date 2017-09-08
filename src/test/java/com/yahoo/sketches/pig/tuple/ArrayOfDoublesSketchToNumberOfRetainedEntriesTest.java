/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

public class ArrayOfDoublesSketchToNumberOfRetainedEntriesTest {

  static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void nullInput() throws Exception {
    EvalFunc<Integer> func = new ArrayOfDoublesSketchToNumberOfRetainedEntries();
    Integer result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    EvalFunc<Integer> func = new ArrayOfDoublesSketchToNumberOfRetainedEntries();
    Integer result = func.exec(tupleFactory.newTuple());
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputSketch() throws Exception {
    EvalFunc<Integer> func = new ArrayOfDoublesSketchToNumberOfRetainedEntries();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Integer result = func.exec(tupleFactory.newTuple(new DataByteArray(sketch.compact().toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertEquals((int) result, 0);
  }

  @Test
  public void nonEmptyInputSketch() throws Exception {
    EvalFunc<Integer> func = new ArrayOfDoublesSketchToNumberOfRetainedEntries();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {0});
    Integer result = func.exec(tupleFactory.newTuple(new DataByteArray(sketch.compact().toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertEquals((int) result, 1);
  }

}
