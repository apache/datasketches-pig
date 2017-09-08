/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

public class ArrayOfDoublesSketchToQuantilesSketchTest {

  static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void nullInput() throws Exception {
    EvalFunc<DataByteArray> func = new ArrayOfDoublesSketchToQuantilesSketch();
    DataByteArray result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    EvalFunc<DataByteArray> func = new ArrayOfDoublesSketchToQuantilesSketch();
    DataByteArray result = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputSketch() throws Exception {
    EvalFunc<DataByteArray> func = new ArrayOfDoublesSketchToQuantilesSketch();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    DataByteArray result = func.exec(tupleFactory.newTuple(new DataByteArray(sketch.compact().toByteArray())));
    Assert.assertNotNull(result);
    DoublesSketch quantilesSketch = DoublesSketch.wrap(Memory.wrap(result.get()));
    Assert.assertTrue(quantilesSketch.isEmpty());
  }

  @Test
  public void nonEmptyInputSketchWithTwoColumnsExplicitK() throws Exception {
    int k = 256;
    EvalFunc<DataByteArray> func = new ArrayOfDoublesSketchToQuantilesSketch(k);
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch.update(1, new double[] {1.0, 2.0});
    sketch.update(2, new double[] {10.0, 20.0});
    DataByteArray result = func.exec(tupleFactory.newTuple(Arrays.asList(
        new DataByteArray(sketch.compact().toByteArray()),
        2
    )));
    Assert.assertNotNull(result);
    DoublesSketch quantilesSketch = DoublesSketch.wrap(Memory.wrap(result.get()));
    Assert.assertFalse(quantilesSketch.isEmpty());
    Assert.assertEquals(quantilesSketch.getK(), k);
    Assert.assertEquals(quantilesSketch.getMinValue(), 2.0);
    Assert.assertEquals(quantilesSketch.getMaxValue(), 20.0);
  }

}
