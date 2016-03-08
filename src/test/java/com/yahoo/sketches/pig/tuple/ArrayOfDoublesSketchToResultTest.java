/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import java.util.Iterator;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;

import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

public class ArrayOfDoublesSketchToResultTest {
  @Test
  public void emptySketch() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToResult();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals(resultTuple.get(0), 0.0); // estimate
    Assert.assertEquals(((DataBag)resultTuple.get(1)).size(), 0);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToResult();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update("a", new double[] {1.0});
    sketch.update("b", new double[] {1.0});
    sketch.update("a", new double[] {2.0});
    sketch.update("b", new double[] {2.0});
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals(resultTuple.get(0), 2.0); // estimate
    DataBag bag = (DataBag) resultTuple.get(1);
    Assert.assertEquals(bag.size(), 2);
    Iterator<Tuple> it = bag.iterator();
    while (it.hasNext()) {
      Tuple tuple = it.next();
      Assert.assertEquals(tuple.size(), 1);
      Assert.assertEquals(tuple.get(0), 3.0);
    }
  }

  @Test
  public void twoValuesPerKey() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToResult();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch.update("a", new double[] {1.0, 2.0});
    sketch.update("b", new double[] {1.0, 2.0});
    sketch.update("a", new double[] {2.0, 4.0});
    sketch.update("b", new double[] {2.0, 4.0});
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals(resultTuple.get(0), 2.0); // estimate
    DataBag bag = (DataBag) resultTuple.get(1);
    Assert.assertEquals(bag.size(), 2);
    Iterator<Tuple> it = bag.iterator();
    while (it.hasNext()) {
      Tuple tuple = it.next();
      Assert.assertEquals(tuple.size(), 2);
      Assert.assertEquals(tuple.get(0), 3.0);
      Assert.assertEquals(tuple.get(1), 6.0);
    }
  }
}
