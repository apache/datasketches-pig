/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

public class ArrayOfDoublesSketchToMeansTest {

  @Test
  public void nullInput() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToMeans();
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToMeans();
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void emptyInputSketch() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToMeans();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void oneEntryInputSketch() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToMeans();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {1});
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    Assert.assertEquals(resultTuple.get(0), 1.0);
  }

  @Test
  public void manyEntriesTwoValuesInputSketch() throws Exception {
    EvalFunc<Tuple> func = new ArrayOfDoublesSketchToMeans();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    Random rand = new Random(0);
    int numKeys = 10000; // to saturate the sketch with default number of nominal entries (4K)
    for (int i = 0; i < numKeys; i++ ) {
      // two random values normally distributed with means of 0 and 1
      sketch.update(i, new double[] {rand.nextGaussian(), rand.nextGaussian() + 1.0});
    }
    Assert.assertTrue(sketch.getRetainedEntries() >= 4096);
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals((double) resultTuple.get(0), 0.0, 0.04);
    Assert.assertEquals((double) resultTuple.get(1), 1.0, 0.04);
  }

}
