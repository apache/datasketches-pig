/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

public class DataToDoubleSummarySketchTest {

  @Test
  public void execNullInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch();
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch();
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyBag() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch();
    Tuple inputTuple = PigUtil.objectsToTuple(BagFactory.getInstance().newDefaultBag());
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void execWrongSizeOfInnerTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch();
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple(1));
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void execWrongKeyType() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch();
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple(new Object(), 1.0)); // Object in place of key is not supported
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test
  public void execAllInputTypes() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch();
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a", 1.0));
    bag.add(PigUtil.objectsToTuple("b", 1.0));
    bag.add(PigUtil.objectsToTuple("a", 2.0));
    bag.add(PigUtil.objectsToTuple("b", 2.0));

    bag.add(PigUtil.objectsToTuple(1, 3.0));
    bag.add(PigUtil.objectsToTuple(2L, 3.0));
    bag.add(PigUtil.objectsToTuple(1f, 3.0));
    bag.add(PigUtil.objectsToTuple(2.0, 3.0));
    bag.add(PigUtil.objectsToTuple((byte)3, 3.0));
    bag.add(PigUtil.objectsToTuple(new DataByteArray("c".getBytes()), 3.0));

    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 8.0, 0.0);

    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 3.0);
    }
  }

  @Test
  public void execMinMode() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch("32", "Min");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a", 1.0));
    bag.add(PigUtil.objectsToTuple("b", 2.0));
    bag.add(PigUtil.objectsToTuple("a", 2.0));
    bag.add(PigUtil.objectsToTuple("b", 1.0));

    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);

    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 1.0);
    }
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<Tuple> func = new DataToDoubleSummarySketch("32");

    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a", 1.0));
    inputTuple.set(0, bag);

    func.accumulate(inputTuple);

    inputTuple = TupleFactory.getInstance().newTuple(1);
    bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("b", 1.0));
    bag.add(PigUtil.objectsToTuple("a", 2.0));
    bag.add(PigUtil.objectsToTuple("b", 2.0));
    inputTuple.set(0, bag);

    func.accumulate(inputTuple);

    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);

    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 3.0);
    }

    // after cleanup, the value should always be 0
    func.cleanup();
    resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch2 = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch2.getEstimate(), 0.0, 0.0);
  }

  @Test
  public void algebraicInitialOneParam() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch.Initial(null);
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple(null, null));
    bag.add(PigUtil.objectsToTuple(null, null));
    bag.add(PigUtil.objectsToTuple(null, null));
    inputTuple.set(0, bag);

    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataBag resultBag = (DataBag) resultTuple.get(0);
    Assert.assertEquals(resultBag.size(), 3);
  }

  @Test
  public void algebraicInitialTwoParams() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch.Initial(null, null);
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple(null, null));
    bag.add(PigUtil.objectsToTuple(null, null));
    bag.add(PigUtil.objectsToTuple(null, null));
    inputTuple.set(0, bag);

    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataBag resultBag = (DataBag) resultTuple.get(0);
    Assert.assertEquals(resultBag.size(), 3);
  }

  @Test
  public void algebraicIntermediateFinal() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch.IntermediateFinal("32");
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    // this is to simulate the output from Initial
    bag.add(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple("a", 1.0))));

    // this is to simulate the output from a prior call of IntermediateFinal
    UpdatableSketch<Double, DoubleSummary> ts = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    ts.update("b", 1.0);
    ts.update("a", 2.0);
    ts.update("b", 2.0);
    Sketch<DoubleSummary> cs = ts.compact();
    bag.add(PigUtil.objectsToTuple(new DataByteArray(cs.toByteArray())));

    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);

    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 3.0);
    }
  }

  @Test
  public void algebraicIntermediateFinalMaxMode() throws Exception {
    EvalFunc<Tuple> func = new DataToDoubleSummarySketch.IntermediateFinal("32", "Max");
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    // this is to simulate the output from Initial
    bag.add(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple("a", 1.0))));
    bag.add(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple("b", 1.0))));
    bag.add(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple("a", 2.0))));
    bag.add(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple("b", 2.0))));

    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);

    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 2.0);
    }
  }
}
