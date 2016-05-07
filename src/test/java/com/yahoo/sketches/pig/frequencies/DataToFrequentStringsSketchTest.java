/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.frequencies;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.frequencies.ArrayOfStringsSerDe;
import com.yahoo.sketches.frequencies.FrequentItemsSketch;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.pig.tuple.PigUtil;

public class DataToFrequentStringsSketchTest {
  @Test
  public void execNullInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyBag() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    Tuple inputTuple = PigUtil.objectsToTuple(BagFactory.getInstance().newDefaultBag());
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    FrequentItemsSketch<String> sketch = FrequentItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void execWrongSizeOfInnerTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple());
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void execWrongItemType() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple(new Object(), 1L)); // Object in place of String is not supported
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void execWrongCountType() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a", 1)); // integer count is not supported
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test
  public void exec() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a"));
    bag.add(PigUtil.objectsToTuple("b", 5L));
    bag.add(PigUtil.objectsToTuple("a", 2L));
    bag.add(PigUtil.objectsToTuple("b"));

    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    FrequentItemsSketch<String> sketch = FrequentItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertEquals(sketch.getNumActiveItems(), 2);
    Assert.assertEquals(sketch.getEstimate("a"), 3);
    Assert.assertEquals(sketch.getEstimate("b"), 6);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<Tuple> func = new DataToFrequentStringsSketch("8");

    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a"));
    inputTuple.set(0, bag);

    func.accumulate(inputTuple);

    inputTuple = TupleFactory.getInstance().newTuple(1);
    bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("b"));
    bag.add(PigUtil.objectsToTuple("a", 2L));
    bag.add(PigUtil.objectsToTuple("b", 5L));
    inputTuple.set(0, bag);

    func.accumulate(inputTuple);

    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    FrequentItemsSketch<String> sketch = FrequentItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertEquals(sketch.getNumActiveItems(), 2);
    Assert.assertEquals(sketch.getEstimate("a"), 3);
    Assert.assertEquals(sketch.getEstimate("b"), 6);

    // after cleanup, the value should always be 0
    func.cleanup();
    resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    FrequentItemsSketch<String> sketch2 = FrequentItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 0);
  }

  @Test
  public void algebraicInitial() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch.Initial(null);
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
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch.IntermediateFinal("8");
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    // this is to simulate the output from Initial
    bag.add(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple("a"))));

    // this is to simulate the output from a prior call of IntermediateFinal
    FrequentItemsSketch<String> s = new FrequentItemsSketch<String>(8);
    s.update("b", 1L);
    s.update("a", 2L);
    s.update("b", 3L);
    bag.add(PigUtil.objectsToTuple(new DataByteArray(s.serializeToByteArray(new ArrayOfStringsSerDe()))));

    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    FrequentItemsSketch<String> sketch = FrequentItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertEquals(sketch.getNumActiveItems(), 2);
    Assert.assertEquals(sketch.getEstimate("a"), 3);
    Assert.assertEquals(sketch.getEstimate("b"), 4);
  }
}
