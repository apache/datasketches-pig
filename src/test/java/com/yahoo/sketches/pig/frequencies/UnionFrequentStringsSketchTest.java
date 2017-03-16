/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.frequencies;

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
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;
import com.yahoo.sketches.pig.tuple.PigUtil;

public class UnionFrequentStringsSketchTest {
  @Test
  public void execNullInput() throws Exception {
    EvalFunc<Tuple> func = new UnionFrequentStringsSketch("8");
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new UnionFrequentStringsSketch("8");
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void exec() throws Exception {
    EvalFunc<Tuple> func = new UnionFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      ItemsSketch<String> sketch = new ItemsSketch<String>(8);
      sketch.update("a");
      sketch.update("b");
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.toByteArray(new ArrayOfStringsSerDe()))));
    }
    {
      ItemsSketch<String> sketch = new ItemsSketch<String>(8);
      sketch.update("a");
      sketch.update("b");
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.toByteArray(new ArrayOfStringsSerDe()))));
    }
    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertEquals(sketch.getNumActiveItems(), 2);
    Assert.assertEquals(sketch.getEstimate("a"), 2);
    Assert.assertEquals(sketch.getEstimate("b"), 2);
  }

  @Test
  public void accumulatorNullInput() throws Exception {
    Accumulator<Tuple> func = new UnionFrequentStringsSketch("8");
    func.accumulate(null);
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
  }

  @Test
  public void accumulatorEmptyInputTuple() throws Exception {
    Accumulator<Tuple> func = new UnionFrequentStringsSketch("8");
    func.accumulate(TupleFactory.getInstance().newTuple());
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
  }

  @Test
  public void accumulatorNotABag() throws Exception {
    Accumulator<Tuple> func = new UnionFrequentStringsSketch("8");
    func.accumulate(PigUtil.objectsToTuple((Object) null));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
  }

  @Test
  public void accumulatorEmptyBag() throws Exception {
    Accumulator<Tuple> func = new UnionFrequentStringsSketch("8");
    func.accumulate(PigUtil.objectsToTuple(BagFactory.getInstance().newDefaultBag()));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
  }

  @Test
  public void accumulatorEmptyInnerTuple() throws Exception {
    Accumulator<Tuple> func = new UnionFrequentStringsSketch("8");
    func.accumulate(PigUtil.objectsToTuple(PigUtil.tuplesToBag(TupleFactory.getInstance().newTuple())));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
  }

  @Test
  public void accumulatorNullSketch() throws Exception {
    Accumulator<Tuple> func = new UnionFrequentStringsSketch("8");
    func.accumulate(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple((Object) null))));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
  }

  @Test
  public void accumulatorEmptySketch() throws Exception {
    Accumulator<Tuple> func = new UnionFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      ItemsSketch<String> sketch = new ItemsSketch<String>(8);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.toByteArray(new ArrayOfStringsSerDe()))));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<Tuple> func = new UnionFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      ItemsSketch<String> sketch = new ItemsSketch<String>(8);
      sketch.update("a");
      sketch.update("b");
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.toByteArray(new ArrayOfStringsSerDe()))));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));

    bag = BagFactory.getInstance().newDefaultBag();
    {
      ItemsSketch<String> sketch = new ItemsSketch<String>(8);
      sketch.update("a");
      sketch.update("b");
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.toByteArray(new ArrayOfStringsSerDe()))));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));

    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 2);
    Assert.assertEquals(sketch.getEstimate("a"), 2);
    Assert.assertEquals(sketch.getEstimate("b"), 2);
  }

  @Test
  public void algebraicInitial() throws Exception {
    EvalFunc<Tuple> func = new UnionFrequentStringsSketch.Initial(null);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(null);
    bag.add(null);
    bag.add(null);

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataBag resultBag = (DataBag) resultTuple.get(0);
    Assert.assertEquals(resultBag.size(), 3);
  }

  @Test
  public void algebraicIntemediateFinalExact() throws Exception {
    EvalFunc<Tuple> func = new UnionFrequentStringsSketch.IntermediateFinal("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    // this is to simulate the output from Initial
    {
      ItemsSketch<String> sketch = new ItemsSketch<String>(8);
      sketch.update("a");
      sketch.update("b");
      DataBag innerBag = PigUtil.tuplesToBag(PigUtil.objectsToTuple(new DataByteArray(sketch.toByteArray(new ArrayOfStringsSerDe()))));
      bag.add(PigUtil.objectsToTuple(innerBag));
    }

    // this is to simulate the output from a prior call of IntermediateFinal
    {
      ItemsSketch<String> sketch = new ItemsSketch<String>(8);
      sketch.update("a", 2L);
      sketch.update("b", 3L);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.toByteArray(new ArrayOfStringsSerDe()))));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 2);
    Assert.assertEquals(sketch.getEstimate("a"), 3);
    Assert.assertEquals(sketch.getEstimate("b"), 4);
  }

  @Test
  public void algebraicIntemediateFinalEstimation() throws Exception {
    EvalFunc<Tuple> func = new UnionFrequentStringsSketch.IntermediateFinal("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    // this is to simulate the output from Initial
    {
      ItemsSketch<String> sketch = new ItemsSketch<String>(8);
      sketch.update("a", 10);
      sketch.update("b");
      sketch.update("c");
      sketch.update("d");
      sketch.update("e");
      sketch.update("f");
      sketch.update("g");
      sketch.update("g");
      DataBag innerBag = PigUtil.tuplesToBag(PigUtil.objectsToTuple(new DataByteArray(sketch.toByteArray(new ArrayOfStringsSerDe()))));
      bag.add(PigUtil.objectsToTuple(innerBag));
    }

    // this is to simulate the output from a prior call of IntermediateFinal
    {
      ItemsSketch<String> sketch = new ItemsSketch<String>(8);
      sketch.update("a");
      sketch.update("a");
      sketch.update("g", 5);
      sketch.update("h");
      sketch.update("i");
      sketch.update("j");
      sketch.update("k");
      sketch.update("l");
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.toByteArray(new ArrayOfStringsSerDe()))));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getStreamLength(), 29);

    ItemsSketch.Row<String>[] items = sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
    Assert.assertEquals(items.length, 2);
    // only 2 items ("a" and "g") should have counts more than 1
    int count = 0;
    for (ItemsSketch.Row<String> item: items) {
      if (item.getLowerBound() > 1) count++;
    }
    Assert.assertEquals(count, 2);
  }
}
