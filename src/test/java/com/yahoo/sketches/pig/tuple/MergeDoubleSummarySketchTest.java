/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

import java.util.Random;

public class MergeDoubleSummarySketchTest {
  @Test
  public void execNullInput() throws Exception {
    EvalFunc<Tuple> func = new MergeDoubleSummarySketch("32");
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new MergeDoubleSummarySketch("32");
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void exec() throws Exception {
    EvalFunc<Tuple> func = new MergeDoubleSummarySketch("4096");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 2.0, 0.0);
    }
  }

  @Test
  public void execMaxMode() throws Exception {
    EvalFunc<Tuple> func = new MergeDoubleSummarySketch("4096", "Max");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
      sketch.update(1, 3.0);
      sketch.update(2, 3.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 3.0, 0.0);
    }
  }

  @Test
  public void accumulatorNullInput() throws Exception {
    Accumulator<Tuple> func = new MergeDoubleSummarySketch("32");
    func.accumulate(null);
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorEmptyInputTuple() throws Exception {
    Accumulator<Tuple> func = new MergeDoubleSummarySketch("32");
    func.accumulate(TupleFactory.getInstance().newTuple());
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<Tuple> func = new MergeDoubleSummarySketch("4096");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));

    bag = BagFactory.getInstance().newDefaultBag();
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));

    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 2.0, 0.0);
    }
  }

  @Test
  public void algebraicInitial() throws Exception {
    EvalFunc<Tuple> func = new MergeDoubleSummarySketch.Initial(null);
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
  public void algebraicIntemediateFinalExactMinMode() throws Exception {
    EvalFunc<Tuple> func = new MergeDoubleSummarySketch.IntermediateFinal("4096", "Min");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    // this is to simulate the output from Initial
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      DataBag innerBag = PigUtil.tuplesToBag(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      bag.add(PigUtil.objectsToTuple(innerBag));
    }

    // this is to simulate the output from a prior call of IntermediateFinal
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
      sketch.update(1, 3.0);
      sketch.update(2, 3.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 1.0, 0.0);
    }
  }

  @Test
  public void algebraicIntemediateFinalEstimation() throws Exception {
    EvalFunc<Tuple> func = new MergeDoubleSummarySketch.IntermediateFinal("16384");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    long value = 1;
    // this is to simulate the output from Initial
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(16384).build();
      for (int i = 0; i < 20000; i++) sketch.update(value++, 1.0);
      DataBag innerBag = PigUtil.tuplesToBag(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      bag.add(PigUtil.objectsToTuple(innerBag));
    }

    // this is to simulate the output from a prior call of IntermediateFinal
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(16384).build();
      for (int i = 0; i < 20000; i++) sketch.update(value++, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 40000.0, 40000.0 * 0.01);
    Assert.assertTrue(sketch.getRetainedEntries() > 16384);
    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 1.0, 0.0);
    }
  }

  @Test
  public void algebraicIntemediateFinalSingleCall() throws Exception {
    EvalFunc<Tuple> func = new MergeDoubleSummarySketch.IntermediateFinal("1024");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    long value = 1;
    // this is to simulate the output from a prior call of IntermediateFinal
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(1024).build();
      for (int i = 0; i < 10000; i++) sketch.update(value++, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 10000.0, 10000.0 * 0.02);
    Assert.assertTrue(sketch.getRetainedEntries() > 1024);
    for (DoubleSummary summary: sketch.getSummaries()) {
      Assert.assertEquals(summary.getValue(), 1.0, 0.0);
    }
  }

  @Test
  public void algebraicIntemediateFinalRandomized() throws Exception {
    EvalFunc<Tuple> func = new MergeDoubleSummarySketch.IntermediateFinal("16384");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    long key = 1;
    Random rnd = new Random();
    long uniques = 0;
    long updates = 0;
    // this is to simulate the output from a prior call of IntermediateFinal
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(16384).build();
      for (int i = 0; i < 40000; i++) sketch.update(key++, rnd.nextDouble() * 20);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 40000;
    }
    key -= 20000; // overlap
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) sketch.update(key++, rnd.nextDouble() * 20);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }
    key -= 20000; // overlap
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) sketch.update(key++, rnd.nextDouble() * 20);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }
    key -= 20000; // overlap
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) sketch.update(key++, rnd.nextDouble() * 20);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(16384).build();
      for (int i = 0; i < 40000; i++) sketch.update(key++, rnd.nextDouble() * 20);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 40000;
    }
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(16384).build();
      for (int i = 0; i < 40000; i++) sketch.update(key++, rnd.nextDouble() * 20);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 40000;
    }
    key -= 20000; // overlap
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) sketch.update(key++, rnd.nextDouble() * 20);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), uniques, uniques * 0.01);
    Assert.assertTrue(sketch.getRetainedEntries() > 16384);
    double sum = 0;
    for (DoubleSummary summary: sketch.getSummaries()) {
      sum += summary.getValue();
    }
    // each update added 10 to the total on average
    Assert.assertEquals(sum / sketch.getTheta(), updates * 10.0, updates * 10.0 * 0.02); // there is a slight chance of failing here 
  }
}
