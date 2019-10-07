/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.pig.tuple;

import java.util.Random;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;

@SuppressWarnings("javadoc")
public class UnionDoubleSummarySketchTest {

  @Test
  public void execNullInput() throws Exception {
    EvalFunc<Tuple> func = new UnionDoubleSummarySketch();
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new UnionDoubleSummarySketch();
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void exec() throws Exception {
    EvalFunc<Tuple> func = new UnionDoubleSummarySketch("4096");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch =
        Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 2.0, 0.0);
    }
  }

  @Test
  public void execMaxMode() throws Exception {
    EvalFunc<Tuple> func = new UnionDoubleSummarySketch("4096", "Max");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch.update(1, 3.0);
      sketch.update(2, 3.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 3.0, 0.0);
    }
  }

  @Test
  public void accumulatorNullInput() throws Exception {
    Accumulator<Tuple> func = new UnionDoubleSummarySketch("32");
    func.accumulate(null);
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorEmptyInputTuple() throws Exception {
    Accumulator<Tuple> func = new UnionDoubleSummarySketch("32");
    func.accumulate(TupleFactory.getInstance().newTuple());
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorNotABag() throws Exception {
    Accumulator<Tuple> func = new UnionDoubleSummarySketch("32");
    func.accumulate(PigUtil.objectsToTuple((Object) null));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorEmptyBag() throws Exception {
    Accumulator<Tuple> func = new UnionDoubleSummarySketch("32");
    func.accumulate(PigUtil.objectsToTuple(BagFactory.getInstance().newDefaultBag()));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorEmptyInnerTuple() throws Exception {
    Accumulator<Tuple> func = new UnionDoubleSummarySketch("32");
    func.accumulate(PigUtil.objectsToTuple(PigUtil.tuplesToBag(TupleFactory.getInstance().newTuple())));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorNullSketch() throws Exception {
    Accumulator<Tuple> func = new UnionDoubleSummarySketch("32");
    func.accumulate(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple((Object) null))));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorEmptySketch() throws Exception {
    Accumulator<Tuple> func = new UnionDoubleSummarySketch("4096");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch =
        Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<Tuple> func = new UnionDoubleSummarySketch("4096");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));

    bag = BagFactory.getInstance().newDefaultBag();
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
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
    Sketch<DoubleSummary> sketch =
        Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 2.0, 0.0);
    }
  }

  @Test
  public void algebraicInitial() throws Exception {
    EvalFunc<Tuple> func = new UnionDoubleSummarySketch.Initial(null);
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
    EvalFunc<Tuple> func = new UnionDoubleSummarySketch.IntermediateFinal("4096", "Min");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    // this is to simulate the output from Initial
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch.update(1, 1.0);
      sketch.update(2, 1.0);
      DataBag innerBag = PigUtil.tuplesToBag(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      bag.add(PigUtil.objectsToTuple(innerBag));
    }

    // this is to simulate the output from a prior call of IntermediateFinal
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch.update(1, 3.0);
      sketch.update(2, 3.0);
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0, 0.0);
    }
  }

  @Test
  public void algebraicIntemediateFinalEstimation() throws Exception {
    EvalFunc<Tuple> func = new UnionDoubleSummarySketch.IntermediateFinal("16384");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    long value = 1;
    // this is to simulate the output from Initial
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(
              new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(16384).build();
      for (int i = 0; i < 20000; i++) {
        sketch.update(value++, 1.0);
      }
      DataBag innerBag = PigUtil.tuplesToBag(PigUtil.objectsToTuple(
          new DataByteArray(sketch.compact().toByteArray())));
      bag.add(PigUtil.objectsToTuple(innerBag));
    }

    // this is to simulate the output from a prior call of IntermediateFinal
    {
      UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<>(
          new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(16384).build();
      for (int i = 0; i < 20000; i++) {
        sketch.update(value++, 1.0);
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch =
        Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 40000.0, 40000.0 * 0.01);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0, 0.0);
    }
  }

  @Test
  public void algebraicIntemediateFinalSingleCall() throws Exception {
    EvalFunc<Tuple> func = new UnionDoubleSummarySketch.IntermediateFinal("1024");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    long value = 1;
    // this is to simulate the output from a prior call of IntermediateFinal
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(
              new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(1024).build();
      for (int i = 0; i < 10000; i++) {
        sketch.update(value++, 1.0);
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), 10000.0, 10000.0 * 0.02);
    SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0, 0.0);
    }
  }

  @Test
  public void algebraicIntemediateFinalRandomized() throws Exception {
    EvalFunc<Tuple> func = new UnionDoubleSummarySketch.IntermediateFinal("16384");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    long key = 1;
    Random rnd = new Random();
    long uniques = 0;
    long updates = 0;
    // this is to simulate the output from a prior call of IntermediateFinal
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(
              new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(16384).build();
      for (int i = 0; i < 40000; i++) {
        sketch.update(key++, rnd.nextDouble() * 20);
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 40000;
    }
    key -= 20000; // overlap
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(
              new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) {
        sketch.update(key++, rnd.nextDouble() * 20);
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }
    key -= 20000; // overlap
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) {
        sketch.update(key++, rnd.nextDouble() * 20);
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }
    key -= 20000; // overlap
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(
              new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) {
        sketch.update(key++, rnd.nextDouble() * 20);
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(
              new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(16384).build();
      for (int i = 0; i < 40000; i++) {
        sketch.update(key++, rnd.nextDouble() * 20);
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 40000;
    }
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(
              new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(16384).build();
      for (int i = 0; i < 40000; i++) {
        sketch.update(key++, rnd.nextDouble() * 20);
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 40000;
    }
    key -= 20000; // overlap
    {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(
              new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) {
        sketch.update(key++, rnd.nextDouble() * 20);
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    Sketch<DoubleSummary> sketch =
        Sketches.heapifySketch(Memory.wrap(bytes.get()), new DoubleSummaryDeserializer());
    Assert.assertEquals(sketch.getEstimate(), uniques, uniques * 0.01);
    double sum = 0;
    SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      sum += it.getSummary().getValue();
    }
    // each update added 10 to the total on average
    // there is a slight chance of failing here
    Assert.assertEquals(sum / sketch.getTheta(), updates * 10.0, updates * 10.0 * 0.02);
  }
}
